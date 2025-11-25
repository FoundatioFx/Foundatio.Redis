using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Foundatio.AsyncEx;
using Foundatio.Extensions;
using Foundatio.Redis;
using Foundatio.Redis.Extensions;
using Foundatio.Redis.Utility;
using Foundatio.Serializer;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using StackExchange.Redis;

namespace Foundatio.Caching;

public sealed class RedisCacheClient : ICacheClient, IHaveSerializer
{
    private readonly RedisCacheClientOptions _options;
    private readonly TimeProvider _timeProvider;
    private readonly ILogger _logger;

    private readonly AsyncLock _lock = new();
    private bool _scriptsLoaded;
    private bool? _supportsMsetEx;

    private LoadedLuaScript _incrementWithExpire;
    private LoadedLuaScript _removeIfEqual;
    private LoadedLuaScript _replaceIfEqual;
    private LoadedLuaScript _setIfHigher;
    private LoadedLuaScript _setIfLower;
    private LoadedLuaScript _getAllExpiration;
    private LoadedLuaScript _setAllExpiration;

    public RedisCacheClient(RedisCacheClientOptions options)

    {
        _options = options;
        _timeProvider = options.TimeProvider ?? TimeProvider.System;
        options.Serializer ??= DefaultSerializer.Instance;
        _logger = options.LoggerFactory?.CreateLogger(typeof(RedisCacheClient)) ?? NullLogger.Instance;

        options.ConnectionMultiplexer.ConnectionRestored += ConnectionMultiplexerOnConnectionRestored;
        options.ConnectionMultiplexer.ConnectionFailed += ConnectionMultiplexerOnConnectionFailed;
    }

    public RedisCacheClient(Builder<RedisCacheClientOptionsBuilder, RedisCacheClientOptions> config)
        : this(config(new RedisCacheClientOptionsBuilder()).Build())
    {
    }

    public IDatabase Database => _options.ConnectionMultiplexer.GetDatabase(_options.Database);

    public Task<bool> RemoveAsync(string key)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);

        _logger.LogTrace("RemoveAsync: Removing key: {Key}", key);
        return Database.KeyDeleteAsync(key);
    }

    public async Task<bool> RemoveIfEqualAsync<T>(string key, T expected)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);

        await LoadScriptsAsync().AnyContext();

        var expectedValue = expected.ToRedisValue(_options.Serializer);
        var redisResult = await Database.ScriptEvaluateAsync(_removeIfEqual, new { key = (RedisKey)key, expected = expectedValue }).AnyContext();
        int result = (int)redisResult;

        return result > 0;
    }

    public async Task<int> RemoveAllAsync(IEnumerable<string> keys = null)
    {
        // NOTE: Batch size matches default page size (RedisBase.CursorUtils.DefaultLibraryPageSize)
        int batchSize = 250;

        long deleted = 0;
        if (keys is null)
        {
            var endpoints = _options.ConnectionMultiplexer.GetEndPoints();
            if (endpoints.Length == 0)
                return 0;

            // Get non-replica endpoints for processing
            var nonReplicaEndpoints = endpoints
                .Select(endpoint => _options.ConnectionMultiplexer.GetServer(endpoint))
                .Where(server => !server.IsReplica)
                .ToArray();

            // Most Redis deployments have few endpoints (1-3), so parallelism here is helpful
            // but not critical. Controlling it helps prevent excessive load on Redis cluster.
            int maxEndpointParallelism = Math.Min(Environment.ProcessorCount, nonReplicaEndpoints.Length);

            await Parallel.ForEachAsync(nonReplicaEndpoints, new ParallelOptions { MaxDegreeOfParallelism = maxEndpointParallelism },
                async (server, ct) =>
            {
                // Try FLUSHDB first (fastest approach)
                bool flushed = false;
                try
                {
                    long dbSize = await server.DatabaseSizeAsync(_options.Database).AnyContext();
                    await server.FlushDatabaseAsync(_options.Database).AnyContext();
                    Interlocked.Add(ref deleted, dbSize);
                    flushed = true;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Unable to flush database on {Endpoint}: {Message}", server.EndPoint, ex.Message);
                }

                // If FLUSHDB failed, fall back to SCAN + DELETE
                if (!flushed)
                {
                    try
                    {
                        // NOTE: We need to use a HashSet to avoid duplicate counts due to SCAN is non-deterministic.
                        // A Performance win could be had if we are sure dbSize didn't fail and we know nothing was changing
                        // keys while we were deleting.
                        var seen = new HashSet<RedisKey>();
                        await foreach (var key in server.KeysAsync(_options.Database).ConfigureAwait(false))
                            seen.Add(key);

                        // NOTE: StackExchange.Redis multiplexes all operations over a single connection and handles
                        // pipelining internally, so parallelism limits here only affect client-side Task creation,
                        // not Redis server load. Consider simplifying to Task.WhenAll in a future refactor.
                        int maxParallelism = Math.Min(8, Environment.ProcessorCount);
                        await Parallel.ForEachAsync(seen.Chunk(batchSize), new ParallelOptions { MaxDegreeOfParallelism = maxParallelism }, async (batch, ct) =>
                        {
                            long count = await Database.KeyDeleteAsync(batch.ToArray()).AnyContext();
                            Interlocked.Add(ref deleted, count);
                        }).AnyContext();
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Error deleting all keys on {Endpoint}: {Message}", server.EndPoint, ex.Message);
                    }
                }
            }).AnyContext();
        }
        else if (Database.Multiplexer.IsCluster())
        {
            var redisKeys = keys is ICollection<string> collection ? new List<RedisKey>(collection.Count) : [];
            foreach (string key in keys.Distinct())
            {
                ArgumentException.ThrowIfNullOrEmpty(key, nameof(keys));
                redisKeys.Add(key);
            }

            if (redisKeys.Count is 0)
                return 0;

            foreach (var batch in redisKeys.Chunk(batchSize))
            {
                await Parallel.ForEachAsync(
                    batch.GroupBy(k => Database.Multiplexer.HashSlot(k)),
                    async (hashSlotGroup, ct) =>
                    {
                        var hashSlotKeys = hashSlotGroup.ToArray();
                        try
                        {
                            long count = await Database.KeyDeleteAsync(hashSlotKeys).AnyContext();
                            Interlocked.Add(ref deleted, count);
                        }
                        catch (Exception ex)
                        {
                            _logger.LogError(ex, "Unable to delete {HashSlot} keys ({Keys}): {Message}", hashSlotGroup.Key, hashSlotKeys, ex.Message);
                        }
                    }).AnyContext();
            }
        }
        else
        {
            var redisKeys = keys is ICollection<string> collection ? new List<RedisKey>(collection.Count) : [];
            foreach (string key in keys.Distinct())
            {
                ArgumentException.ThrowIfNullOrEmpty(key, nameof(keys));
                redisKeys.Add(key);
            }

            if (redisKeys.Count is 0)
                return 0;

            var keyBatches = redisKeys.Chunk(batchSize).ToArray();

            // NOTE: StackExchange.Redis multiplexes all operations over a single connection and handles
            // pipelining internally, so parallelism limits here only affect client-side Task creation,
            // not Redis server load. Consider simplifying to Task.WhenAll in a future refactor.
            int maxParallelism = Math.Min(8, Environment.ProcessorCount);
            await Parallel.ForEachAsync(keyBatches, new ParallelOptions { MaxDegreeOfParallelism = maxParallelism }, async (batch, ct) =>
            {
                try
                {
                    long count = await Database.KeyDeleteAsync(batch).AnyContext();
                    Interlocked.Add(ref deleted, count);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Unable to delete keys ({Keys}): {Message}", batch, ex.Message);
                }
            }).AnyContext();
        }

        return (int)deleted;
    }

    public async Task<int> RemoveByPrefixAsync(string prefix)
    {
        if (String.IsNullOrEmpty(prefix))
            return await RemoveAllAsync().AnyContext();

        var endpoints = _options.ConnectionMultiplexer.GetEndPoints();
        if (endpoints.Length == 0)
            return 0;

        const int batchSize = 250;
        bool isCluster = _options.ConnectionMultiplexer.IsCluster();

        long deleted = 0;
        foreach (var endpoint in endpoints)
        {
            var server = _options.ConnectionMultiplexer.GetServer(endpoint);
            if (server.IsReplica)
                continue;

            await foreach (var keys in server.KeysAsync(
                               database: Database.Database,
                               pattern: $"{RedisPattern.Escape(prefix)}*",
                               pageSize: batchSize
                           ).BatchAsync(batchSize).ConfigureAwait(false))
            {
                if (isCluster)
                {
                    await Parallel.ForEachAsync(
                        keys.GroupBy(k => _options.ConnectionMultiplexer.HashSlot(k)),
                        async (slotGroup, ct) =>
                        {
                            long count = await Database.KeyDeleteAsync(slotGroup.ToArray()).AnyContext();
                            Interlocked.Add(ref deleted, count);
                        }).AnyContext();
                }
                else
                {
                    deleted += await Database.KeyDeleteAsync(keys).AnyContext();
                }
            }
        }

        return (int)deleted;
    }

    private static readonly RedisValue _nullValue = "@@NULL";

    public async Task<CacheValue<T>> GetAsync<T>(string key)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);

        var redisValue = await Database.StringGetAsync(key, _options.ReadMode).AnyContext();
        return RedisValueToCacheValue<T>(redisValue);
    }

    private CacheValue<ICollection<T>> RedisValuesToCacheValue<T>(RedisValue[] redisValues)
    {
        var result = new List<T>();
        foreach (var redisValue in redisValues)
        {
            if (!redisValue.HasValue)
                continue;
            if (redisValue == _nullValue)
                continue;

            try
            {
                var value = redisValue.ToValueOfType<T>(_options.Serializer);
                result.Add(value);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Unable to deserialize value {Value} to type {Type}: {Message}", redisValue, typeof(T).FullName, ex.Message);
                if (_options.ShouldThrowOnSerializationError)
                    throw;
            }
        }

        return new CacheValue<ICollection<T>>(result, result.Count > 0);
    }

    private CacheValue<T> RedisValueToCacheValue<T>(RedisValue redisValue)
    {
        if (!redisValue.HasValue)
            return CacheValue<T>.NoValue;

        if (redisValue == _nullValue)
            return CacheValue<T>.Null;

        try
        {
            var value = redisValue.ToValueOfType<T>(_options.Serializer);
            return new CacheValue<T>(value, true);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Unable to deserialize value {Value} to type {Type}: {Message}", redisValue, typeof(T).FullName, ex.Message);
            if (_options.ShouldThrowOnSerializationError)
                throw;

            return CacheValue<T>.NoValue;
        }
    }

    public async Task<IDictionary<string, CacheValue<T>>> GetAllAsync<T>(IEnumerable<string> keys)
    {
        ArgumentNullException.ThrowIfNull(keys);

        var redisKeys = keys is ICollection<string> collection ? new List<RedisKey>(collection.Count) : [];
        foreach (string key in keys.Distinct())
        {
            ArgumentException.ThrowIfNullOrEmpty(key, nameof(keys));
            redisKeys.Add(key);
        }

        if (redisKeys.Count is 0)
            return ReadOnlyDictionary<string, CacheValue<T>>.Empty;

        if (_options.ConnectionMultiplexer.IsCluster())
        {
            // Use the default concurrency on .NET 8 (-1)
            var result = new ConcurrentDictionary<string, CacheValue<T>>(-1, redisKeys.Count);
            await Parallel.ForEachAsync(
                redisKeys.GroupBy(k => _options.ConnectionMultiplexer.HashSlot(k)),
                async (hashSlotGroup, ct) =>
                {
                    var hashSlotKeys = hashSlotGroup.ToArray();
                    var values = await Database.StringGetAsync(hashSlotKeys, _options.ReadMode).AnyContext();

                    // Redis MGET guarantees that values are returned in the same order as keys.
                    // Non-existent keys return nil/empty values in their respective positions.
                    // https://redis.io/commands/mget
                    for (int i = 0; i < hashSlotKeys.Length; i++)
                        result[hashSlotKeys[i]] = RedisValueToCacheValue<T>(values[i]);
                }).AnyContext();

            return result.AsReadOnly();
        }
        else
        {
            var result = new Dictionary<string, CacheValue<T>>(redisKeys.Count);
            var values = await Database.StringGetAsync(redisKeys.ToArray(), _options.ReadMode).AnyContext();

            // Redis MGET guarantees that values are returned in the same order as keys
            for (int i = 0; i < redisKeys.Count; i++)
                result[redisKeys[i]] = RedisValueToCacheValue<T>(values[i]);

            return result.AsReadOnly();
        }
    }

    public async Task<CacheValue<ICollection<T>>> GetListAsync<T>(string key, int? page = null, int pageSize = 100)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);

        if (page is < 1)
            throw new ArgumentOutOfRangeException(nameof(page), "Page cannot be less than 1");

        try
        {
            if (!page.HasValue)
            {
                var set = await Database.SortedSetRangeByScoreAsync(key, _timeProvider.GetUtcNow().ToUnixTimeMilliseconds(), Double.PositiveInfinity, Exclude.Start, flags: _options.ReadMode).AnyContext();
                return RedisValuesToCacheValue<T>(set);
            }
            else
            {
                long skip = (page.Value - 1) * pageSize;
                var set = await Database.SortedSetRangeByScoreAsync(key, _timeProvider.GetUtcNow().ToUnixTimeMilliseconds(), Double.PositiveInfinity, Exclude.Start, skip: skip, take: pageSize, flags: _options.ReadMode).AnyContext();
                return RedisValuesToCacheValue<T>(set);
            }
        }
        catch (RedisServerException ex) when (ex.Message.StartsWith("WRONGTYPE"))
        {
            _logger.LogInformation(ex, "Migrating legacy set to sorted set for key: {Key}", key);
            await MigrateLegacySetToSortedSetForKeyAsync<T>(key, typeof(T) == typeof(string)).AnyContext();
            return await GetListAsync<T>(key, page, pageSize).AnyContext();
        }
    }

    public Task<bool> AddAsync<T>(string key, T value, TimeSpan? expiresIn = null)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);

        return InternalSetAsync(key, value, expiresIn, When.NotExists);
    }

    public async Task<long> ListAddAsync<T>(string key, IEnumerable<T> values, TimeSpan? expiresIn = null)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        ArgumentNullException.ThrowIfNull(values);

        var expiresAt = expiresIn.HasValue ? _timeProvider.GetUtcNow().UtcDateTime.SafeAdd(expiresIn.Value) : DateTime.MaxValue;
        if (expiresAt < _timeProvider.GetUtcNow().UtcDateTime)
        {
            await ListRemoveAsync(key, values).AnyContext();
            return 0;
        }

        var redisValues = new List<SortedSetEntry>();
        long expiresAtMilliseconds = expiresAt.ToUnixTimeMilliseconds();

        if (values is string stringValue)
            redisValues.Add(new SortedSetEntry(stringValue.ToRedisValue(_options.Serializer), expiresAtMilliseconds));
        else
            redisValues.AddRange(values.Where(v => v is not null).Select(value => new SortedSetEntry(value.ToRedisValue(_options.Serializer), expiresAtMilliseconds)));

        await RemoveExpiredListValuesAsync<T>(key, values is string).AnyContext();

        if (redisValues.Count == 0)
            return 0;

        long added = await Database.SortedSetAddAsync(key, redisValues.ToArray()).AnyContext();
        if (added > 0)
            await SetListExpirationAsync(key).AnyContext();

        return added;
    }

    public async Task<long> ListRemoveAsync<T>(string key, IEnumerable<T> values, TimeSpan? expiresIn = null)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        ArgumentNullException.ThrowIfNull(values);

        var redisValues = new List<RedisValue>();
        if (values is string stringValue)
            redisValues.Add(stringValue.ToRedisValue(_options.Serializer));
        else
            redisValues.AddRange(values.Where(v => v is not null).Select(value => value.ToRedisValue(_options.Serializer)));

        await RemoveExpiredListValuesAsync<T>(key, values is string).AnyContext();

        if (redisValues.Count == 0)
            return 0;

        long removed = await Database.SortedSetRemoveAsync(key, redisValues.ToArray()).AnyContext();
        if (removed > 0)
            await SetListExpirationAsync(key).AnyContext();

        return removed;
    }

    private const long MaxUnixEpochMilliseconds = 253_402_300_799_999L; // 9999-12-31T23:59:59.999Z

    private async Task SetListExpirationAsync(string key)
    {
        var items = await Database.SortedSetRangeByRankWithScoresAsync(key, 0, 0, order: Order.Descending).AnyContext();
        if (items.Length == 0)
        {
            _logger.LogTrace("Sorted set is empty for key: {Key}, expiration will not be set", key);
            return;
        }

        long highestExpirationInMs = (long)items.Single().Score;
        if (highestExpirationInMs > MaxUnixEpochMilliseconds)
        {
            await SetExpirationAsync(key, TimeSpan.MaxValue).AnyContext();
            return;
        }

        var furthestExpirationUtc = highestExpirationInMs.FromUnixTimeMilliseconds();
        var expiresIn = furthestExpirationUtc - _timeProvider.GetUtcNow();
        await SetExpirationAsync(key, expiresIn).AnyContext();
    }

    private async Task RemoveExpiredListValuesAsync<T>(string key, bool isStringValues)
    {
        try
        {
            long expiredValues = await Database
                .SortedSetRemoveRangeByScoreAsync(key, 0, _timeProvider.GetUtcNow().ToUnixTimeMilliseconds())
                .AnyContext();

            if (expiredValues > 0)
                _logger.LogTrace("Removed {ExpiredValues} expired values for key: {Key}", expiredValues, key);
        }
        catch (RedisServerException ex) when (ex.Message.StartsWith("WRONGTYPE"))
        {
            _logger.LogInformation(ex, "Migrating legacy set to sorted set for key: {Key}", key);
            await MigrateLegacySetToSortedSetForKeyAsync<T>(key, isStringValues).AnyContext();
        }
    }

    private async Task MigrateLegacySetToSortedSetForKeyAsync<T>(string key, bool isStringValues)
    {
        // convert legacy set to sorted set
        var oldItems = await Database.SetMembersAsync(key).AnyContext();
        var currentKeyExpiresIn = await GetExpirationAsync(key).AnyContext();
        await Database.KeyDeleteAsync(key).AnyContext();
        if (isStringValues)
        {
            var oldItemValues = new List<string>(oldItems.Length);
            foreach (string oldItem in RedisValuesToCacheValue<string>(oldItems).Value)
                oldItemValues.Add(oldItem);

            await ListAddAsync(key, oldItemValues, currentKeyExpiresIn).AnyContext();
        }
        else
        {
            var oldItemValues = new List<T>(oldItems.Length);
            foreach (var oldItem in RedisValuesToCacheValue<T>(oldItems).Value)
                oldItemValues.Add(oldItem);

            await ListAddAsync(key, oldItemValues, currentKeyExpiresIn).AnyContext();
        }
    }

    public Task<bool> SetAsync<T>(string key, T value, TimeSpan? expiresIn = null)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);

        return InternalSetAsync(key, value, expiresIn);
    }

    public async Task<double> SetIfHigherAsync(string key, double value, TimeSpan? expiresIn = null)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);

        await LoadScriptsAsync().AnyContext();

        if (expiresIn.HasValue)
        {
            var result = await Database.ScriptEvaluateAsync(_setIfHigher, new { key = (RedisKey)key, value, expires = (int)expiresIn.Value.TotalMilliseconds }).AnyContext();
            return (double)result;
        }
        else
        {
            var result = await Database.ScriptEvaluateAsync(_setIfHigher, new { key = (RedisKey)key, value, expires = RedisValue.EmptyString }).AnyContext();
            return (double)result;
        }
    }

    public async Task<long> SetIfHigherAsync(string key, long value, TimeSpan? expiresIn = null)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);

        await LoadScriptsAsync().AnyContext();

        if (expiresIn.HasValue)
        {
            var result = await Database.ScriptEvaluateAsync(_setIfHigher, new { key = (RedisKey)key, value, expires = (int)expiresIn.Value.TotalMilliseconds }).AnyContext();
            return (long)result;
        }
        else
        {
            var result = await Database.ScriptEvaluateAsync(_setIfHigher, new { key = (RedisKey)key, value, expires = RedisValue.EmptyString }).AnyContext();
            return (long)result;
        }
    }

    public async Task<double> SetIfLowerAsync(string key, double value, TimeSpan? expiresIn = null)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);

        await LoadScriptsAsync().AnyContext();

        if (expiresIn.HasValue)
        {
            var result = await Database.ScriptEvaluateAsync(_setIfLower, new { key = (RedisKey)key, value, expires = (int)expiresIn.Value.TotalMilliseconds }).AnyContext();
            return (double)result;
        }
        else
        {
            var result = await Database.ScriptEvaluateAsync(_setIfLower, new { key = (RedisKey)key, value, expires = RedisValue.EmptyString }).AnyContext();
            return (double)result;
        }
    }

    public async Task<long> SetIfLowerAsync(string key, long value, TimeSpan? expiresIn = null)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);

        await LoadScriptsAsync().AnyContext();

        if (expiresIn.HasValue)
        {
            var result = await Database.ScriptEvaluateAsync(_setIfLower, new { key = (RedisKey)key, value, expires = (int)expiresIn.Value.TotalMilliseconds }).AnyContext();
            return (long)result;
        }
        else
        {
            var result = await Database.ScriptEvaluateAsync(_setIfLower, new { key = (RedisKey)key, value, expires = RedisValue.EmptyString }).AnyContext();
            return (long)result;
        }
    }

    private async Task<bool> InternalSetAsync<T>(string key, T value, TimeSpan? expiresIn = null, When when = When.Always, CommandFlags flags = CommandFlags.None)
    {
        if (expiresIn?.Ticks <= 0)
        {
            _logger.LogTrace("Removing expired key: {Key}", key);
            await RemoveAsync(key).AnyContext();
            return false;
        }

        var redisValue = value.ToRedisValue(_options.Serializer);
        return await Database.StringSetAsync(key, redisValue, expiresIn, when, flags).AnyContext();
    }

    public async Task<int> SetAllAsync<T>(IDictionary<string, T> values, TimeSpan? expiresIn = null)
    {
        ArgumentNullException.ThrowIfNull(values);
        if (values.Count is 0)
            return 0;

        if (expiresIn?.Ticks <= 0)
        {
            _logger.LogTrace("Removing expired keys: {Keys}", values.Keys);
            await RemoveAllAsync(values.Keys).AnyContext();
            return 0;
        }

        // Validate keys and serialize values upfront
        var pairs = new KeyValuePair<RedisKey, RedisValue>[values.Count];
        int index = 0;
        foreach (var kvp in values)
        {
            ArgumentException.ThrowIfNullOrEmpty(kvp.Key, nameof(values));
            pairs[index++] = new KeyValuePair<RedisKey, RedisValue>(kvp.Key, kvp.Value.ToRedisValue(_options.Serializer));
        }

        if (_options.ConnectionMultiplexer.IsCluster())
        {
            // For cluster/sentinel, group keys by hash slot since batch operations
            // require all keys to be in the same slot
            int successCount = 0;
            await Parallel.ForEachAsync(
                pairs.GroupBy(p => _options.ConnectionMultiplexer.HashSlot(p.Key)),
                async (slotGroup, ct) =>
                {
                    int count = await SetAllInternalAsync(slotGroup.ToArray(), expiresIn).AnyContext();
                    Interlocked.Add(ref successCount, count);
                }).AnyContext();
            return successCount;
        }

        return await SetAllInternalAsync(pairs, expiresIn).AnyContext();
    }

    /// <summary>
    /// Internal method that performs the actual batch set operation.
    /// All keys in <paramref name="pairs"/> must be in the same hash slot for cluster mode.
    /// </summary>
    private async Task<int> SetAllInternalAsync(KeyValuePair<RedisKey, RedisValue>[] pairs, TimeSpan? expiresIn)
    {
        if (expiresIn.HasValue)
        {
            // With expiration: use MSETEX if available, otherwise fall back to pipelined SETs
            if (SupportsMsetexCommand())
            {
                bool success = await Database.StringSetAsync(pairs, When.Always, new Expiration(expiresIn.Value)).AnyContext();
                return success ? pairs.Length : 0;
            }

            // Fallback for Redis < 8.4: pipelined individual SET commands with expiration
            var tasks = new List<Task<bool>>(pairs.Length);
            foreach (var pair in pairs)
            {
                tasks.Add(Database.StringSetAsync(pair.Key, pair.Value, expiresIn, When.Always));
            }

            bool[] results = await Task.WhenAll(tasks).AnyContext();
            return results.Count(r => r);
        }

        // No expiration: use MSET (available since Redis 1.0.1) - single atomic batch operation
        bool msetSuccess = await Database.StringSetAsync(pairs).AnyContext();
        return msetSuccess ? pairs.Length : 0;
    }

    /// <summary>
    /// Checks if the connected Redis server supports the MSETEX command (Redis 8.4+).
    /// </summary>
    /// <remarks>
    /// <para>
    /// MSETEX allows setting multiple keys with expiration in a single atomic operation.
    /// StackExchange.Redis 2.10.1+ supports this via the <c>StringSetAsync</c> overload that
    /// accepts an <see cref="Expiration"/> parameter. However, SE.Redis does NOT automatically
    /// fall back to individual SET commands on older Redis versions - it will fail.
    /// </para>
    /// <para>
    /// This method detects the Redis server version and caches the result. The cache is
    /// invalidated when the connection is restored (e.g., after failover to a different server).
    /// </para>
    /// <para>
    /// Note: For batch sets WITHOUT expiration, use <c>StringSetAsync(pairs, When, CommandFlags)</c>
    /// which uses MSET - available since Redis 1.0.1 and doesn't require version detection.
    /// MSET replaces existing values and removes any existing TTL, just like regular SET.
    /// </para>
    /// </remarks>
    /// <returns>
    /// <c>true</c> if all connected primary servers support MSETEX;
    /// <c>false</c> if any primary is running Redis &lt; 8.4 or no primaries are connected.
    /// </returns>
    private bool SupportsMsetexCommand()
    {
        if (_supportsMsetEx.HasValue)
            return _supportsMsetEx.Value;

        // Redis 8.4 RC1 is internally versioned as 8.3.224
        var minVersion = new Version(8, 3, 224);

        var endpoints = _options.ConnectionMultiplexer.GetEndPoints();
        if (endpoints.Length == 0)
        {
            _logger.LogDebug("SupportsMsetexCommand: No endpoints configured, MSETEX not available");
            return false; // Don't cache - no endpoints configured
        }

        bool foundConnectedPrimary = false;
        foreach (var endpoint in endpoints)
        {
            var server = _options.ConnectionMultiplexer.GetServer(endpoint);
            if (server.IsConnected && !server.IsReplica)
            {
                foundConnectedPrimary = true;
                if (server.Version < minVersion)
                {
                    _logger.LogDebug("SupportsMsetexCommand: Server {Endpoint} version {Version} does not support MSETEX (requires {MinVersion}+)",
                        endpoint, server.Version, minVersion);
                    _supportsMsetEx = false;
                    return false;
                }
            }
        }

        if (foundConnectedPrimary)
        {
            _supportsMsetEx = true;
            return true;
        }

        _logger.LogDebug("SupportsMsetexCommand: No connected primaries found, MSETEX availability unknown");
        // No connected primaries found - don't cache, will retry on next call
        return false;
    }

    public Task<bool> ReplaceAsync<T>(string key, T value, TimeSpan? expiresIn = null)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);

        return InternalSetAsync(key, value, expiresIn, When.Exists);
    }

    public async Task<bool> ReplaceIfEqualAsync<T>(string key, T value, T expected, TimeSpan? expiresIn = null)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);

        await LoadScriptsAsync().AnyContext();

        var redisValue = value.ToRedisValue(_options.Serializer);
        var expectedValue = expected.ToRedisValue(_options.Serializer);
        RedisResult redisResult;

        // NOTE: If the expires is negative, we need to set it to 1ms to avoid an exception. We could look into replacing the operation to PEXPIRE
        if (expiresIn.HasValue)
            redisResult = await Database.ScriptEvaluateAsync(_replaceIfEqual, new { key = (RedisKey)key, value = redisValue, expected = expectedValue, expires = Math.Max((int)expiresIn.Value.TotalMilliseconds, 1) }).AnyContext();
        else
            redisResult = await Database.ScriptEvaluateAsync(_replaceIfEqual, new { key = (RedisKey)key, value = redisValue, expected = expectedValue, expires = "" }).AnyContext();

        var result = (int)redisResult;

        return result > 0;
    }

    public async Task<double> IncrementAsync(string key, double amount = 1, TimeSpan? expiresIn = null)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);

        if (expiresIn.HasValue)
        {
            await LoadScriptsAsync().AnyContext();
            var result = await Database.ScriptEvaluateAsync(_incrementWithExpire, new { key = (RedisKey)key, value = amount, expires = (int)expiresIn.Value.TotalMilliseconds }).AnyContext();
            return (double)result;
        }

        return await Database.StringIncrementAsync(key, amount).AnyContext();
    }

    public async Task<long> IncrementAsync(string key, long amount = 1, TimeSpan? expiresIn = null)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);

        if (expiresIn?.Ticks <= 0)
        {
            _logger.LogTrace("Removing expired key: {Key}", key);
            await RemoveAsync(key).AnyContext();
            return -1;
        }

        if (expiresIn.HasValue)
        {
            await LoadScriptsAsync().AnyContext();
            var result = await Database.ScriptEvaluateAsync(_incrementWithExpire, new { key = (RedisKey)key, value = amount, expires = (int)expiresIn.Value.TotalMilliseconds }).AnyContext();
            return (long)result;
        }

        return await Database.StringIncrementAsync(key, amount).AnyContext();
    }

    public Task<bool> ExistsAsync(string key)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);

        return Database.KeyExistsAsync(key);
    }

    public Task<TimeSpan?> GetExpirationAsync(string key)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);

        return Database.KeyTimeToLiveAsync(key);
    }

    public Task SetExpirationAsync(string key, TimeSpan expiresIn)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);

        return Database.KeyExpireAsync(key, expiresIn);
    }

    public async Task<IDictionary<string, TimeSpan?>> GetAllExpirationAsync(IEnumerable<string> keys)
    {
        ArgumentNullException.ThrowIfNull(keys);

        var keyList = keys.Where(k => !String.IsNullOrEmpty(k)).Distinct().ToList();
        if (keyList.Count is 0)
            return ReadOnlyDictionary<string, TimeSpan?>.Empty;

        await LoadScriptsAsync().AnyContext();

        if (_options.ConnectionMultiplexer.IsCluster())
        {
            // Use the default concurrency on .NET 8 (-1)
            var result = new ConcurrentDictionary<string, TimeSpan?>(-1, keyList.Count);
            await Parallel.ForEachAsync(
                keyList.GroupBy(k => _options.ConnectionMultiplexer.HashSlot(k)),
                async (hashSlotGroup, ct) =>
                {
                    var hashSlotKeys = hashSlotGroup.Select(k => (RedisKey)k).ToArray();
                    var redisResult = await Database.ScriptEvaluateAsync(_getAllExpiration.Hash, hashSlotKeys).AnyContext();
                    if (redisResult.IsNull)
                        return;

                    // Lua script returns array of TTL values in milliseconds (in same order as keys)
                    // -2 = key doesn't exist, -1 = no expiration, positive = TTL in ms
                    long[] ttls = (long[])redisResult;
                    if (ttls is null || ttls.Length != hashSlotKeys.Length)
                        throw new ArgumentException("Hash slot count mismatch");

                    for (int hashSlotIndex = 0; hashSlotIndex < hashSlotKeys.Length; hashSlotIndex++)
                    {
                        string key = hashSlotKeys[hashSlotIndex];
                        long ttl = ttls[hashSlotIndex];
                        if (ttl >= 0) // Only include keys with positive TTL (exclude non-existent and persistent)
                            result[key] = TimeSpan.FromMilliseconds(ttl);
                    }
                }).AnyContext();

            return result.AsReadOnly();
        }
        else
        {
            var redisKeys = keyList.Select(k => (RedisKey)k).ToArray();
            var redisResult = await Database.ScriptEvaluateAsync(_getAllExpiration.Hash, redisKeys).AnyContext();

            if (redisResult.IsNull)
                return ReadOnlyDictionary<string, TimeSpan?>.Empty;

            // Lua script returns array of TTL values in milliseconds (in same order as keys)
            // -2 = key doesn't exist, -1 = no expiration, positive = TTL in ms
            long[] ttls = (long[])redisResult;
            if (ttls is null || ttls.Length != redisKeys.Length)
                throw new ArgumentException("Hash slot count mismatch");

            var result = new Dictionary<string, TimeSpan?>();
            for (int keyIndex = 0; keyIndex < redisKeys.Length; keyIndex++)
            {
                string key = redisKeys[keyIndex];
                long ttl = ttls[keyIndex];
                if (ttl >= 0) // Only include keys with positive TTL (exclude non-existent and persistent)
                    result[key] = TimeSpan.FromMilliseconds(ttl);
            }

            return result.AsReadOnly();
        }
    }

    public async Task SetAllExpirationAsync(IDictionary<string, TimeSpan?> expirations)
    {
        ArgumentNullException.ThrowIfNull(expirations);

        var validExpirations = expirations.Where(kvp => !String.IsNullOrEmpty(kvp.Key)).ToList();
        if (validExpirations.Count == 0)
            return;

        await LoadScriptsAsync().AnyContext();

        if (_options.ConnectionMultiplexer.IsCluster())
        {
            await Parallel.ForEachAsync(
                validExpirations.GroupBy(kvp => _options.ConnectionMultiplexer.HashSlot(kvp.Key)),
                async (hashSlotGroup, ct) =>
                {
                    var hashSlotExpirations = hashSlotGroup.ToList();
                    var keys = hashSlotExpirations.Select(kvp => (RedisKey)kvp.Key).ToArray();
                    var values = hashSlotExpirations
                        .Select(kvp => (RedisValue)(kvp.Value.HasValue ? (long)kvp.Value.Value.TotalMilliseconds : -1))
                        .ToArray();

                    await Database.ScriptEvaluateAsync(_setAllExpiration.Hash, keys, values).AnyContext();
                }).AnyContext();
        }
        else
        {
            var keys = validExpirations.Select(kvp => (RedisKey)kvp.Key).ToArray();
            var values = validExpirations
                .Select(kvp => (RedisValue)(kvp.Value.HasValue ? (long)kvp.Value.Value.TotalMilliseconds : -1))
                .ToArray();

            await Database.ScriptEvaluateAsync(_setAllExpiration.Hash, keys, values).AnyContext();
        }
    }

    private async Task LoadScriptsAsync()
    {
        if (_scriptsLoaded)
            return;

        using (await _lock.LockAsync().AnyContext())
        {
            if (_scriptsLoaded)
                return;

            // Prepare all the Lua scripts
            var incrementWithExpire = LuaScript.Prepare(IncrementWithScript);
            var removeIfEqual = LuaScript.Prepare(RemoveIfEqualScript);
            var replaceIfEqual = LuaScript.Prepare(ReplaceIfEqualScript);
            var setIfHigher = LuaScript.Prepare(SetIfHigherScript);
            var setIfLower = LuaScript.Prepare(SetIfLowerScript);
            var getAllExpiration = LuaScript.Prepare(GetAllExpirationScript);
            var setAllExpiration = LuaScript.Prepare(SetAllExpirationScript);

            // Get all non-replica endpoints
            var endpoints = _options.ConnectionMultiplexer.GetEndPoints()
                .Select(ep => _options.ConnectionMultiplexer.GetServer(ep))
                .Where(server => !server.IsReplica)
                .ToArray();

            if (endpoints.Length == 0)
                return;

            // In Redis Cluster, each node maintains its own script cache
            // Scripts must be loaded on every node where they might execute
            // See: https://redis.io/docs/latest/develop/programmability/eval-intro/#evalsha-and-script-load
            // See: https://redis.io/docs/management/scaling/#redis-cluster-architecture

            // Store the loaded scripts from each node separately
            // We'll load scripts on all servers in parallel first, then set the class fields
            // once using the results from any server (scripts have same SHA everywhere)

            // Create a task to load scripts on all servers in parallel
            var loadTasks = new List<Task<(
                LoadedLuaScript IncrementWithExpire,
                LoadedLuaScript RemoveIfEqual,
                LoadedLuaScript ReplaceIfEqual,
                LoadedLuaScript SetIfHigher,
                LoadedLuaScript SetIfLower,
                LoadedLuaScript GetAllExpiration,
                LoadedLuaScript SetAllExpiration)>>();

            // Start script loading tasks for all endpoints
            foreach (var server in endpoints)
            {
                if (server.IsReplica)
                    continue;

                // Create and start task for this server - completely independent
                var loadTask = Task.Run(async () =>
                {
                    // Load all scripts on this server
                    var incr = await incrementWithExpire.LoadAsync(server).AnyContext();
                    var remove = await removeIfEqual.LoadAsync(server).AnyContext();
                    var replace = await replaceIfEqual.LoadAsync(server).AnyContext();
                    var setHigher = await setIfHigher.LoadAsync(server).AnyContext();
                    var setLower = await setIfLower.LoadAsync(server).AnyContext();
                    var getAllExp = await getAllExpiration.LoadAsync(server).AnyContext();
                    var setAllExp = await setAllExpiration.LoadAsync(server).AnyContext();

                    return (incr, remove, replace, setHigher, setLower, getAllExp, setAllExp);
                });

                loadTasks.Add(loadTask);
            }

            // Wait for any server to complete loading its scripts
            // All should produce identical SHA hashes, so we only need one result
            var completedTask = await Task.WhenAny(loadTasks).AnyContext();
            var scripts = await completedTask.AnyContext();

            // Continue loading on other servers in the background (required for Redis Cluster)
            // but don't wait for them to finish - the scripts are available immediately
            // once loaded on at least one node

            // Assign the results to the instance fields
            _incrementWithExpire = scripts.IncrementWithExpire;
            _removeIfEqual = scripts.RemoveIfEqual;
            _replaceIfEqual = scripts.ReplaceIfEqual;
            _setIfHigher = scripts.SetIfHigher;
            _setIfLower = scripts.SetIfLower;
            _getAllExpiration = scripts.GetAllExpiration;
            _setAllExpiration = scripts.SetAllExpiration;

            _scriptsLoaded = true;
        }
    }

    private void ConnectionMultiplexerOnConnectionRestored(object sender, ConnectionFailedEventArgs connectionFailedEventArgs)
    {
        _logger.LogInformation("Redis connection restored");
        _scriptsLoaded = false;
        _supportsMsetEx = null; // Re-check version on next call
    }

    private void ConnectionMultiplexerOnConnectionFailed(object sender, ConnectionFailedEventArgs connectionFailedEventArgs)
    {
        _logger.LogWarning("Redis connection failed: {FailureType}", connectionFailedEventArgs.FailureType);
    }

    public void Dispose()
    {
        _options.ConnectionMultiplexer.ConnectionRestored -= ConnectionMultiplexerOnConnectionRestored;
        _options.ConnectionMultiplexer.ConnectionFailed -= ConnectionMultiplexerOnConnectionFailed;
    }

    ISerializer IHaveSerializer.Serializer => _options.Serializer;

    private static readonly string IncrementWithScript = EmbeddedResourceLoader.GetEmbeddedResource("Foundatio.Redis.Scripts.IncrementWithExpire.lua");
    private static readonly string RemoveIfEqualScript = EmbeddedResourceLoader.GetEmbeddedResource("Foundatio.Redis.Scripts.RemoveIfEqual.lua");
    private static readonly string ReplaceIfEqualScript = EmbeddedResourceLoader.GetEmbeddedResource("Foundatio.Redis.Scripts.ReplaceIfEqual.lua");
    private static readonly string SetIfHigherScript = EmbeddedResourceLoader.GetEmbeddedResource("Foundatio.Redis.Scripts.SetIfHigher.lua");
    private static readonly string SetIfLowerScript = EmbeddedResourceLoader.GetEmbeddedResource("Foundatio.Redis.Scripts.SetIfLower.lua");
    private static readonly string GetAllExpirationScript = EmbeddedResourceLoader.GetEmbeddedResource("Foundatio.Redis.Scripts.GetAllExpiration.lua");
    private static readonly string SetAllExpirationScript = EmbeddedResourceLoader.GetEmbeddedResource("Foundatio.Redis.Scripts.SetAllExpiration.lua");
}

