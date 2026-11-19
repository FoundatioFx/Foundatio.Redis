using System;
using System.Linq;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Foundatio.Caching;
using StackExchange.Redis;

namespace Foundatio.Benchmarks.Caching;

[MemoryDiagnoser]
public class RedisHybridListBenchmarks
{
    private const int BatchSize = 64;
    private readonly int[] _member = [1];
    private readonly string _prefix = $"{{{RedisBenchmarkEnvironment.CreatePrefix()}}}";
    private ConnectionMultiplexer _connection = null!;
    private RedisHybridCacheClient _cache = null!;
    private string[] _coldKeys = null!;
    private string _warmKey = null!;

    [GlobalSetup]
    public async Task SetupAsync()
    {
        _connection = await RedisBenchmarkEnvironment.ConnectAsync();
        _cache = new RedisHybridCacheClient(o => o.ConnectionMultiplexer(_connection).RedisChannelName(_prefix));
        _coldKeys = Enumerable.Range(0, BatchSize).Select(i => $"{_prefix}:cold:{i}").ToArray();
        _warmKey = $"{_prefix}:warm";
        await _cache.ListAddAsync(_warmKey, new[] { 0 });
    }

    [Benchmark(OperationsPerInvoke = BatchSize)]
    public async Task AddToNewListAsync()
    {
        long added = 0;
        foreach (string key in _coldKeys)
            added += await _cache.ListAddAsync(key, _member);

        if (added != BatchSize)
            throw new InvalidOperationException("Expected one new member in every cold list");
    }

    [IterationCleanup(Target = nameof(AddToNewListAsync))]
    public Task RemoveColdListsAsync() => _cache.RemoveAllAsync(_coldKeys);

    [Benchmark]
    public async Task AddAndRemoveWarmMemberAsync()
    {
        long added = await _cache.ListAddAsync(_warmKey, _member);
        long removed = await _cache.ListRemoveAsync(_warmKey, _member);
        if (added != 1 || removed != 1)
            throw new InvalidOperationException("Expected one member added to and removed from the warm list");
    }

    [GlobalCleanup]
    public async Task CleanupAsync()
    {
        await _cache.RemoveAllAsync(_coldKeys);
        await _cache.RemoveAsync(_warmKey);
        _cache.Dispose();
        _connection.Dispose();
    }
}
