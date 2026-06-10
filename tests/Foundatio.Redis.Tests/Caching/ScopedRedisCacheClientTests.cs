using System;
using System.Threading.Tasks;
using Foundatio.Caching;
using Foundatio.Redis.Tests.Extensions;
using Foundatio.Tests.Caching;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;
using Xunit;

namespace Foundatio.Redis.Tests.Caching;

public class ScopedRedisCacheClientTests : CacheClientTestsBase, IAsyncLifetime
{
    protected virtual RedisProtocol? Protocol => null;

    public ScopedRedisCacheClientTests(ITestOutputHelper output) : base(output)
    {
    }

    protected override ICacheClient? GetCacheClient(bool shouldThrowOnSerializationError = true)
    {
        var muxer = SharedConnection.GetMuxer(Log, Protocol);
        if (muxer is null)
            return null;

        return new ScopedCacheClient(new RedisCacheClient(o => o.ConnectionMultiplexer(muxer).LoggerFactory(Log).ShouldThrowOnSerializationError(shouldThrowOnSerializationError)), "scoped");
    }

    [Fact]
    public override Task AddAsync_WithConcurrentRequests_OnlyOneSucceeds()
    {
        return base.AddAsync_WithConcurrentRequests_OnlyOneSucceeds();
    }

    [Fact]
    public override Task AddAsync_WithExpiration_SetsExpirationCorrectly()
    {
        return base.AddAsync_WithExpiration_SetsExpirationCorrectly();
    }

    [Fact(Skip = "Performance Test")]
    public override Task CacheOperations_WithMultipleTypes_MeasuresThroughput()
    {
        return base.CacheOperations_WithMultipleTypes_MeasuresThroughput();
    }

    [Fact(Skip = "Performance Test")]
    public override Task CacheOperations_WithRepeatedSetAndGet_MeasuresThroughput()
    {
        return base.CacheOperations_WithRepeatedSetAndGet_MeasuresThroughput();
    }

    [Fact]
    public override Task ExistsAsync_WithVariousKeys_ReturnsCorrectExistenceStatus()
    {
        return base.ExistsAsync_WithVariousKeys_ReturnsCorrectExistenceStatus();
    }

    [Fact]
    public override Task ExistsAsync_WithExpiredKey_ReturnsFalse() => base.ExistsAsync_WithExpiredKey_ReturnsFalse();

    [Fact]
    public override Task ExistsAsync_WithScopedCache_ChecksOnlyWithinScope() => base.ExistsAsync_WithScopedCache_ChecksOnlyWithinScope();

    [Fact]
    public override Task ExistsAsync_WithInvalidKey_ThrowsArgumentException() => base.ExistsAsync_WithInvalidKey_ThrowsArgumentException();

    [Fact]
    public override Task GetAllAsync_WithInvalidKeys_ValidatesCorrectly()
    {
        return base.GetAllAsync_WithInvalidKeys_ValidatesCorrectly();
    }

    [Fact]
    public override Task GetAllAsync_WithMultipleKeys_ReturnsCorrectValues()
    {
        return base.GetAllAsync_WithMultipleKeys_ReturnsCorrectValues();
    }

    [Fact]
    public override Task GetAllAsync_WithScopedCache_ReturnsUnscopedKeys()
    {
        return base.GetAllAsync_WithScopedCache_ReturnsUnscopedKeys();
    }

    [Theory]
    [InlineData(1000)]
    [InlineData(10000)]
    public override Task GetAllExpirationAsync_WithLargeNumberOfKeys_ReturnsAllExpirations(int count)
    {
        return base.GetAllExpirationAsync_WithLargeNumberOfKeys_ReturnsAllExpirations(count);
    }

    [Fact]
    public override Task GetAllExpirationAsync_WithInvalidKeys_ValidatesCorrectly()
    {
        return base.GetAllExpirationAsync_WithInvalidKeys_ValidatesCorrectly();
    }

    [Fact]
    public override Task GetAllExpirationAsync_WithMixedKeys_ReturnsExpectedResults()
    {
        return base.GetAllExpirationAsync_WithMixedKeys_ReturnsExpectedResults();
    }

    [Fact]
    public override Task GetAsync_WithNumericTypeConversion_ConvertsBetweenTypes()
    {
        return base.GetAsync_WithNumericTypeConversion_ConvertsBetweenTypes();
    }

    [Fact]
    public override Task GetExpirationAsync_WithVariousKeyStates_ReturnsExpectedExpiration()
    {
        return base.GetExpirationAsync_WithVariousKeyStates_ReturnsExpectedExpiration();
    }

    [Fact]
    public override Task GetExpirationAsync_WithInvalidKey_ThrowsArgumentException()
    {
        return base.GetExpirationAsync_WithInvalidKey_ThrowsArgumentException();
    }

    [Fact]
    public override Task GetListAsync_WithExpiredItems_RemovesExpiredAndReturnsActive()
    {
        return base.GetListAsync_WithExpiredItems_RemovesExpiredAndReturnsActive();
    }

    [Fact]
    public override Task GetListAsync_WithInvalidKey_ThrowsArgumentException()
    {
        return base.GetListAsync_WithInvalidKey_ThrowsArgumentException();
    }

    [Fact]
    public override Task GetListAsync_WithPaging_ReturnsCorrectResults()
    {
        return base.GetListAsync_WithPaging_ReturnsCorrectResults();
    }

    [Fact]
    public override Task GetUnixTimeMillisecondsAsync_WithLocalDateTime_ReturnsCorrectly()
    {
        return base.GetUnixTimeMillisecondsAsync_WithLocalDateTime_ReturnsCorrectly();
    }

    [Fact]
    public override Task GetUnixTimeMillisecondsAsync_WithUtcDateTime_ReturnsCorrectly()
    {
        return base.GetUnixTimeMillisecondsAsync_WithUtcDateTime_ReturnsCorrectly();
    }

    [Fact]
    public override Task GetUnixTimeSecondsAsync_WithUtcDateTime_ReturnsCorrectly()
    {
        return base.GetUnixTimeSecondsAsync_WithUtcDateTime_ReturnsCorrectly();
    }

    [Fact]
    public override Task IncrementAsync_WithExpiration_SetsExpirationCorrectly()
    {
        return base.IncrementAsync_WithExpiration_SetsExpirationCorrectly();
    }

    [Fact]
    public override Task IncrementAsync_WithInvalidKey_ThrowsException()
    {
        return base.IncrementAsync_WithInvalidKey_ThrowsException();
    }

    [Fact]
    public override Task IncrementAsync_WithKey_IncrementsCorrectly()
    {
        return base.IncrementAsync_WithKey_IncrementsCorrectly();
    }

    [Fact]
    public override Task IncrementAsync_WithScopedCache_WorksWithinScope()
    {
        return base.IncrementAsync_WithScopedCache_WorksWithinScope();
    }

    [Fact]
    public override Task ListAddAsync_WithExpiration_SetsExpirationCorrectly()
    {
        return base.ListAddAsync_WithExpiration_SetsExpirationCorrectly();
    }

    [Fact]
    public override Task ListAddAsync_WithInvalidArguments_ThrowsException()
    {
        return base.ListAddAsync_WithInvalidArguments_ThrowsException();
    }

    [Fact]
    public override Task ListAddAsync_WithSingleString_StoresAsStringNotCharArray()
    {
        return base.ListAddAsync_WithSingleString_StoresAsStringNotCharArray();
    }

    [Fact]
    public override Task ListAddAsync_WithVariousInputs_HandlesCorrectly()
    {
        return base.ListAddAsync_WithVariousInputs_HandlesCorrectly();
    }

    [Fact]
    public override Task ListRemoveAsync_WithInvalidInputs_ThrowsAppropriateException()
    {
        return base.ListRemoveAsync_WithInvalidInputs_ThrowsAppropriateException();
    }

    [Fact]
    public override Task ListRemoveAsync_WithValidValues_RemovesKeyWhenEmpty()
    {
        return base.ListRemoveAsync_WithValidValues_RemovesKeyWhenEmpty();
    }

    [Fact]
    public override Task ListRemoveAsync_WithValues_RemovesCorrectly()
    {
        return base.ListRemoveAsync_WithValues_RemovesCorrectly();
    }

    [Fact]
    public override Task ListRemoveAsync_WithMultipleItems_RemovesCorrectly()
    {
        return base.ListRemoveAsync_WithMultipleItems_RemovesCorrectly();
    }

    [Fact]
    public override Task RemoveAllAsync_WithInvalidKeys_ValidatesCorrectly()
    {
        return base.RemoveAllAsync_WithInvalidKeys_ValidatesCorrectly();
    }

    [Fact]
    public override Task RemoveAllAsync_WithLargeNumberOfKeys_RemovesAllKeysEfficiently()
    {
        return base.RemoveAllAsync_WithLargeNumberOfKeys_RemovesAllKeysEfficiently();
    }

    [Fact]
    public override Task RemoveAllAsync_WithScopedCache_AffectsOnlyScopedKeys()
    {
        return base.RemoveAllAsync_WithScopedCache_AffectsOnlyScopedKeys();
    }

    [Fact]
    public override Task RemoveAllAsync_WithSpecificKeyCollection_RemovesOnlySpecifiedKeys()
    {
        return base.RemoveAllAsync_WithSpecificKeyCollection_RemovesOnlySpecifiedKeys();
    }

    [Fact]
    public override Task RemoveAsync_WithInvalidKey_ThrowsArgumentException()
    {
        return base.RemoveAsync_WithInvalidKey_ThrowsArgumentException();
    }

    [Fact]
    public override Task RemoveAsync_WithNonExistentKey_ReturnsFalse()
    {
        return base.RemoveAsync_WithNonExistentKey_ReturnsFalse();
    }

    [Fact]
    public override Task RemoveAsync_WithExpiredKey_KeyDoesNotExist()
    {
        return base.RemoveAsync_WithExpiredKey_KeyDoesNotExist();
    }

    [Fact]
    public override Task RemoveAsync_WithScopedCache_RemovesOnlyWithinScope()
    {
        return base.RemoveAsync_WithScopedCache_RemovesOnlyWithinScope();
    }

    [Fact]
    public override Task RemoveAsync_WithValidKey_RemovesSuccessfully()
    {
        return base.RemoveAsync_WithValidKey_RemovesSuccessfully();
    }

    [Theory]
    [InlineData("snowboard", 1)] // Exact key match
    [InlineData("s", 1)] // Partial prefix match
    [InlineData(null, 1)] // Null prefix (all keys in scope)
    [InlineData("", 1)] // Empty prefix (all keys in scope)
    public override Task RemoveByPrefixAsync_FromScopedCache_RemovesOnlyScopedKeys(string? prefixToRemove, int expectedRemovedCount)
    {
        return base.RemoveByPrefixAsync_FromScopedCache_RemovesOnlyScopedKeys(prefixToRemove, expectedRemovedCount);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    public override Task RemoveByPrefixAsync_NullOrEmptyPrefixWithScopedCache_RemovesCorrectKeys(string? prefix)
    {
        return base.RemoveByPrefixAsync_NullOrEmptyPrefixWithScopedCache_RemovesCorrectKeys(prefix);
    }

    [Fact]
    public override Task RemoveByPrefixAsync_PartialPrefixWithScopedCache_RemovesMatchingKeys()
    {
        return base.RemoveByPrefixAsync_PartialPrefixWithScopedCache_RemovesMatchingKeys();
    }

    [Fact]
    public override Task RemoveByPrefixAsync_WithAsteriskPrefix_TreatedAsLiteral()
    {
        return base.RemoveByPrefixAsync_WithAsteriskPrefix_TreatedAsLiteral();
    }

    [Theory]
    [MemberData(nameof(GetLineEndingPrefixes))]
    public override Task RemoveByPrefixAsync_WithLineEndingPrefix_TreatsAsLiteral(string lineEndingPrefix)
    {
        return base.RemoveByPrefixAsync_WithLineEndingPrefix_TreatsAsLiteral(lineEndingPrefix);
    }

    [Fact]
    public override Task RemoveByPrefixAsync_WithMatchingPrefix_RemovesOnlyMatchingKeys()
    {
        return base.RemoveByPrefixAsync_WithMatchingPrefix_RemovesOnlyMatchingKeys();
    }

    [Theory]
    [InlineData(50)]
    [InlineData(500)]
    [InlineData(5000)]
    [InlineData(50000)]
    public override Task RemoveByPrefixAsync_WithMultipleMatchingKeys_RemovesOnlyPrefixedKeys(int count)
    {
        return base.RemoveByPrefixAsync_WithMultipleMatchingKeys_RemovesOnlyPrefixedKeys(count);
    }

    [Fact]
    public override Task RemoveByPrefixAsync_WithNullOrEmptyPrefix_RemovesAllKeys()
    {
        return base.RemoveByPrefixAsync_WithNullOrEmptyPrefix_RemovesAllKeys();
    }

    [Theory]
    [MemberData(nameof(GetRegexSpecialCharacters))]
    public override Task RemoveByPrefixAsync_WithRegexMetacharacter_TreatsAsLiteral(string specialChar)
    {
        return base.RemoveByPrefixAsync_WithRegexMetacharacter_TreatsAsLiteral(specialChar);
    }

    [Fact]
    public override Task RemoveByPrefixAsync_WithScopedCache_AffectsOnlyScopedKeys()
    {
        return base.RemoveByPrefixAsync_WithScopedCache_AffectsOnlyScopedKeys();
    }

    [Theory]
    [MemberData(nameof(GetSpecialPrefixes))]
    public override Task RemoveByPrefixAsync_WithSpecialCharacterPrefix_TreatsAsLiteral(string specialPrefix)
    {
        return base.RemoveByPrefixAsync_WithSpecialCharacterPrefix_TreatsAsLiteral(specialPrefix);
    }

    [Theory]
    [MemberData(nameof(GetWildcardPatterns))]
    public override Task RemoveByPrefixAsync_WithWildcardPattern_TreatsAsLiteral(string pattern)
    {
        return base.RemoveByPrefixAsync_WithWildcardPattern_TreatsAsLiteral(pattern);
    }

    [Fact]
    public override Task RemoveIfEqualAsync_WithInvalidKey_ThrowsArgumentException()
    {
        return base.RemoveIfEqualAsync_WithInvalidKey_ThrowsArgumentException();
    }

    [Fact]
    public override Task RemoveIfEqualAsync_WithMatchingValue_ReturnsTrueAndRemoves()
    {
        return base.RemoveIfEqualAsync_WithMatchingValue_ReturnsTrueAndRemoves();
    }

    [Fact]
    public override Task RemoveIfEqualAsync_WithMismatchedValue_ReturnsFalseAndDoesNotRemove()
    {
        return base.RemoveIfEqualAsync_WithMismatchedValue_ReturnsFalseAndDoesNotRemove();
    }

    [Fact]
    public override Task ReplaceAsync_WithExistingKey_ReturnsTrueAndReplacesValue()
    {
        return base.ReplaceAsync_WithExistingKey_ReturnsTrueAndReplacesValue();
    }

    [Fact]
    public override Task ReplaceAsync_WithInvalidKey_ThrowsException()
    {
        return base.ReplaceAsync_WithInvalidKey_ThrowsException();
    }

    [Fact]
    public override Task ReplaceAsync_WithNonExistentKey_ReturnsFalseAndDoesNotCreateKey()
    {
        return base.ReplaceAsync_WithNonExistentKey_ReturnsFalseAndDoesNotCreateKey();
    }

    [Fact]
    public override Task ReplaceAsync_WithExpiration_SetsExpirationCorrectly()
    {
        return base.ReplaceAsync_WithExpiration_SetsExpirationCorrectly();
    }

    [Fact]
    public override Task ReplaceIfEqualAsync_WithExpiration_SetsExpirationCorrectly()
    {
        return base.ReplaceIfEqualAsync_WithExpiration_SetsExpirationCorrectly();
    }

    [Fact]
    public override Task ReplaceIfEqualAsync_WithInvalidKey_ThrowsArgumentException()
    {
        return base.ReplaceIfEqualAsync_WithInvalidKey_ThrowsArgumentException();
    }

    [Fact]
    public override Task ReplaceIfEqualAsync_WithMatchingOldValue_ReturnsTrueAndReplacesValue()
    {
        return base.ReplaceIfEqualAsync_WithMatchingOldValue_ReturnsTrueAndReplacesValue();
    }

    [Fact]
    public override Task ReplaceIfEqualAsync_WithMismatchedOldValue_ReturnsFalseAndDoesNotReplace()
    {
        return base.ReplaceIfEqualAsync_WithMismatchedOldValue_ReturnsFalseAndDoesNotReplace();
    }

    [Fact(Skip = "Performance Test")]
    public override Task Serialization_WithComplexObjectsAndValidation_MeasuresThroughput()
    {
        return base.Serialization_WithComplexObjectsAndValidation_MeasuresThroughput();
    }

    [Fact(Skip = "Performance Test")]
    public override Task Serialization_WithSimpleObjectsAndValidation_MeasuresThroughput()
    {
        return base.Serialization_WithSimpleObjectsAndValidation_MeasuresThroughput();
    }

    [Fact]
    public override Task SetAllAsync_WithExpiration_SetsExpirationCorrectly()
    {
        return base.SetAllAsync_WithExpiration_SetsExpirationCorrectly();
    }

    [Fact]
    public override Task SetAllAsync_WithInvalidItems_ValidatesCorrectly()
    {
        return base.SetAllAsync_WithInvalidItems_ValidatesCorrectly();
    }

    [Fact]
    public override Task SetAllAsync_WithLargeNumberOfKeys_MeasuresThroughput()
    {
        return base.SetAllAsync_WithLargeNumberOfKeys_MeasuresThroughput();
    }

    [Fact]
    public override Task SetAllExpirationAsync_WithInvalidItems_ValidatesCorrectly()
    {
        return base.SetAllExpirationAsync_WithInvalidItems_ValidatesCorrectly();
    }

    [Theory]
    [InlineData(1000)]
    [InlineData(10000)]
    public override Task SetAllExpirationAsync_WithLargeNumberOfKeys_SetsAllExpirations(int count)
    {
        return base.SetAllExpirationAsync_WithLargeNumberOfKeys_SetsAllExpirations(count);
    }

    [Fact]
    public override Task SetAllExpirationAsync_WithExpiration_SetsExpirationCorrectly()
    {
        return base.SetAllExpirationAsync_WithExpiration_SetsExpirationCorrectly();
    }

    [Fact]
    public override Task SetAsync_WithComplexObject_StoresCorrectly()
    {
        return base.SetAsync_WithComplexObject_StoresCorrectly();
    }

    [Fact]
    public override Task SetAsync_WithExpiration_SetsExpirationCorrectly()
    {
        return base.SetAsync_WithExpiration_SetsExpirationCorrectly();
    }

    [Fact]
    public override Task SetAsync_WithInvalidKey_ThrowsArgumentException()
    {
        return base.SetAsync_WithInvalidKey_ThrowsArgumentException();
    }

    [Fact]
    public override Task SetAsync_WithLargeNumbersAndExpiration_PreservesValues()
    {
        return base.SetAsync_WithLargeNumbersAndExpiration_PreservesValues();
    }

    [Fact]
    public override Task SetAsync_WithNullValue_StoresAsNullValue()
    {
        return base.SetAsync_WithNullValue_StoresAsNullValue();
    }

    [Fact]
    public override Task SetAsync_WithScopedCaches_IsolatesKeys()
    {
        return base.SetAsync_WithScopedCaches_IsolatesKeys();
    }

    [Fact]
    public override Task SetAsync_WithShortExpiration_ExpiresCorrectly()
    {
        return base.SetAsync_WithShortExpiration_ExpiresCorrectly();
    }

    [Fact]
    public override Task SetExpirationAsync_WithInvalidKey_ThrowsArgumentException()
    {
        return base.SetExpirationAsync_WithInvalidKey_ThrowsArgumentException();
    }

    [Fact]
    public override Task SetExpirationAsync_WithExpiration_SetsExpirationCorrectly()
    {
        return base.SetExpirationAsync_WithExpiration_SetsExpirationCorrectly();
    }

    [Fact]
    public override Task SetIfHigherAsync_WithDateTime_UpdatesWhenHigher()
    {
        return base.SetIfHigherAsync_WithDateTime_UpdatesWhenHigher();
    }

    [Fact]
    public override Task SetIfHigherAsync_WithLargeNumbers_HandlesCorrectly()
    {
        return base.SetIfHigherAsync_WithLargeNumbers_HandlesCorrectly();
    }

    [Fact]
    public override Task SetIfHigherAsync_WithExpiration_SetsExpirationCorrectly()
    {
        return base.SetIfHigherAsync_WithExpiration_SetsExpirationCorrectly();
    }

    [Fact]
    public override Task SetIfLowerAsync_WithDateTime_UpdatesWhenLower()
    {
        return base.SetIfLowerAsync_WithDateTime_UpdatesWhenLower();
    }

    [Fact]
    public override Task SetIfLowerAsync_WithLargeNumbers_HandlesCorrectly()
    {
        return base.SetIfLowerAsync_WithLargeNumbers_HandlesCorrectly();
    }

    [Fact]
    public override Task SetIfLowerAsync_WithExpiration_SetsExpirationCorrectly()
    {
        return base.SetIfLowerAsync_WithExpiration_SetsExpirationCorrectly();
    }

    [Fact]
    public override Task SetUnixTimeMillisecondsAsync_WithLocalDateTime_StoresCorrectly()
    {
        return base.SetUnixTimeMillisecondsAsync_WithLocalDateTime_StoresCorrectly();
    }

    [Fact]
    public override Task SetUnixTimeSecondsAsync_WithUtcDateTime_StoresCorrectly()
    {
        return base.SetUnixTimeSecondsAsync_WithUtcDateTime_StoresCorrectly();
    }

    public override async ValueTask InitializeAsync()
    {
        await base.InitializeAsync();
        _logger.LogDebug("Initializing");
        var muxer = SharedConnection.GetMuxer(Log, Protocol);
        if (muxer is null)
            return;

        await muxer.FlushAllAsync();
    }

    public override async ValueTask DisposeAsync()
    {
        await base.DisposeAsync();
        _logger.LogDebug("Disposing");
    }
}

public class ScopedRedisCacheClientResp3Tests : ScopedRedisCacheClientTests
{
    public ScopedRedisCacheClientResp3Tests(ITestOutputHelper output) : base(output) { }
    protected override RedisProtocol? Protocol => RedisProtocol.Resp3;

    [Fact]
    public override async Task ListAddAsync_WithExpiration_SetsExpirationCorrectly()
    {
        var cache = GetCacheClient();
        if (cache is null)
            return;

        using (cache)
        {
            await cache.RemoveAllAsync();

            // Past expiration on new key
            long result = await cache.ListAddAsync("list-past-exp-new", [1], TimeSpan.FromMilliseconds(-1));
            Assert.Equal(0, result);
            Assert.False(await cache.ExistsAsync("list-past-exp-new"));
            Assert.False((await cache.GetListAsync<int>("list-past-exp-new")).HasValue);

            // Past expiration on existing key: only removes the values being added
            Assert.Equal(1, await cache.ListAddAsync("list-past-exp-existing", [1]));
            Assert.True(await cache.ExistsAsync("list-past-exp-existing"));
            result = await cache.ListAddAsync("list-past-exp-existing", [2], TimeSpan.FromSeconds(-1));
            Assert.Equal(0, result);
            Assert.True(await cache.ExistsAsync("list-past-exp-existing"));
            var existingList = await cache.GetListAsync<int>("list-past-exp-existing");
            Assert.True(existingList.HasValue);
            Assert.Single(existingList.Value);
            Assert.Contains(1, existingList.Value);

            // Past expiration on existing key with multiple items
            Assert.Equal(1, await cache.ListAddAsync("list-past-exp-multi", [1]));
            Assert.Equal(1, await cache.ListAddAsync("list-past-exp-multi", [2]));
            Assert.Equal(2, (await cache.GetListAsync<int>("list-past-exp-multi")).Value.Count);
            result = await cache.ListAddAsync("list-past-exp-multi", [3], TimeSpan.FromSeconds(-1));
            Assert.Equal(0, result);
            Assert.True(await cache.ExistsAsync("list-past-exp-multi"));
            var multiList = await cache.GetListAsync<int>("list-past-exp-multi");
            Assert.Equal(2, multiList.Value.Count);
            Assert.Contains(1, multiList.Value);
            Assert.Contains(2, multiList.Value);

            // Past expiration removes existing item if it matches
            Assert.Equal(1, await cache.ListAddAsync("list-past-exp-remove", [1]));
            Assert.Equal(1, await cache.ListAddAsync("list-past-exp-remove", [2]));
            result = await cache.ListAddAsync("list-past-exp-remove", [1], TimeSpan.FromSeconds(-1));
            Assert.Equal(0, result);
            Assert.True(await cache.ExistsAsync("list-past-exp-remove"));
            var removeList = await cache.GetListAsync<int>("list-past-exp-remove");
            Assert.Single(removeList.Value);
            Assert.Contains(2, removeList.Value);
            Assert.DoesNotContain(1, removeList.Value);

            // Zero expiration: treated as expired
            result = await cache.ListAddAsync("list-zero-exp", [1], TimeSpan.Zero);
            Assert.Equal(0, result);
            Assert.False(await cache.ExistsAsync("list-zero-exp"));
            Assert.False((await cache.GetListAsync<int>("list-zero-exp")).HasValue);

            // Max expiration: no expiration
            result = await cache.ListAddAsync("list-max-exp", [1, 2, 3], TimeSpan.MaxValue);
            Assert.Equal(3, result);
            Assert.True(await cache.ExistsAsync("list-max-exp"));
            var listValue = await cache.GetListAsync<int>("list-max-exp");
            Assert.True(listValue.HasValue);
            Assert.Equal(3, listValue.Value.Count);
            var expiration = await cache.GetExpirationAsync("list-max-exp");
            Assert.Null(expiration);

            // Normal expiration
            result = await cache.ListAddAsync("list-normal-exp", [1, 2, 3], TimeSpan.FromHours(1));
            Assert.Equal(3, result);
            Assert.True(await cache.ExistsAsync("list-normal-exp"));
            listValue = await cache.GetListAsync<int>("list-normal-exp");
            Assert.True(listValue.HasValue);
            Assert.Equal(3, listValue.Value.Count);
            expiration = await cache.GetExpirationAsync("list-normal-exp");
            Assert.NotNull(expiration);
            Assert.InRange(expiration.Value, TimeSpan.FromMinutes(59), TimeSpan.FromHours(1));

            // Staggered expiration with wider margins to avoid CI timing flakes
            const string key = "list:staggered-expiration";
            Assert.Equal(1, await cache.ListAddAsync(key, [2], TimeSpan.FromMilliseconds(250)));
            Assert.Equal(1, await cache.ListAddAsync(key, [3], TimeSpan.FromSeconds(5)));

            var cacheValue = await cache.GetListAsync<int>(key);
            Assert.True(cacheValue.HasValue);
            Assert.Equal(2, cacheValue.Value.Count);

            await Task.Delay(500, TestCancellationToken);
            cacheValue = await cache.GetListAsync<int>(key);
            Assert.True(cacheValue.HasValue);
            Assert.Single(cacheValue.Value);
            Assert.Contains(3, cacheValue.Value);
            Assert.DoesNotContain(2, cacheValue.Value);

            await Task.Delay(TimeSpan.FromSeconds(5), TestCancellationToken);
            Assert.False(await cache.ExistsAsync(key));

            // Null expiration: removes expiration
            result = await cache.ListAddAsync("list-null-exp", [1, 2], TimeSpan.FromHours(1));
            Assert.Equal(2, result);
            Assert.True(await cache.ExistsAsync("list-null-exp"));
            expiration = await cache.GetExpirationAsync("list-null-exp");
            Assert.NotNull(expiration);

            result = await cache.ListAddAsync("list-null-exp", [3]);
            Assert.Equal(1, result);
            Assert.True(await cache.ExistsAsync("list-null-exp"));
            listValue = await cache.GetListAsync<int>("list-null-exp");
            Assert.True(listValue.HasValue);
            Assert.Equal(3, listValue.Value.Count);
            expiration = await cache.GetExpirationAsync("list-null-exp");
            Assert.Null(expiration);
        }
    }
}
