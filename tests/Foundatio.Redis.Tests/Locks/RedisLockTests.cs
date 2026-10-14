using System;
using System.Threading.Tasks;
using Foundatio.Caching;
using Foundatio.Lock;
using Foundatio.Messaging;
using Foundatio.Redis.Tests.Extensions;
using Foundatio.Tests.Locks;
using Microsoft.Extensions.Logging;
using Xunit;

namespace Foundatio.Redis.Tests.Locks;

public class RedisLockTests : LockTestBase, IDisposable, IAsyncLifetime
{
    private readonly string _topic = $"test-lock-{Guid.NewGuid().ToString("N")[..10]}";
    private readonly ICacheClient _cache;
    private readonly IMessageBus _messageBus;

    public RedisLockTests(ITestOutputHelper output) : base(output)
    {
        var muxer = SharedConnection.GetMuxer(Log)
            ?? throw new InvalidOperationException("Redis connection is not configured. Set the RedisConnectionString environment variable.");

        _cache = new RedisCacheClient(o => o.ConnectionMultiplexer(muxer).LoggerFactory(Log));
        _messageBus = new RedisMessageBus(o => o.Subscriber(muxer.GetSubscriber()).Topic(_topic).LoggerFactory(Log));
    }

    protected override ILockProvider? GetThrottlingLockProvider(int maxHits, TimeSpan period)
    {
        var muxer = SharedConnection.GetMuxer(Log);
        if (muxer is null)
            return null;

        return new ThrottlingLockProvider(_cache, maxHits, period, null, null, Log);
    }

    protected override ILockProvider? GetLockProvider()
    {
        var muxer = SharedConnection.GetMuxer(Log);
        if (muxer is null)
            return null;

        return new CacheLockProvider(_cache, _messageBus, null, null, Log);
    }

    [Fact]
    public override Task CanAcquireAndReleaseLockAsync()
    {
        return base.CanAcquireAndReleaseLockAsync();
    }

    [Fact]
    public override Task AcquireAsync_WithReleaseOnDisposeFalse_DoesNotReleaseOnDispose()
    {
        return base.AcquireAsync_WithReleaseOnDisposeFalse_DoesNotReleaseOnDispose();
    }

    [Fact]
    public override Task LockWillTimeoutAsync()
    {
        return base.LockWillTimeoutAsync();
    }

    [Fact]
    public override Task Lock_AcquiredTimeUtc_ReturnsValidTimestamp()
    {
        return base.Lock_AcquiredTimeUtc_ReturnsValidTimestamp();
    }

    [Fact]
    public override Task Lock_LockIdAndResource_ReturnCorrectValues()
    {
        return base.Lock_LockIdAndResource_ReturnCorrectValues();
    }

    [Fact]
    public override Task LockOneAtATimeAsync()
    {
        return base.LockOneAtATimeAsync();
    }

    [Fact]
    public override Task CanAcquireMultipleResources()
    {
        return base.CanAcquireMultipleResources();
    }

    [Fact]
    public override Task CanAcquireLocksInParallel()
    {
        return base.CanAcquireLocksInParallel();
    }

    [Fact]
    public override Task CanAcquireScopedLocksInParallel()
    {
        return base.CanAcquireScopedLocksInParallel();
    }

    [Fact]
    public override Task CanAcquireMultipleLocksInParallel()
    {
        return base.CanAcquireMultipleLocksInParallel();
    }

    [Fact]
    public override Task CanAcquireMultipleScopedResources()
    {
        return base.CanAcquireMultipleScopedResources();
    }

    [Fact]
    public override Task WillThrottleCallsAsync()
    {
        return base.WillThrottleCallsAsync();
    }

    [Fact]
    public override Task CanReleaseLockMultipleTimes()
    {
        return base.CanReleaseLockMultipleTimes();
    }

    [Fact]
    public override Task ReleaseAsync_WithForceRelease_ReleasesLockWithoutLockId()
    {
        return base.ReleaseAsync_WithForceRelease_ReleasesLockWithoutLockId();
    }

    [Fact]
    public override Task TryUsingAsync_WithSuccessfulAction_ExecutesAndReleasesLock()
    {
        return base.TryUsingAsync_WithSuccessfulAction_ExecutesAndReleasesLock();
    }

    [Fact]
    public override Task LockWontTimeoutEarly()
    {
        return base.LockWontTimeoutEarly();
    }

    [Fact]
    public override Task AcquireAsync_AfterPeriodExhausted_RecoversWithinNextPeriodAsync()
    {
        return base.AcquireAsync_AfterPeriodExhausted_RecoversWithinNextPeriodAsync();
    }

    [Fact]
    public override Task AcquireAsync_MultiResource_ThrowsWhenAnyLockUnavailableAsync()
    {
        return base.AcquireAsync_MultiResource_ThrowsWhenAnyLockUnavailableAsync();
    }

    [Fact]
    public override Task AcquireAsync_ThrowsWhenCancellationTokenCancelledAsync()
    {
        return base.AcquireAsync_ThrowsWhenCancellationTokenCancelledAsync();
    }

    [Fact]
    public override Task AcquireAsync_ThrowsWhenLockNotAvailableAsync()
    {
        return base.AcquireAsync_ThrowsWhenLockNotAvailableAsync();
    }

    [Fact]
    public override Task RenewAsync_AfterRelease_ThrowsLockExceptionAndDoesNotRecreateLock()
    {
        return base.RenewAsync_AfterRelease_ThrowsLockExceptionAndDoesNotRecreateLock();
    }

    [Theory]
    [InlineData(-1)]
    [InlineData(0)]
    [InlineData(4)]
    public override Task RenewAsync_WithInvalidDuration_PreservesCurrentOwner(int milliseconds)
    {
        return base.RenewAsync_WithInvalidDuration_PreservesCurrentOwner(milliseconds);
    }

    [Fact]
    public override Task RenewAsync_WithMissingLock_ThrowsLockException()
    {
        return base.RenewAsync_WithMissingLock_ThrowsLockException();
    }

    [Fact]
    public override Task RenewAsync_WithMultipleResources_WhenOneLockReplaced_ThrowsLockExceptionAndPreservesCurrentOwner()
    {
        return base.RenewAsync_WithMultipleResources_WhenOneLockReplaced_ThrowsLockExceptionAndPreservesCurrentOwner();
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public override Task RenewAsync_WithReplacedOwner_ThrowsLockExceptionAndPreservesCurrentOwner(bool scoped)
    {
        return base.RenewAsync_WithReplacedOwner_ThrowsLockExceptionAndPreservesCurrentOwner(scoped);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public override Task TryAcquireAsync_WithMultipleResources_WhenResourceNamesShareSuffix_ReleasesAcquiredLocksAndReturnsNull(bool scoped)
    {
        return base.TryAcquireAsync_WithMultipleResources_WhenResourceNamesShareSuffix_ReleasesAcquiredLocksAndReturnsNull(scoped);
    }

    public void Dispose()
    {
        _cache.Dispose();
        _messageBus.Dispose();
    }

    public override async ValueTask InitializeAsync()
    {
        await base.InitializeAsync();
        _logger.LogDebug("Initializing");
        var muxer = SharedConnection.GetMuxer(Log);
        if (muxer is null)
            return;

        await muxer.FlushAllAsync();
    }

    public override async ValueTask DisposeAsync()
    {
        await base.DisposeAsync();
        _logger.LogDebug("Disposing");
        Dispose();
    }
}
