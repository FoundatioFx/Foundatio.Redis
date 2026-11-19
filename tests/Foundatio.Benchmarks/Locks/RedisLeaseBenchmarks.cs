using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Foundatio.Caching;
using Foundatio.Jobs;
using Foundatio.Lock;
using Foundatio.Messaging;
using Foundatio.Queues;
using StackExchange.Redis;

namespace Foundatio.Benchmarks.Locks;

[MemoryDiagnoser]
public class RedisLeaseBenchmarks
{
    private readonly string _prefix = $"foundatio-bench-{Guid.NewGuid():N}";
    private ConnectionMultiplexer _connection = null!;
    private RedisCacheClient _cache = null!;
    private RedisMessageBus _bus = null!;
    private CacheLockProvider _locks = null!;
    private ILock _renewable = null!;
    private string _resource = null!;
    private string[] _twoResources = null!;
    private string[] _sixteenResources = null!;
    private RedisQueue<WorkItemData> _queue = null!;
    private WorkItemJob _job = null!;

    [GlobalSetup]
    public async Task SetupAsync()
    {
        _connection = await RedisBenchmarkConnection.ConnectAsync();
        _cache = new RedisCacheClient(o => o.ConnectionMultiplexer(_connection));
        _bus = new RedisMessageBus(o => o.Subscriber(_connection.GetSubscriber()).Topic(_prefix));
        _locks = new CacheLockProvider(_cache, _bus);
        _resource = $"{_prefix}-acquire";
        _sixteenResources = Enumerable.Range(0, 16).Select(i => $"{_prefix}-{i}").ToArray();
        _twoResources = _sixteenResources[..2];
        _renewable = await _locks.AcquireAsync($"{_prefix}-renew");

        _queue = new RedisQueue<WorkItemData>(o => o.ConnectionMultiplexer(_connection).Name(_prefix));
        var handlers = new WorkItemHandlers();
        handlers.Register<bool>(new Handler(_locks, $"{_prefix}-job") { AutoRenewLockOnProgress = true });
        _job = new WorkItemJob(_queue, _bus, handlers);
    }

    [Benchmark]
    public async Task AcquireAndReleaseAsync()
    {
        var lease = await _locks.AcquireAsync(_resource);
        await lease.ReleaseAsync();
    }

    [Benchmark]
    public Task RenewAsync() => _renewable.RenewAsync();

    [Benchmark]
    [Arguments(2)]
    [Arguments(16)]
    public async Task AcquireRenewAndReleaseAsync(int resources)
    {
        var leases = await _locks.AcquireAsync(resources == 2 ? _twoResources : _sixteenResources);
        try
        {
            await leases.RenewAsync();
        }
        finally
        {
            await leases.ReleaseAsync();
        }
    }

    [Benchmark]
    [Arguments(false)]
    [Arguments(true)]
    public async Task ProcessWorkItemAsync(bool reportProgress)
    {
        await _queue.EnqueueAsync(reportProgress, includeProgressReporting: reportProgress);
        var result = await _job.RunAsync();
        if (!result.IsSuccess)
            throw new InvalidOperationException(result.Message, result.Error);
    }

    [GlobalCleanup]
    public async Task CleanupAsync()
    {
        await _renewable.ReleaseAsync();
        await _queue.DeleteQueueAsync();
        _queue.Dispose();
        _bus.Dispose();
        _cache.Dispose();
        _connection.Dispose();
    }

    private sealed class Handler(ILockProvider locks, string resource) : WorkItemHandlerBase
    {
        public override Task<ILock?> GetWorkItemLockAsync(object workItem, CancellationToken cancellationToken = default) =>
            locks.TryAcquireAsync(resource, cancellationToken: cancellationToken);

        public override Task HandleItemAsync(WorkItemContext context) =>
            context.Data is true ? context.ReportProgressAsync(50) : Task.CompletedTask;
    }
}
