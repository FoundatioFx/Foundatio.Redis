﻿using System;
using System.Threading;
using System.Threading.Tasks;
using Exceptionless;
using Foundatio.Caching;
using Foundatio.Extensions;
using Foundatio.Jobs;
using Foundatio.Lock;
using Foundatio.Messaging;
using Foundatio.Queues;
using Microsoft.Extensions.Logging;

namespace Foundatio.SampleJob;

public class PingQueueJob : QueueJobBase<PingRequest>
{
    private readonly ILockProvider _locker;
    private int _runCount;

    public PingQueueJob(IQueue<PingRequest> queue, ILoggerFactory loggerFactory, ICacheClient cacheClient, IMessageBus messageBus) : base(queue, null, loggerFactory)
    {
        AutoComplete = true;
        _locker = new CacheLockProvider(cacheClient, messageBus, null, loggerFactory);
    }

    public int RunCount => _runCount;

    protected override Task<ILock> GetQueueEntryLockAsync(IQueueEntry<PingRequest> queueEntry, CancellationToken cancellationToken = new CancellationToken())
    {
        return _locker.AcquireAsync(String.Concat("pull:", queueEntry.Value.Id),
            TimeSpan.FromMinutes(30),
            TimeSpan.FromSeconds(1));
    }

    protected override async Task<JobResult> ProcessQueueEntryAsync(QueueEntryContext<PingRequest> context)
    {
        Interlocked.Increment(ref _runCount);

        _logger.LogInformation("Got {RunCount} ping. Sending pong!", RunCount.ToOrdinal());
        await Task.Delay(TimeSpan.FromMilliseconds(1)).AnyContext();

        if (RandomData.GetBool(context.QueueEntry.Value.PercentChanceOfException))
            throw new ApplicationException("Boom!");

        return JobResult.Success;
    }
}

public class PingRequest
{
    public string Data { get; set; }
    public string Id { get; set; }
    public int PercentChanceOfException { get; set; } = 0;
}
