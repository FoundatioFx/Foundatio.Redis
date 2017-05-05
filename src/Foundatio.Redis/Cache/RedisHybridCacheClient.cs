﻿using System;
using Foundatio.Logging;
using Foundatio.Messaging;
using Foundatio.Serializer;
using StackExchange.Redis;

namespace Foundatio.Caching {
    public class RedisHybridCacheClient : HybridCacheClient {
        public RedisHybridCacheClient(ConnectionMultiplexer connectionMultiplexer, ISerializer serializer = null, ILoggerFactory loggerFactory = null)
            : base(new RedisCacheClient(connectionMultiplexer, serializer, loggerFactory), new RedisMessageBus(new RedisMessageBusOptions { Subscriber = connectionMultiplexer.GetSubscriber(), Topic = "cache-messages", Serializer = serializer, LoggerFactory = loggerFactory }), loggerFactory) { }

        public override void Dispose() {
            base.Dispose();
            _distributedCache.Dispose();
            _messageBus.Dispose();
        }
    }
}
