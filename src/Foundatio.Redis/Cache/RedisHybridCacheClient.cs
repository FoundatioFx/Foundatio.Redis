using System;
using Foundatio.Messaging;
namespace Foundatio.Caching;

public class RedisHybridCacheClient : HybridCacheClient
{
    public RedisHybridCacheClient(RedisHybridCacheClientOptions options, InMemoryCacheClientOptions? localOptions = null)
        : base(CreateCacheClient(options), CreateMessageBus(options), localOptions, options.LoggerFactory)
    {
    }

    private static RedisCacheClient CreateCacheClient(RedisHybridCacheClientOptions options)
    {
        var connectionMultiplexer = options.ConnectionMultiplexer ?? throw new ArgumentNullException(nameof(options), "ConnectionMultiplexer is required.");
        return new RedisCacheClient(o => o
            .ConnectionMultiplexer(connectionMultiplexer)
            .Serializer(options.Serializer)
            .LoggerFactory(options.LoggerFactory)
            .ShouldThrowOnSerializationError(options.ShouldThrowOnSerializationError)
            .ReadMode(options.ReadMode)
            .UseDatabase(options.Database));
    }

    private static RedisMessageBus CreateMessageBus(RedisHybridCacheClientOptions options)
    {
        var connectionMultiplexer = options.ConnectionMultiplexer ?? throw new ArgumentNullException(nameof(options), "ConnectionMultiplexer is required.");
        return new RedisMessageBus(o => o
            .Subscriber(connectionMultiplexer.GetSubscriber())
            .Topic(options.RedisChannelName)
            .Serializer(options.Serializer)
            .LoggerFactory(options.LoggerFactory));
    }

    public RedisHybridCacheClient(Builder<RedisHybridCacheClientOptionsBuilder, RedisHybridCacheClientOptions> config,
        Builder<InMemoryCacheClientOptionsBuilder, InMemoryCacheClientOptions>? localConfig = null)
        : this(config(new RedisHybridCacheClientOptionsBuilder()).Build(),
            localConfig?.Invoke(new InMemoryCacheClientOptionsBuilder()).Build())
    {
    }

    public override void Dispose()
    {
        base.Dispose();
        _distributedCache.Dispose();
        _messageBus.Dispose();
    }
}
