using Foundatio.Caching;
using Foundatio.Extensions;
using Foundatio.Messaging;
using Microsoft.Extensions.DependencyInjection;
using StackExchange.Redis;

namespace Foundatio.Redis;

public static class FoundatioServicesExtensions
{
    public static FoundatioBuilder UseRedis(this FoundatioBuilder.CachingBuilder builder)
    {
        IFoundatioBuilder foundatioBuilder = builder;
        foundatioBuilder.Services.ReplaceSingleton<ICacheClient>(sp => new RedisCacheClient(b => b.UseServices(sp).ConnectionMultiplexer(sp.GetRequiredService<IConnectionMultiplexer>())));

        return foundatioBuilder.Builder;
    }

    public static FoundatioBuilder UseRedis(this FoundatioBuilder.CachingBuilder builder, RedisCacheClientOptions options)
    {
        IFoundatioBuilder foundatioBuilder = builder;
        foundatioBuilder.Services.ReplaceSingleton<ICacheClient>(sp =>
        {
            options = options.UseServices(sp);
            options.ConnectionMultiplexer ??= sp.GetRequiredService<IConnectionMultiplexer>();
            return new RedisCacheClient(options);
        });

        return foundatioBuilder.Builder;
    }

    public static FoundatioBuilder UseRedis(this FoundatioBuilder.CachingBuilder builder, Builder<RedisCacheClientOptionsBuilder, RedisCacheClientOptions> config)
    {
        IFoundatioBuilder foundatioBuilder = builder;
        foundatioBuilder.Services.ReplaceSingleton<ICacheClient>(sp =>
        {
            return new RedisCacheClient(b => b.ConnectionMultiplexer(sp.GetRequiredService<IConnectionMultiplexer>()).Configure(config).UseServices(sp));
        });

        return foundatioBuilder.Builder;
    }

    public static FoundatioBuilder UseRedis(this FoundatioBuilder.MessagingBuilder builder)
    {
        IFoundatioBuilder foundatioBuilder = builder;
        foundatioBuilder.Services.ReplaceSingleton<IMessageBus>(sp => new RedisMessageBus(b => b.UseServices(sp).Subscriber(sp.GetRequiredService<IConnectionMultiplexer>().GetSubscriber())));

        return foundatioBuilder.Builder;
    }

    public static FoundatioBuilder UseRedis(this FoundatioBuilder.MessagingBuilder builder, RedisMessageBusOptions options)
    {
        IFoundatioBuilder foundatioBuilder = builder;
        foundatioBuilder.Services.ReplaceSingleton<IMessageBus>(sp =>
        {
            options = options.UseServices(sp);
            options.Subscriber ??= sp.GetRequiredService<IConnectionMultiplexer>().GetSubscriber();
            return new RedisMessageBus(options);
        });

        return foundatioBuilder.Builder;
    }

    public static FoundatioBuilder UseRedis(this FoundatioBuilder.MessagingBuilder builder, Builder<RedisMessageBusOptionsBuilder, RedisMessageBusOptions> config)
    {
        IFoundatioBuilder foundatioBuilder = builder;
        foundatioBuilder.Services.ReplaceSingleton<IMessageBus>(sp =>
        {
            return new RedisMessageBus(b => b.Subscriber(sp.GetRequiredService<IConnectionMultiplexer>().GetSubscriber()).Configure(config).UseServices(sp));
        });

        return foundatioBuilder.Builder;
    }
}
