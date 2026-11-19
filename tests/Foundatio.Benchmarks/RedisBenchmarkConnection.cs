using System;
using System.Threading.Tasks;
using StackExchange.Redis;

namespace Foundatio.Benchmarks;

internal static class RedisBenchmarkConnection
{
    public static Task<ConnectionMultiplexer> ConnectAsync() =>
        ConnectionMultiplexer.ConnectAsync(Environment.GetEnvironmentVariable("ConnectionStrings__RedisConnectionString") ?? "localhost");
}
