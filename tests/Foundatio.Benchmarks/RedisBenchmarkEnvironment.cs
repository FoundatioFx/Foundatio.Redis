using System;
using System.Threading.Tasks;
using StackExchange.Redis;

namespace Foundatio.Benchmarks;

internal static class RedisBenchmarkEnvironment
{
    public static string CreatePrefix() =>
        Environment.GetEnvironmentVariable("FOUNDATIO_BENCHMARK_PREFIX") ?? $"foundatio-bench-{Guid.NewGuid():N}";

    public static Task<ConnectionMultiplexer> ConnectAsync() =>
        ConnectionMultiplexer.ConnectAsync(Environment.GetEnvironmentVariable("ConnectionStrings__RedisConnectionString") ?? "localhost");
}
