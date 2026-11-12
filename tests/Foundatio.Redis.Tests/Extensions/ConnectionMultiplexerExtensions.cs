using System;
using System.Linq;
using System.Threading.Tasks;
using StackExchange.Redis;

namespace Foundatio.Redis.Tests.Extensions;

public static class ConnectionMultiplexerExtensions
{
    public static async Task FlushAllAsync(this ConnectionMultiplexer muxer)
    {
        const int maxAttempts = 5;
        for (int attempt = 1; ; attempt++)
        {
            try
            {
                await FlushPrimariesAsync(muxer);
                return;
            }
            catch (RedisException ex) when (IsReplicaError(ex) && attempt < maxAttempts)
            {
                // Cluster roles can change after the multiplexer cached them (for example while the cluster settles at startup).
                // Refresh the topology so the current primaries are flushed instead of a node that became a replica.
                await Task.Delay(TimeSpan.FromMilliseconds(250 * attempt));
                await muxer.ConfigureAsync();
            }
        }
    }

    public static async Task<long> CountAllKeysAsync(this ConnectionMultiplexer muxer)
    {
        int database = muxer.GetDatabase().Database;
        long count = 0;
        foreach (var server in muxer.GetEndPoints().Select(ep => muxer.GetServer(ep)).Where(s => !s.IsReplica))
            count += await server.DatabaseSizeAsync(database);

        return count;
    }

    private static async Task FlushPrimariesAsync(ConnectionMultiplexer muxer)
    {
        int database = muxer.GetDatabase().Database;
        foreach (var server in muxer.GetEndPoints().Select(ep => muxer.GetServer(ep)).Where(s => !s.IsReplica))
            await server.FlushDatabaseAsync(database);
    }

    private static bool IsReplicaError(RedisException ex)
    {
        return ex.Message.Contains("replica", StringComparison.OrdinalIgnoreCase)
            || ex.InnerException?.Message.Contains("replica", StringComparison.OrdinalIgnoreCase) is true;
    }
}
