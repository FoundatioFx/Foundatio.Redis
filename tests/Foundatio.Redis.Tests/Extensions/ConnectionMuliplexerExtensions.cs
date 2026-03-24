using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using StackExchange.Redis;

namespace Foundatio.Redis.Tests.Extensions;

public static class ConnectionMultiplexerExtensions
{
    public static async Task FlushAllAsync(this ConnectionMultiplexer muxer)
    {
        var endpoints = muxer.GetEndPoints();
        if (endpoints.Length == 0)
            return;

        foreach (var endpoint in endpoints)
        {
            var server = muxer.GetServer(endpoint);
            if (!server.IsReplica)
                await server.FlushAllDatabasesAsync();
        }
    }

    public static async Task<long> CountAllKeysAsync(this ConnectionMultiplexer muxer)
    {
        var endpoints = muxer.GetEndPoints();
        if (endpoints.Length == 0)
            return 0;

        long count = 0;
        foreach (var endpoint in endpoints)
        {
            var server = muxer.GetServer(endpoint);
            if (!server.IsReplica)
                count += await server.DatabaseSizeAsync();
        }

        return count;
    }

    public static List<string> GetAllKeys(this ConnectionMultiplexer muxer)
    {
        var keys = new List<string>();
        foreach (var endpoint in muxer.GetEndPoints())
        {
            var server = muxer.GetServer(endpoint);
            if (!server.IsReplica)
                keys.AddRange(server.Keys(pattern: "*").Select(k => k.ToString()));
        }

        return keys;
    }
}
