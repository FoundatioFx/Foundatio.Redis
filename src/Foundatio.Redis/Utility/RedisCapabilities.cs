using System;
using System.Threading;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace Foundatio.Redis.Utility;

internal sealed class RedisCapabilities
{
    private static readonly Version LMoveMinVersion = new(6, 2, 0);
    private static readonly Version MsetexMinVersion = new(8, 4, 0);
    private static readonly Version CasMinVersion = new(8, 4, 0);

    private readonly IConnectionMultiplexer _muxer;
    private readonly ILogger _logger;

    private int _lMove; // 0 = unknown, 1 = supported, -1 = not supported
    private int _msetex; // 0 = unknown, 1 = supported, -1 = not supported
    private int _cas; // 0 = unknown, 1 = supported, -1 = not supported

    public RedisCapabilities(IConnectionMultiplexer muxer, ILogger logger)
    {
        ArgumentNullException.ThrowIfNull(muxer);
        ArgumentNullException.ThrowIfNull(logger);

        _muxer = muxer;
        _logger = logger;
    }

    /// <summary>
    /// LMOVE requires Redis 6.2+. Callers should fall back to the (functionally identical) RPOPLPUSH
    /// variant when this returns <c>false</c>, e.g. against Azure Cache for Redis, which is pinned at 6.0.x.
    /// </summary>
    public bool SupportsLMove => CheckVersion(ref _lMove, LMoveMinVersion, "LMOVE", requireRedisProduct: false);

    /// <summary>
    /// MSETEX is a Redis-proprietary command introduced in Redis 8.4. It is not available on Valkey or other
    /// forks, even though their self-reported version numbers may be numerically &gt;= 8.4.0.
    /// </summary>
    public bool SupportsMsetex => CheckVersion(ref _msetex, MsetexMinVersion, "MSETEX", requireRedisProduct: true);

    /// <summary>
    /// SET/DELEX ... IFEQ (compare-and-swap) is a Redis-proprietary feature introduced in Redis 8.4. It is not
    /// available on Valkey or other forks, even though their self-reported version numbers may be numerically
    /// &gt;= 8.4.0.
    /// </summary>
    public bool SupportsCas => CheckVersion(ref _cas, CasMinVersion, "SET/DELEX IFEQ (CAS)", requireRedisProduct: true);

    public void Invalidate()
    {
        Volatile.Write(ref _lMove, 0);
        Volatile.Write(ref _msetex, 0);
        Volatile.Write(ref _cas, 0);
    }

    private bool CheckVersion(ref int cached, Version minVersion, string featureName, bool requireRedisProduct)
    {
        int value = Volatile.Read(ref cached);
        if (value is not 0)
            return value > 0;

        var endpoints = _muxer.GetEndPoints();
        if (endpoints is { Length: 0 })
        {
            _logger.LogDebug("{Feature}: No endpoints configured, feature not available", featureName);
            return false;
        }

        bool foundConnectedPrimary = false;
        foreach (var endpoint in endpoints)
        {
            var server = _muxer.GetServer(endpoint);
            if (server.IsConnected && !server.IsReplica)
            {
                foundConnectedPrimary = true;
                if (server.Version < minVersion)
                {
                    _logger.LogDebug("{Feature}: Server {Endpoint} version {Version} does not support feature (requires {MinVersion}+)",
                        featureName, endpoint, server.Version, minVersion);
                    Volatile.Write(ref cached, -1);
                    return false;
                }

                if (requireRedisProduct && server.GetProductVariant(out _) != ProductVariant.Redis)
                {
                    _logger.LogDebug("{Feature}: Server {Endpoint} is not a Redis-proprietary product, feature not available",
                        featureName, endpoint);
                    Volatile.Write(ref cached, -1);
                    return false;
                }
            }
        }

        if (foundConnectedPrimary)
        {
            Volatile.Write(ref cached, 1);
            return true;
        }

        _logger.LogDebug("{Feature}: No connected primaries found, feature availability unknown", featureName);
        return false;
    }
}
