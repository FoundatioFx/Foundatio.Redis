using System;

namespace Foundatio.Redis.Errors;

public class RedisConfigurationException : Exception
{
    public RedisConfigurationException() { }
    public RedisConfigurationException(string message) : base(message) { }
    public RedisConfigurationException(string message, System.Exception inner) : base(message, inner) { }
}