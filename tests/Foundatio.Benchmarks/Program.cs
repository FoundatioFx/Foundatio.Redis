using BenchmarkDotNet.Running;

namespace Foundatio.Benchmarks;

public static class Program
{
    public static void Main(string[] args) => BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);
}
