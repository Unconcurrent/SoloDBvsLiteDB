using SoloDatabase;
using System.Diagnostics;

namespace BenchMaster;

static class Program
{
    internal static Random random = new Random(101);
    internal const int PerformanceIterations = 3;
    internal const int UserCount = 10 * 1000;
    static void Main(string[] args)
    {
#if DEBUG
        Console.WriteLine("Run in RELEASE please.");
        return;
#endif

        if (IsSlaveProcess(args))
        {
            SubProgram.ExecuteSlaveProcess(args);
            return;
        }

        // Master process
        Console.Clear();
        Console.WriteLine("=== Database Testing Program ===");
        Console.WriteLine($"Testing on SSD: LiteDB vs SoloDB");
        Console.WriteLine($"Performance iterations: {PerformanceIterations}");
        Console.WriteLine($"User count per test: {UserCount:N0}");
        Console.WriteLine();

        var allResults = new List<TestResult>();
        
        string[] databases =
        {
            "LiteDB", 
            "SoloDB"
        };

        foreach (var dbName in databases)
        {
            Console.WriteLine($"\n=== Testing {dbName} ===");
            
            var perfResults = RunPerformanceTests(dbName);
            allResults.AddRange(perfResults);
        }
        
        ShowSummary(allResults);
    }

    private static bool IsSlaveProcess(string[] args)
    {
        return args.Any(arg => arg == "--slave");
    }
    

    private static List<TestResult> RunPerformanceTests(string dbName)
    {
        var results = new List<TestResult>();
        Console.WriteLine($"\n--- Performance Testing {dbName} ---");

        Console.WriteLine($"Running performance iterations {PerformanceIterations}...");

        var stopwatch = Stopwatch.StartNew();
        var benchResult = RunPerformanceTestInSlave(dbName, PerformanceIterations);
        stopwatch.Stop();

        var result = new TestResult
        {
            DatabaseName = dbName,
            TestType = $"Performance_Iterations",
            Success = benchResult.Success,
            Details = benchResult.Success ? "Performance test completed" : "Performance test failed",
            Duration = stopwatch.Elapsed,
            Metrics = new Dictionary<string, object>
            {
                ["TotalSteps"] = benchResult.Steps.Count,
                ["AverageStepTime"] = benchResult.Steps.Count > 0 ? benchResult.Steps.Average(s => s.Time.TotalMilliseconds) : 0,
                ["TotalOperationTime"] = benchResult.TotalDuration.TotalMilliseconds,
                ["BenchmarkSteps"] = benchResult.Steps
            }
        };

        results.Add(result);

        if (benchResult.Success)
        {
            Console.WriteLine($"  ✓ Completed in {stopwatch.Elapsed.TotalSeconds:F2}s");
        }
        else
        {
            Console.WriteLine($"  ✗ Failed after {stopwatch.Elapsed.TotalSeconds:F2}s");
        }

        return results;
    }

    private static BenchmarkResults RunPerformanceTestInSlave(string dbName, int iterations)
    {
        var startInfo = new ProcessStartInfo
        {
            FileName = Process.GetCurrentProcess().MainModule!.FileName,
            Arguments = $"--slave --test-type=performance --db={dbName} --iterations={iterations}",
            UseShellExecute = false,
            CreateNoWindow = true,
            RedirectStandardOutput = true,
            RedirectStandardError = true
        };

        using var process = Process.Start(startInfo);
        string output = process.StandardOutput.ReadToEnd();
        string error = process.StandardError.ReadToEnd();
        process.WaitForExit();

        if (process.ExitCode != 0)
        {
            Console.WriteLine(output);
            Environment.Exit(-2);
        }

        return ParsePerformanceOutput(output, process.ExitCode == 0);
    }

    private static BenchmarkResults ParsePerformanceOutput(string output, bool success)
    {
        var result = new BenchmarkResults { Success = success };
        var steps = new List<BenchStep>();

        if (!success)
        {
            return result;
        }

        var lines = output.Split('\n');
        foreach (var line in lines)
        {
            if (line.StartsWith("SLAVE_STEP:"))
            {
                var parts = line.Substring(11).Split(" - ");
                if (parts.Length >= 4)
                {
                    var i = 0;
                    var category = parts[i++];
                    var name = parts[i++];
                    if (double.TryParse(parts[i++].Replace("ms", ""), out double ms) &&
                        long.TryParse(parts[i++].Replace("B", ""), out long bytes))
                    {
                        steps.Add(new BenchStep(category, name, TimeSpan.FromMilliseconds(ms), bytes));
                    }
                }
            }
        }

        result.Steps = steps;
        result.TotalDuration = TimeSpan.FromMilliseconds(steps.Sum(s => s.Time.TotalMilliseconds));
        return result;
    }

    private static void ShowSummary(List<TestResult> results)
    {
        var databases = results.Select(r => r.DatabaseName).Distinct().ToList();

        var benchmarkForDb = new Dictionary<string, Benchmark>();
        
        foreach (var dbName in databases)
        {
            var dbResults = results.Where(r => r.DatabaseName == dbName).ToList();
            var perfResults = dbResults.Where(r => r.TestType.StartsWith("Performance")).ToList();

            if (perfResults.Any() && perfResults.All(r => r.Success))
            {
                Console.WriteLine($"\n{new string('=', 60)}");
                Console.WriteLine($"{dbName} DETAILED BENCHMARK RESULTS           ");
                Console.WriteLine(new string('=', 60));
                
                var allSteps = new List<BenchStep>();
                foreach (var result in perfResults)
                {
                    if (result.Metrics.ContainsKey("BenchmarkSteps"))
                    {
                        var steps = (List<BenchStep>)result.Metrics["BenchmarkSteps"];
                        allSteps.AddRange(steps);
                    }
                }

                if (allSteps.Any())
                {
                    var benchmark = new Benchmark($"{dbName} Performance Results ({perfResults.Count} iterations)", allSteps);
                    benchmark.PrintSummary();
                    benchmarkForDb[dbName] = benchmark;
                }
            }
        }

        Benchmark.PrintComparisonOfResults(benchmarkForDb["LiteDB"], benchmarkForDb["SoloDB"]);
    }
}