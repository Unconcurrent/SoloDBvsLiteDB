using System.Diagnostics;

namespace BenchMaster;

/// <summary>
/// A record to hold the results of a single benchmark step.
/// </summary>
public record BenchStep(string Category, string Name, TimeSpan Time, long AllocatedBytes)
{
    public static BenchStep Record(string category, string name, Action a)
    {
        try
        {
            var allocatedBefore = GC.GetTotalAllocatedBytes(true);
            var ts = Stopwatch.GetTimestamp();
            a();
            var elapsed = Stopwatch.GetElapsedTime(ts);
            var allocatedAfter = GC.GetTotalAllocatedBytes(true);
            return new BenchStep(category, name, elapsed, allocatedAfter - allocatedBefore);
        }
        catch (Exception e)
        {
            Console.WriteLine(e);
            throw;
        }
    }
}