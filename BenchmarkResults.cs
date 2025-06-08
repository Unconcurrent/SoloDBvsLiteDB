namespace BenchMaster;

public class BenchmarkResults
{
    public string DatabaseName { get; set; }
    public List<BenchStep> Steps { get; set; } = new();
    public TimeSpan TotalDuration { get; set; }
    public bool Success { get; set; }
}