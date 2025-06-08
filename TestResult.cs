namespace BenchMaster;

public class TestResult
{
    public string DatabaseName { get; set; }
    public string TestType { get; set; }
    public bool Success { get; set; }
    public string Details { get; set; }
    public TimeSpan Duration { get; set; }
    public Dictionary<string, object> Metrics { get; set; } = new();
}