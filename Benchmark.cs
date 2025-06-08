using System.Text;

namespace BenchMaster;

/// <summary>
/// A record to hold and process a collection of benchmark steps.
/// </summary>
record Benchmark(string Name, IList<BenchStep> items)
{
    #region Internal Record for Aggregation
    private record AggregatedStep(
        string Name,
        int Count,
        TimeSpan AvgTime, TimeSpan MinTime, TimeSpan MaxTime,
        long AvgBytes, long MinBytes, long MaxBytes,
        TimeSpan TotalTime
    );
    #endregion

    #region Single Benchmark Summary
    public void PrintSummary()
    {
        Console.OutputEncoding = Encoding.UTF8;

        if (items == null || items.Count == 0)
        {
            Console.WriteLine($"Benchmark '{Name}' contains no steps to display.");
            return;
        }

        var orderedCategories = items
            .Select(step => step.Category)
            .Distinct()
            .OrderBy(cat => cat)
            .ToList();

        foreach (var category in orderedCategories)
        {
            var stepsInCategory = items.Where(step => step.Category == category).ToList();
            if (stepsInCategory.Count == 0) continue;

            var orderedUniqueNames = stepsInCategory
                .Select(step => step.Name)
                .Distinct()
                .ToList();

            var aggregatedSteps = new List<AggregatedStep>();
            foreach (var stepName in orderedUniqueNames)
            {
                var group = stepsInCategory.Where(step => step.Name == stepName).ToList();
                if (group.Count > 0)
                {
                    aggregatedSteps.Add(new AggregatedStep(
                        Name: stepName,
                        Count: group.Count,
                        AvgTime: TimeSpan.FromSeconds(group.Average(s => s.Time.TotalSeconds)),
                        MinTime: group.Min(s => s.Time),
                        MaxTime: group.Max(s => s.Time),
                        AvgBytes: (long)group.Average(s => s.AllocatedBytes),
                        MinBytes: group.Min(s => s.AllocatedBytes),
                        MaxBytes: group.Max(s => s.AllocatedBytes),
                        TotalTime: TimeSpan.FromTicks(group.Sum(s => s.Time.Ticks))
                    ));
                }
            }

            var grandTotalTime = TimeSpan.FromTicks(stepsInCategory.Sum(step => step.Time.Ticks));

            string[] headers = {
                "Step Name", "N", "Time Avg", "Time Min", "Time Max", "% Time",
                "GC Alloc Avg", "GC Alloc Min", "GC Alloc Max"
            };

            var rows = aggregatedSteps.Select(agg =>
            {
                var timePercentage = grandTotalTime.Ticks > 0
                    ? (agg.TotalTime.TotalMilliseconds / grandTotalTime.TotalMilliseconds * 100)
                    : 0;
                return new[] {
                    agg.Name,
                    agg.Count.ToString(),
                    FormatTime(agg.AvgTime),
                    FormatTime(agg.MinTime),
                    FormatTime(agg.MaxTime),
                    $"{timePercentage:F1}",
                    FormatBytes(agg.AvgBytes),
                    FormatBytes(agg.MinBytes),
                    FormatBytes(agg.MaxBytes)
                };
            }).ToList();

            PrintTable($"Category: {category}", headers, rows);
        }
    }
    #endregion

    #region Comparison of Results (New Method)

    private record ComparisonStep(string Name, TimeSpan LiteDbTime, long LiteDbBytes, TimeSpan SoloDbTime, long SoloDbBytes);

    public static void PrintComparisonOfResults(Benchmark liteDbBenchmark, Benchmark soloDbBenchmark)
    {
        Console.OutputEncoding = Encoding.UTF8;
        var mainTitle = "COMPARISON OF RESULTS";
        Console.WriteLine();
        Console.WriteLine(mainTitle);
        Console.WriteLine(new string('=', mainTitle.Length));

        var allCategories = liteDbBenchmark.items.Select(i => i.Category)
            .Concat(soloDbBenchmark.items.Select(i => i.Category))
            .Distinct()
            .OrderBy(c => c)
            .ToList();

        foreach (var category in allCategories)
        {
            var stepsInCategoryLiteDb = liteDbBenchmark.items.Where(s => s.Category == category).ToList();
            var stepsInCategorySoloDb = soloDbBenchmark.items.Where(s => s.Category == category).ToList();

            var stepNames = stepsInCategoryLiteDb.Select(s => s.Name)
                .Concat(stepsInCategorySoloDb.Select(s => s.Name))
                .Distinct()
                .OrderBy(name => name)
                .ToList();

            var comparisonSteps = new List<ComparisonStep>();
            foreach (var name in stepNames)
            {
                var liteDbGroup = stepsInCategoryLiteDb.Where(s => s.Name == name).ToList();
                var soloDbGroup = stepsInCategorySoloDb.Where(s => s.Name == name).ToList();

                var liteDbAvgTime = liteDbGroup.Any() ? TimeSpan.FromSeconds(liteDbGroup.Average(s => s.Time.TotalSeconds)) : TimeSpan.Zero;
                var liteDbAvgBytes = liteDbGroup.Any() ? (long)liteDbGroup.Average(s => s.AllocatedBytes) : 0L;
                var soloDbAvgTime = soloDbGroup.Any() ? TimeSpan.FromSeconds(soloDbGroup.Average(s => s.Time.TotalSeconds)) : TimeSpan.Zero;
                var soloDbAvgBytes = soloDbGroup.Any() ? (long)soloDbGroup.Average(s => s.AllocatedBytes) : 0L;

                if (liteDbGroup.Any() || soloDbGroup.Any())
                {
                    comparisonSteps.Add(new ComparisonStep(name, liteDbAvgTime, liteDbAvgBytes, soloDbAvgTime, soloDbAvgBytes));
                }
            }

            if (!comparisonSteps.Any()) continue;

            string[] headers = {
                "Step Name", "LiteDB Time", "SoloDB Time", "Difference",
                "LiteDB GC Alloc", "SoloDB GC Alloc", "Difference"
            };

            var rows = comparisonSteps.Select(cs =>
            {
                // The new CalculateDifferenceString method is called here.
                var timeDiff = CalculateDifferenceString(cs.LiteDbTime.TotalMilliseconds, cs.SoloDbTime.TotalMilliseconds);
                var allocDiff = CalculateDifferenceString(cs.LiteDbBytes, cs.SoloDbBytes, true);

                return new[] {
                    cs.Name,
                    cs.LiteDbTime > TimeSpan.Zero ? FormatTime(cs.LiteDbTime) : "N/A",
                    cs.SoloDbTime > TimeSpan.Zero ? FormatTime(cs.SoloDbTime) : "N/A",
                    timeDiff,
                    cs.LiteDbBytes != 0 ? FormatBytes(cs.LiteDbBytes) : "N/A",
                    cs.SoloDbBytes != 0 ? FormatBytes(cs.SoloDbBytes) : "N/A",
                    allocDiff
                };
            }).ToList();

            PrintTable($"Category: {category}", headers, rows);
        }
    }
    #endregion

    #region Private Helpers

    private static string CalculateDifferenceString(double liteDbValue, double soloDbValue, bool invert = false)
    {
        if (Math.Abs(liteDbValue) < 1e-9 && Math.Abs(soloDbValue) < 1e-9) return "N/A";
        if (Math.Abs(liteDbValue) < 1e-9) return "SoloDB  +Inf%"; // Infinite improvement if baseline is 0

        // For time and memory, a smaller value is better.
        double difference = (soloDbValue - liteDbValue) / liteDbValue * 100;
        var sign = invert ? "-" : "+";

        if (Math.Abs(difference) < 0.1) return "≈ 0.0%";

        // If 'difference' is negative, SoloDB used less time/memory, which is better.
        if (difference < 0)
        {
            // We show the absolute difference and declare SoloDB as better.
            return $"SoloDB {sign}{Math.Abs(difference):#,##0.0}%";
        }
        else
        {
            // If 'difference' is positive, LiteDB was better.
            return $"LiteDB {sign}{difference:#,##0.0}%";
        }
    }

    private static void PrintTable(string title, string[] headers, List<string[]> rows)
    {
        Console.WriteLine();
        Console.WriteLine(title);
        Console.WriteLine(new string('-', title.Length));

        if (!rows.Any())
        {
            Console.WriteLine("No data available for this section.");
            return;
        }

        var columnWidths = new int[headers.Length];
        for (int i = 0; i < headers.Length; i++)
        {
            int headerWidth = headers[i].Length;
            int dataMaxWidth = rows.Max(row => row[i].Length);
            columnWidths[i] = Math.Max(headerWidth, dataMaxWidth);
        }

        var headerLine = new StringBuilder("| ");
        var separatorLine = new StringBuilder("|-");

        for (int i = 0; i < headers.Length; i++)
        {
            // Left-align the first column (step name), right-align all others
            var alignment = (i == 0) ? -columnWidths[i] : columnWidths[i];
            headerLine.AppendFormat($"{{0,{alignment}}} | ", headers[i]);
            separatorLine.Append(new string('-', columnWidths[i] + 1)).Append("|-");
        }
        separatorLine.Length -= 1;

        Console.WriteLine(headerLine.ToString());
        Console.WriteLine(separatorLine.ToString());

        foreach (var row in rows)
        {
            var dataLine = new StringBuilder("| ");
            for (int i = 0; i < row.Length; i++)
            {
                var alignment = (i == 0) ? -columnWidths[i] : columnWidths[i];
                dataLine.AppendFormat($"{{0,{alignment}}} | ", row[i]);
            }
            Console.WriteLine(dataLine.ToString());
        }
        Console.WriteLine(separatorLine.ToString().Replace('|', '-'));
    }

    private static string FormatTime(TimeSpan ts)
    {
        if (ts.TotalMilliseconds < 1) return $"{ts.TotalMicroseconds:F2} μs";
        if (ts.TotalMilliseconds < 1000) return $"{ts.TotalMilliseconds:F2} ms";
        return $"{ts.TotalSeconds:F2} s";
    }
    private static string FormatBytes(long bytes)
    {
        if (bytes == 0) return "0 B";
        const int scale = 1024;
        string[] orders = { "B", "KB", "MB", "GB", "TB" };
        int i = (int)Math.Floor(Math.Log(Math.Abs(bytes), scale));
        double value = bytes / Math.Pow(scale, i);
        return $"{value:F2} {orders[i]}";
    }
    #endregion
}