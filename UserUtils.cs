internal static class UserUtils
{
    private static readonly string[] _categorySource = new[]
    {
        "Technology", "Science", "Art", "Music", "Sports", "Travel", "Food", "History", "Literature",
        "Technology", "Science", "Art", "Music", "Sports", "Travel", "Food", "History", "Literature",
        "Technology", "Science", "Art", "Music", "Sports", "Travel", "Food", "History", "Literature",
        // "Gaming" is intentionally rare in the source data.
        "Gaming"
    };

    public static string RandomUsername(int length = 8)
    {
        const string chars = "abcdefghijklmnopqrstuvwxyz0123456789";
        
        return string.Create(length, chars, static (span, charSet) =>
        {
            for (int i = 0; i < span.Length; i++)
            {
                span[i] = charSet[BenchMaster.Program.random.Next(charSet.Length)];
            }
        });
    }

    public static string[] GenerateCategories(int count)
    {
        var distinctCount = 10;
        if (count > distinctCount)
        {
            count = distinctCount;
        }

        if (count <= 0)
        {
            return Array.Empty<string>();
        }

        var shuffledSource = _categorySource.ToArray();
        Shuffle(shuffledSource);

        var uniqueCategories = new HashSet<string>();
        foreach (var category in shuffledSource)
        {
            if (uniqueCategories.Add(category) && uniqueCategories.Count == count)
            {
                break;
            }
        }
        return uniqueCategories.ToArray();
    }

    private static void Shuffle<T>(T[] array)
    {
        int n = array.Length;
        while (n > 1)
        {
            n--;
            int k = BenchMaster.Program.random.Next(n + 1);
            (array[k], array[n]) = (array[n], array[k]);
        }
    }
}