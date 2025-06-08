using LiteDB;
using SoloDatabase;
using SoloDatabase.Attributes;
using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace BenchMaster;

// Database models
sealed class GuidGenerator : IIdGenerator<UserSoloDB>
{
    public object GenerateId(ISoloDBCollection<UserSoloDB> collection, UserSoloDB item)
    {
        return Guid.CreateVersion7();
    }

    public bool IsEmpty(object id)
    {
        return Guid.Empty == Unsafe.Unbox<Guid>(id);
    }
}

sealed class UserSoloDB
{
    [SoloId(typeof(GuidGenerator))] public Guid Id { get; set; }
    [Indexed(unique: true)] public string Username { get; set; } = UserUtils.RandomUsername();
    public List<string> InterestedCategories { get; set; } = UserUtils.GenerateCategories(Program.random.NextSingle() <= 0.7f ? 3 : 2).ToList();
    public List<string> UploadedFiles { get; set; } = new List<string>(0);
}

sealed class UserLiteDB
{
    [LiteDB.BsonId(autoId: false)]
    public Guid Id { get; set; } = Guid.CreateVersion7();
    // The index for Username is defined at database initialization.
    public string Username { get; set; } = UserUtils.RandomUsername();
    public List<string> InterestedCategories { get; set; } = UserUtils.GenerateCategories(Program.random.NextSingle() <= 0.7f ? 3 : 2).ToList();
    public List<string> UploadedFiles { get; set; } = new List<string>(0);
}


static class SubProgram
{
    internal static void ExecuteSlaveProcess(string[] args)
    {
        try
        {
            string testType = GetArgValue(args, "--test-type");
            string dbName = GetArgValue(args, "--db");
            int iterations = int.Parse(GetArgValue(args, "--iterations") ?? "0");

            switch (testType)
            {
                case "performance":
                    ExecutePerformanceSlave(dbName, iterations);
                    break;
                default:
                    throw new ArgumentException($"Unknown test type: {testType}");
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"SLAVE_ERROR: {ex}");
            Environment.Exit(1);
        }
    }
    private static string GetArgValue(string[] args, string prefix)
    {
        return args.FirstOrDefault(arg => arg.StartsWith($"{prefix}="))?.Split('=')[1]!;
    }

    internal static void ExecutePerformanceSlave(string dbName, int iterations)
    {
        Console.WriteLine($"SLAVE_START: Performance test {dbName} iterations {iterations}");

        var stopwatch = Stopwatch.StartNew();
        var steps = new List<BenchStep>();

        try
        {
            for (int i = 0; i < iterations; i++)
            {
                // Clear the GC before the test.
                GC.Collect();
                GC.WaitForPendingFinalizers();

                stopwatch.Restart();

                if (dbName == "LiteDB")
                {
                    steps.AddRange(BenchLiteDB());
                }
                else if (dbName == "SoloDB")
                {
                    steps.AddRange(BenchSoloDB());
                }
                else
                {
                    throw new ArgumentException($"Unknown database: {dbName}");
                }

                stopwatch.Stop();

                Console.WriteLine($"SLAVE_SUCCESS: {steps.Count} steps completed in {stopwatch.Elapsed.TotalMilliseconds}ms");
                foreach (var step in steps)
                {
                    Console.WriteLine($"SLAVE_STEP: {step.Category} - {step.Name} - {step.Time.TotalMilliseconds:F2}ms - {step.AllocatedBytes}B");
                }
                steps.Clear();
            }

            Environment.Exit(0);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"SLAVE_ERROR: {ex.Message}");
            Environment.Exit(1);
        }
    }

    const string CategoryGeneral = "1.General";
    const string CategoryFs = "2.FS";

    // LiteDB Performance Tests
    internal static IEnumerable<BenchStep> BenchLiteDB()
    {
        if (Directory.Exists("./test-db"))
        {
            Directory.Delete("./test-db", true);
        }

        Directory.CreateDirectory("./test-db");

        using var db = new LiteDatabase("./test-db/test-litedb.litedb");
        var users = db.GetCollection<UserLiteDB>();
        users.EnsureIndex(u => u.Username, true);
        users.EnsureIndex(u => u.InterestedCategories, false);

        var usersToInsert = new UserLiteDB[Program.UserCount];
        for (var i = 0; i < usersToInsert.Length; i++)
        {
            usersToInsert[i] = new UserLiteDB();
        }


        yield return BenchStep.Record(CategoryGeneral, $"Inserting {usersToInsert.Length} users", () => users.InsertBulk(usersToInsert));


        List<string> gamingUsers = null;

        yield return BenchStep.Record(CategoryGeneral, $"Searching for gaming users.", () =>
        {
            gamingUsers = users.Query()
                .Where(x => x.InterestedCategories.Contains("Gaming"))
                .Select(x => x.Username)
                .ToList();
        });


        var randomFileData = new byte[64 * 1024];
        Program.random.NextBytes(randomFileData);
        var fs = db.FileStorage;
        var subSetOfGamers = gamingUsers.Take(200).ToList();

        yield return BenchStep.Record(CategoryFs, $"Upload 3 {randomFileData.Length / 1024} kb files for 200 gamers in a transaction.", () =>
        {
            try
            {
                db.BeginTrans();
                
                var gamerUsers = users.Query()
                    .Where(x => subSetOfGamers.Contains(x.Username))
                    .ToList();

                foreach (var gamerUser in gamerUsers!)
                {
                    for (int i = 0; i < 3; i++)
                    {
                        var fileName = $"footage_gaming_{gamerUser.Username}_{i}.bin";

                        using (var file = fs.OpenWrite(fileName, fileName))
                        {
                            file.Write(randomFileData);
                        }

                        fs.SetMetadata(fileName, new BsonDocument() { { "Gaming", true } });
                        gamerUser.UploadedFiles.Add(fileName);
                    }
                }

                users.Update(gamerUsers);

                db.Commit();
            }
            catch
            {
                db.Rollback();
                throw;
            }
        });

        // yield return BenchStep.Record(CategoryGeneral, $"Rebuild", () => /* Closest I could find. */ db.Rebuild());
        // It throws 'Detected loop in FindAll({0})', issue: https://github.com/litedb-org/LiteDB/issues/2582


        var buffer = new byte[256];
        yield return BenchStep.Record(CategoryFs, "Read 256 bytes chunk from random file positions", () =>
        {
            var fileNames = fs.FindAll().Take(50).ToList(); // Test on 5 random files

            foreach (var fileInfo in fileNames)
            {
                using var stream = fs.OpenRead(fileInfo.Id);
                var maxOffset = Math.Max(0, (int)stream.Length - buffer.Length);
                var offset = Program.random.Next(0, maxOffset + 1);
                stream.Seek(offset, SeekOrigin.Begin);
                stream.ReadExactly(buffer, 0, buffer.Length);
            }
        });

        yield return BenchStep.Record(CategoryGeneral, "Update users with username starting with 'a'", () =>
        {
            var updatedCount = users.UpdateMany(
                BsonExpression.Create(
                    "{ Id, Username, InterestedCategories: CONCAT($.InterestedCategories, [@0]), UploadedFiles }",
                    "UpdatedUser"
                ),
                db.Mapper.GetExpression<UserLiteDB, bool>(x => x.Username.StartsWith("a"))
            );
            
            if (updatedCount == 0)
            {
                throw new InvalidOperationException($"updatedCount = {updatedCount}");
            }
        });


        yield return BenchStep.Record(CategoryGeneral, "Delete users with <= 2 categories", () =>
        {
            var usersToDelete = users.Query()
                .Where(x => x.InterestedCategories.Count <= 2)
                .ToList();

            foreach (var user in usersToDelete)
            {
                // Clean up associated files first
                foreach (var fileName in user.UploadedFiles)
                {
                    fs.Delete(fileName);
                }
            }

            users.DeleteMany(x => x.InterestedCategories.Count <= 2);
        });

        yield return BenchStep.Record(CategoryGeneral, "Paginated query (page 3 of 50 users)", () =>
        {
            var page3Users = users.Query()
                .OrderBy(x => x.Username).Select(x => x.Username)
                .Offset(100)
                .Limit(50)
                .ToList();
            if (page3Users.Count != 50)
            {
                throw new InvalidOperationException();
            }
        });

        yield return BenchStep.Record(CategoryGeneral, "Complex query with multiple conditions", () =>
        {
            var complexResults = users.Query()
                .Where(x => x.Username.StartsWith("b") || x.Username.StartsWith("c"))
                .Where(x => x.InterestedCategories.Count >= 1)
                .Where(x => x.UploadedFiles.Count > 0)
                .Select(x => x.UploadedFiles[0].Substring(2, 4))
                .ToList();
        });

        yield return BenchStep.Record(CategoryFs, "Retrieve file&tags from users", () =>
        {
            // Get 100 files with their username and metadata.
            var usersWithFiles = users.Query()
                .Where(x => x.UploadedFiles.Count == 10)
                .Limit(10)
                .ToList();

            var userFilesAndTags = new List<(string username, string filename, BsonDocument metadata)>();

            foreach (var user in usersWithFiles)
            {
                foreach (var fileName in user.UploadedFiles)
                {
                    var metadata = fs.FindById(fileName)!.Metadata;
                    userFilesAndTags.Add((user.Username, fileName, metadata));
                }
            }
        });

        yield return BenchStep.Record(CategoryGeneral, "Count users by username first letter", () =>
        {
            // Handcrafted GroupBy query.
            var letterCounts =
                users
                    .Query()
                    .GroupBy(BsonExpression.Create("SUBSTRING(Username, 0, 1)"))
                    .Select(BsonExpression.Create("{key: @key, count: COUNT(*)}"))
                    .ToEnumerable()
                    .ToDictionary(k => k["key"].AsString, e => e["count"].AsInt64);

        });

        var bufferSmaller = new byte[1024];

        yield return BenchStep.Record(CategoryFs, "Read first 1KB from all gaming files", () =>
        {
            foreach (var fileInfo in fs.Find(x => x.Metadata["Gaming"]))
            {
                using var stream = fs.OpenRead(fileInfo.Id);
                stream.ReadExactly(bufferSmaller, 0, Math.Min(bufferSmaller.Length, (int)stream.Length));
            }
        });
    }

    // SoloDB Performance Tests
    internal static IEnumerable<BenchStep> BenchSoloDB()
    {
        if (Directory.Exists("./test-db"))
        {
            Directory.Delete("./test-db", true);
        }

        Directory.CreateDirectory("./test-db");

        using var db = new SoloDB($"./test-db/solodb.db");
        var users = db.GetCollection<UserSoloDB>();

        var usersToInsert = new UserSoloDB[Program.UserCount];
        for (var i = 0; i < usersToInsert.Length; i++)
        {
            usersToInsert[i] = new UserSoloDB();
        }


        yield return BenchStep.Record(CategoryGeneral, $"Inserting {usersToInsert.Length} users", () => users.InsertBatch(usersToInsert));

        List<string> gamingUsers = null;

        yield return BenchStep.Record(CategoryGeneral, $"Searching for gaming users.", () =>
        {
            gamingUsers = users
                .Where(x => x.InterestedCategories.Contains("Gaming"))
                .Select(x => x.Username)
                .ToList();
        });


        var randomFileData = new byte[64 * 1024];
        Program.random.NextBytes(randomFileData);
        var subSetOfGamers = gamingUsers.Take(200).ToList();

        yield return BenchStep.Record(CategoryFs, $"Upload 3 {randomFileData.Length / 1024} kb files for 200 gamers in a transaction.", () =>
        {
            db.WithTransaction(delegate (TransactionalSoloDB tx)
            {
                var users = tx.GetCollection<UserSoloDB>();
                
                var gamerUsers = users.Where(u => subSetOfGamers.Contains(u.Username)).ToList();

                foreach (var user in gamerUsers)
                {
                    for (int i = 0; i < 3; i++)
                    {
                        // We can query very fast by the path.
                        var filePath = $"/data/{user.Username}/footage_gaming_{i}.bin";

                        var fs = tx.FileSystem;
                        using (var file = fs.OpenOrCreateAt(filePath))
                        {
                            file.Write(randomFileData);
                        }

                        fs.SetMetadata(filePath, "Tags", "Gaming");

                        user.UploadedFiles.Add(filePath);
                    }
                    
                    users.Update(user);
                }
            });
        });

        yield return BenchStep.Record(CategoryGeneral, $"Optimize", () => db.Optimize());


        var buffer = new byte[256];
        yield return BenchStep.Record(CategoryFs, "Read 256 bytes chunk from random file positions", () =>
        {
            var fs = db.FileSystem;
            var fileNames = fs.ListDirectoriesAt("/data/").Take(50).Select(d => $"{d.FullPath}/footage_gaming_1.bin");

            foreach (var filePath in fileNames)
            {
                using var stream = fs.OpenAt(filePath);
                var maxOffset = Math.Max(0, (int)stream.Length - buffer.Length);
                var offset = Program.random.Next(0, maxOffset + 1);
                stream.Seek(offset, SeekOrigin.Begin);
                stream.ReadExactly(buffer, 0, buffer.Length);
            }
        });

        yield return BenchStep.Record(CategoryGeneral, "Update users with username starting with 'a'", () =>
        {
            var updatedCount = users.UpdateMany(x => x.Username.StartsWith("a"), user => user.InterestedCategories.Add("UpdatedUser"));
            if (updatedCount == 0)
            {
                throw new InvalidOperationException($"updatedCount = {updatedCount}");
            }
        });


        yield return BenchStep.Record(CategoryGeneral, "Delete users with <= 2 categories", () =>
        {
            var usersToDelete = users
                .Where(x => x.InterestedCategories.Count <= 2)
                .Select(x => x.Username)
                .ToList();

            foreach (var user in usersToDelete)
            {
                // Clean up associated files first
                db.FileSystem.DeleteDirAt($"/data/{user}");
            }

            users.DeleteMany(u => u.InterestedCategories.Count <= 2);
        });

        yield return BenchStep.Record(CategoryGeneral, "Paginated query (page 3 of 50 users)", () =>
        {
            var page3Users = users
                .OrderBy(x => x.Username).Select(x => x.Username)
                .Skip(100)
                .Take(50)
                .ToList();

            if (page3Users.Count != 50)
            {
                throw new InvalidOperationException();
            }
        });

        yield return BenchStep.Record(CategoryGeneral, "Complex query with multiple conditions", () =>
        {
            var complexResults = users
                .Where(x => x.Username.StartsWith("b") || x.Username.StartsWith("c"))
                .Where(x => x.InterestedCategories.Count >= 1)
                .Where(x => x.UploadedFiles.Count > 0)
                .Select(x => x.UploadedFiles[0].Substring(2, 4))
                .ToList();
        });

        yield return BenchStep.Record(CategoryFs, "Retrieve file&tags from users", () =>
        {
            // Get 100 files with their username and metadata.
            var userFilesAndTags = new List<(string username, string filepath, IReadOnlyDictionary<string, string> metadata)>();

            var usersFiles = db.FileSystem.RecursiveListEntriesAtLazy("/data/").Where(x => x.IsFile).Take(100).ToList();

            foreach (var files in usersFiles)
            {
                var userName = files.FullPath.Split('/', 4)[2];
                userFilesAndTags.Add((userName, files.FullPath, files.Metadata));
            }
        });

        yield return BenchStep.Record(CategoryGeneral, "Count users by username first letter", () =>
        {
            var letterCounts =
                users
                    .GroupBy(x => x.Username[0])
                    .Select(x => new { Key = x.Key, Count = x.Count() })
                    .ToDictionary(k => k.Key.ToString(), e => e.Count);

        });

        var bufferSmaller = new byte[1024];

        yield return BenchStep.Record(CategoryFs, "Read first 1KB from all gaming files", () =>
        {
            var fs = db.FileSystem;
            foreach (var fileInfo in db.FileSystem.RecursiveListEntriesAtLazy("/data")
                         .Where(x => x.IsFile && x.Metadata.ContainsKey("Tags") && x.Metadata["Tags"] == "Gaming").Select(x => x.file).ToList())
            {
                using var stream = fs.Open(fileInfo);
                stream.ReadExactly(bufferSmaller, 0, Math.Min(bufferSmaller.Length, (int)stream.Length));
            }
        });
    }
}