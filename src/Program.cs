using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;
using MongoDB.Driver.Builders;

namespace src
{
    public class Junk
    {
        public ObjectId Id { get; set; }

        [BsonElement("rowId")]
        public string RowId { get; set; }

        [BsonElement("data")]
        public string Data { get; set; }
    }

    class Program
    {
        const string availableChars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        static Random random = new Random();

        static void Main(string[] args)
        {
            if (args.Length < 1)
            {
                SpawnChildren();
            }
            else
            {
                Console.WriteLine("Attempting to insert data.");
                InsertData(int.Parse(args[0]));
            }
        }

        static void SpawnChildren(bool useThreads = false)
        {
            if (useThreads)
            {
                var tasks = new List<Task>();
                for (int i = 0; i < 10; i++)
                {
                    tasks.Add(SpawnThread(i));
                }

                foreach(var task in tasks)
                {
                    task.Wait();
                }
            }
            else
            {
                var processes = new List<Process>();
                for (int i = 0; i < 10; i++)
                {
                    processes.Add(SpawnProcess(i));
                }

                foreach (var proc in processes)
                {
                    proc.WaitForExit();
                }
            }
        }

        static Process SpawnProcess(int number)
        {
            Console.WriteLine($"Spawning process {number}.");
            var process = new Process();
            process.StartInfo.FileName = "dotnet";
            process.StartInfo.Arguments = $"run {number.ToString()}";
            process.StartInfo.UseShellExecute = false;
            process.StartInfo.CreateNoWindow = true;
            process.StartInfo.RedirectStandardOutput = true;
            process.OutputDataReceived += (sender, data) => {
                Console.WriteLine($"[{number}] - stdout - " + data.Data);
            };
            process.StartInfo.RedirectStandardError = true;
            process.ErrorDataReceived += (sender, data) => {
                Console.WriteLine($"[{number}] - stderr - " + data.Data);
            };
            var started = process.Start();

            Console.WriteLine($"{number} {(started ? "started successfully." : "failed to start.")}");

            return process;
        }

        static Task SpawnThread(int number)
        {
            return Task.Run(() => InsertData(number));
        }

        static void InsertData(int start, int run = 5000)
        {
            var client = new MongoClient("mongodb://localhost:27017");
            var database = client.GetDatabase("demo"); // no need to create it.
            var col = database.GetCollection<Junk>("cur_data");

            var init = start * run;
            Console.WriteLine($"Inserting {init} through {init + run}.");
            for (var i = init; i < init + run; i++)
            {
                var junk = new Junk()
                {
                    RowId = i.ToString(),
                    Data = RandomString(2048)
                };
                UpsertJunk(col, junk);
            }
            Console.WriteLine($"Finished inserting {init} through {init + run}.");
        }

        static void UpsertJunk(IMongoCollection<Junk> col, Junk junk)
        {
            var filterCondition = Builders<Junk>.Filter.Eq(j => j.RowId, junk.RowId);
            var updateCondition = Builders<Junk>.Update.Set(j => j.Data, junk.Data);
            col.UpdateOne(filterCondition, updateCondition, new UpdateOptions { IsUpsert = true });
        }

        static string RandomString(int length = 1024)
        {
            var chars = new char[length];
            for (int i = 0; i < chars.Length; i++)
            {
                chars[i] = availableChars[random.Next(availableChars.Length)];
            }

            return new String(chars);
        }
    }
}
