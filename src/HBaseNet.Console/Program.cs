using System.Collections.Generic;
using System.ComponentModel.Design;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using HBaseNet.Utility;
using Pb;
using Serilog;

namespace HBaseNet.Console
{
    using System;

    class Program
    {
        static async Task Main(string[] args)
        {
            Log.Logger = new LoggerConfiguration()
                .Enrich.FromLogContext()
                .MinimumLevel.Debug()
                .WriteTo.Console(
                    outputTemplate: "[{Timestamp:yyyy-MM-dd HH:mm:ss} {Level:u3}] {Message:lj}{NewLine}{Exception}")
                .CreateLogger();

            HBaseConfig.Instance.LoggerFactory
                .AddSerilog(Log.Logger);

            var ip = $"hbase-docker";
            var table = "student";

            var family = new Dictionary<string, string[]>
            {
                {"default", new[] {"key"}}
            };
            var values = new Dictionary<string, IDictionary<string, byte[]>>
            {
                {
                    "default", new Dictionary<string, byte[]>
                    {
                        {"key", "value".ToUtf8Bytes()}
                    }
                }
            };
            var client = new HBaseClient(ip);
            var sth = new Stopwatch();

            Log.Logger.Information($"check table '{table}': {await client.CheckTable(table)}");

            const int putCount = 0;
            sth.Start();
            for (var i = 0; i < putCount; i++)
            {
                var rowKey = new string(DateTime.Now.Ticks.ToString().Reverse().ToArray());
                var rs = await client.Put(table, rowKey, values);
            }

            Log.Logger.Information($"put value count: {putCount}, took {sth.Elapsed}");

            var scanResults = await client.Scan(table, family, "0".ToUtf8Bytes(), "9".ToUtf8Bytes());

            foreach (var result in scanResults.Results)
            {
                var rowKey = result.Cell.Select(t => t.Row.ToStringUtf8()).Single();
                var getResult = await client.Get(table, rowKey, null);
                if (getResult.Result.Cell.Any(t => t.Row.ToStringUtf8() == rowKey) == false)
                    throw new CheckoutException();
                var delResult = await client.Delete(table, rowKey, null);
                Log.Logger.Information($"delete row at key: {rowKey}, processed:{delResult.Processed}");
            }
        }
    }
}