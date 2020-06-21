using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using HBaseNet.HRpc;
using HBaseNet.Utility;
using Pb;
using Serilog;

namespace HBaseNet.Console
{
    using System;

    class Program
    {
        public const string ZkQuorum = "hbase-docker";
        public const string Table = "student";
        public static Dictionary<string, string[]> Family;
        public static Dictionary<string, IDictionary<string, byte[]>> Values;

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

            Family = new Dictionary<string, string[]>
            {
                {"default", new[] {"key"}}
            };
            Values = new Dictionary<string, IDictionary<string, byte[]>>
            {
                {
                    "default", new Dictionary<string, byte[]>
                    {
                        {"key", "value".ToUtf8Bytes()}
                    }
                }
            };

            // var admin = new HBaseClient(ZkQuorum, ClientType.AdminClient);
            // var ct = await admin.SendRPC<CreateTableResponse>(new CreateTableCall(
            //     DateTime.Now.Ticks.ToString().ToUtf8Bytes(),
            //     new[] {"info"}));
            var client = new HBaseClient(ZkQuorum);
            var sth = new Stopwatch();
            var sto = new SingleThreadOperation(client);
            if (await sto.CheckTable() == false) return;
            const int putCount = 100;

            var mto = new MultiThreadOperation(client);
            sth.Restart();
            // await mto.ExecPut(putCount);
            Log.Logger.Information($"exec multi thread put ,count: {putCount},take :{sth.Elapsed}");

            sth.Restart();
            // await sto.ExecPut(putCount);
            // Log.Logger.Information($"exec single thread put ,count: {putCount},take :{sth.Elapsed}");


            sth.Restart();
            await sto.ExecScan();
            Log.Logger.Information($"exec scan,take :{sth.Elapsed}");
            // await sto.ExecScanAndDelete();
            Console.ReadLine();
        }
    }
}