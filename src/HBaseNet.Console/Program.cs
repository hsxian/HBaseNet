using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using HBaseNet.Utility;
using Pb;

namespace HBaseNet.Console
{
    using System;

    class Program
    {
        static async Task Main(string[] args)
        {
            var ip = $"hbase-docker";
            var table = "student";

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
            var tasks = new List<Task<MutateResponse>>();
            for (var i = 0; i < 20; i++)
            {
                var rowKey = new string(DateTime.Now.Ticks.ToString().Reverse().ToArray());
                var rs = await client.Put(table, rowKey, values);
                // tasks.Add(rs);
            }

            await Task.WhenAll(tasks);
        }
    }
}