using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Pb;
using Serilog;

namespace HBaseNet.Console
{
    public class MultiThreadOperation
    {
        private readonly HBaseClient _client;

        public MultiThreadOperation(HBaseClient client)
        {
            _client = client;
        }

        public async Task ExecPut(int count)
        {
            var tasks = new List<Task<MutateResponse>>();
            for (var i = 0; i < count; i++)
            {
                var rowKey = new string(DateTime.Now.Ticks.ToString().Reverse().ToArray());
                var rs = _client.Put(Program.Table, rowKey, Program.Values);
                tasks.Add(rs);
            }

            await Task.WhenAll(tasks);
        }
    }
}