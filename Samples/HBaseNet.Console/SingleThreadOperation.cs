using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using HBaseNet.HRpc;
using HBaseNet.Utility;
using Pb;
using Serilog;

namespace HBaseNet.Console
{
    public class SingleThreadOperation
    {
        private readonly IStandardClient _client;

        public SingleThreadOperation(IStandardClient client)
        {
            _client = client;
        }

        public async Task<bool> CheckTable()
        {
            var result = await _client.CheckTable(Program.Table);
            Log.Logger.Information($"check table '{Program.Table}': {result}");
            return result;
        }

        public async Task ExecPut(int count)
        {
            for (var i = 0; i < count; i++)
            {
                var rowKey = new string(DateTime.Now.Ticks.ToString().Reverse().ToArray());
                var rs = await _client.Put(new MutateCall(Program.Table, rowKey, Program.Values));
            }
        }

        public async Task ExecScan()
        {
            var sc = new ScanCall(Program.Table, Program.Family, "0".ToUtf8Bytes(), "".ToUtf8Bytes())
            {
                Families = Program.Family,
                TimeRange = new TimeRange
                {
                    From = new DateTime(2018, 1, 1).ToUnixU13(),
                    To = new DateTime(2019, 1, 2).ToUnixU13()
                },
                NumberOfRows = 10000000
            };
            var scanResults = await _client.Scan(sc);
            Log.Information($"scan result count:{scanResults.Count}");
        }

        public async Task ExecScanAndDelete()
        {
            var scanResults =
                await _client.Scan(new ScanCall(Program.Table, Program.Family, "0".ToUtf8Bytes(), "1".ToUtf8Bytes()) { NumberOfRows = 1 });
            Log.Information($"scan result count:{scanResults.Count}");
            foreach (var result in scanResults)
            {
                var rowKey = result.Cell.Select(t => t.Row.ToStringUtf8()).Single();
                var delResult = await _client.Delete(new MutateCall(Program.Table, rowKey, null));
                Log.Logger.Information($"delete row at key: {rowKey}, processed:{delResult.Processed}");
            }
        }

        public async Task ExecCheckAndPut()
        {
            var rowKey = new string(DateTime.Now.Ticks.ToString().Reverse().ToArray());
            var put = new MutateCall(Program.Table, rowKey, Program.Values)
            {
                Key = new string(DateTime.Now.Ticks.ToString().Reverse().ToArray()).ToUtf8Bytes()
            };

            var resultT = await _client.CheckAndPut(put, "default", "key", "ex".ToUtf8Bytes(), new CancellationToken());
        }
    }
}