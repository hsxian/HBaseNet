using System;
using System.ComponentModel.Design;
using System.Linq;
using System.Threading.Tasks;
using HBaseNet.HRpc;
using HBaseNet.Utility;
using Pb;
using Serilog;

namespace HBaseNet.Console
{
    public class SingleThreadOperation
    {
        private readonly HBaseClient _client;

        public SingleThreadOperation(HBaseClient client)
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
                var rs = await _client.SendRPC<MultiResponse>(new MutateCall(Program.Table, rowKey, Program.Values,
                    MutationProto.Types.MutationType.Put));
            }
        }

        public async Task ExecScan()
        {
            var scanResults = await _client.Scan(
                new ScanCall(Program.Table, Program.Family, "0".ToUtf8Bytes(), "9".ToUtf8Bytes())
                    {Families = Program.Family});
            Log.Information($"scan result count:{scanResults.Count}");
        }

        public async Task ExecScanAndDelete()
        {
            var scanResults =
                await _client.Scan(new ScanCall(Program.Table, Program.Family, "0".ToUtf8Bytes(), "1".ToUtf8Bytes()));
            Log.Information($"scan result count:{scanResults.Count}");
            foreach (var result in scanResults)
            {
                var rowKey = result.Cell.Select(t => t.Row.ToStringUtf8()).Single();
                var delResult = await _client.SendRPC<MutateResponse>(new MutateCall(Program.Table, rowKey, null,
                    MutationProto.Types.MutationType.Delete));
                Log.Logger.Information($"delete row at key: {rowKey}, processed:{delResult.Processed}");
            }
        }
    }
}