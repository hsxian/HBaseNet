using System;
using System.Threading.Tasks;
using HBaseNet.HRpc;
using HBaseNet.Utility;
using Pb;
using Serilog;

namespace HBaseNet.Console
{
    public class AdminClientOperation
    {
        public IAdminClient _admin { get; }

        public AdminClientOperation(IAdminClient admin)
        {
            _admin = admin;
        }

        public async Task ExecAll()
        {
            var table = DateTime.Now.ToString("yyyyMMddHHmmss").ToUtf8Bytes();
            var cols = new string[] {"info", "special"};
            var create = new CreateTableCall(table, cols)
            {
                SplitKeys = new[] {"0", "5"}
            };
            var delete = new DeleteTableCall(table);

            var ct = await _admin.CreateTable(create);
            Log.Logger.Information($"Create table: {table.ToUtf8String()},result:{ct}");
            var dt = await _admin.DisableTable(new DisableTableCall(table));
            Log.Logger.Information($"Disable table: {table.ToUtf8String()},result:{dt}");
            var del = await _admin.DeleteTable(delete);
            del = await _admin.DeleteTable(delete);
            Log.Logger.Information($"Delete table: {table.ToUtf8String()},result:{del}");
        }
    }
}