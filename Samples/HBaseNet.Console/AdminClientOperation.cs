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
        public async Task ExecAll()
        {
            var admin = new HBaseClient(Program.ZkQuorum, ClientType.AdminClient);
            var table = DateTime.Now.ToString("yyyyMMddHHmmss").ToUtf8Bytes();
            var cols = new string[] { "info" ,"special"};
            var create = new CreateTableCall(table, cols)
            {
                SplitKeys = new[] { "0", "5" }
            };
            var delete = new DeleteTableCall(table);

            var ct = await admin.SendRPC<CreateNamespaceResponse>(create);
            Log.Logger.Information($"Create table: {table.ToUtf8String()},result:{ct != null}");
            var dt = await admin.SendRPC<DisableTableResponse>(new DisableTableCall(table));
            Log.Logger.Information($"Disable table: {table.ToUtf8String()},result:{dt != null}");
            var del = await admin.SendRPC<DeleteTableResponse>(delete);
            Log.Logger.Information($"Delete table: {table.ToUtf8String()},result:{del != null}");
        }
    }
}