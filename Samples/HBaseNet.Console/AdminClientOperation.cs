using System;
using System.Threading.Tasks;
using HBaseNet.HRpc;
using HBaseNet.HRpc.Descriptors;
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
            var table = DateTime.Now.ToString("yyyyMMddHHmmss");
            var cols = new[]
            {
                new ColumnFamily("info")
                {
                    Compression = Compression.GZ,
                    KeepDeletedCells = KeepDeletedCells.TRUE
                },
                new ColumnFamily("special")
                {
                    Compression = Compression.GZ,
                    KeepDeletedCells = KeepDeletedCells.TTL,
                    DataBlockEncoding = DataBlockEncoding.PREFIX
                }
            };
            var create = new CreateTableCall(table, cols)
            {
                SplitKeys = new[] { "0", "5" }
            };
            var disable = new DisableTableCall(table);
            var delete = new DeleteTableCall(table);

            var ct = await _admin.CreateTable(create);
            Log.Logger.Information($"Create table: {table},result:{ct}");
            var dt = await _admin.DisableTable(disable);
            Log.Logger.Information($"Disable table: {table},result:{dt}");
            var del = await _admin.DeleteTable(delete);
            Log.Logger.Information($"Delete table: {table},result:{del}");
        }
    }
}