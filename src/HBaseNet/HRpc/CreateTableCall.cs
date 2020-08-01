using System.Collections.Generic;
using System.Linq;
using Google.Protobuf;
using HBaseNet.HRpc.Descriptors;
using HBaseNet.Utility;
using Pb;

namespace HBaseNet.HRpc
{
    public class CreateTableCall : BaseCall
    {
        public IEnumerable<ColumnFamily> Columns { get; }
        public override string Name => "CreateTable";
        public string[] SplitKeys { get; set; }

        public CreateTableCall(byte[] table, IEnumerable<ColumnFamily> columns)
        {
            Table = table;
            Columns = columns;
        }

        public CreateTableCall(string table, IEnumerable<ColumnFamily> columns) : this(table.ToUtf8Bytes(), columns)
        {
        }

        public override byte[] Serialize()
        {
            var cTable = new Pb.CreateTableRequest
            {
                TableSchema = new TableSchema
                {
                    TableName = new TableName
                    {
                        Namespace = ByteString.CopyFromUtf8("default"),
                        Qualifier = ByteString.CopyFrom(Table)
                    }
                }
            };

            if (SplitKeys?.Any() == true)
            {
                cTable.SplitKeys.AddRange(SplitKeys.Select(ByteString.CopyFromUtf8).ToArray());
            }

            if (Columns?.Any() == true)
            {
                cTable.TableSchema.ColumnFamilies.AddRange(Columns.Select(t => t.ToPbSchema()).ToArray());
            }

            return cTable.ToByteArray();
        }

        public override IMessage ParseResponseFrom(byte[] bts)
        {
            return bts.TryParseTo(CreateTableResponse.Parser.ParseFrom);
        }
    }
}