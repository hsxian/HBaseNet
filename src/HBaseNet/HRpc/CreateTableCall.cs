using System.Linq;
using Google.Protobuf;
using Google.Protobuf.Collections;
using HBaseNet.Utility;
using Pb;

namespace HBaseNet.HRpc
{
    public class CreateTableCall : BaseCall
    {
        public string[] Columns { get; }
        public override string Name => "CreateTable";
        public string[] SplitKeys { get; set; }

        public CreateTableCall(byte[] table, string[] columns)
        {
            Table = table;
            Columns = columns;
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
                var cols = Columns.Select(t => new Pb.ColumnFamilySchema
                {
                    Name = ByteString.CopyFromUtf8(t)
                }).ToArray();
                cTable.TableSchema.ColumnFamilies.AddRange(cols);
            }

            return cTable.ToByteArray();
        }

        public override IMessage ParseResponseFrom(byte[] bts)
        {
            return bts.TryParseTo(CreateNamespaceResponse.Parser.ParseFrom);
        }
    }
}