using System.Collections.Generic;
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

        private readonly Dictionary<string, string> defaultAttributes = new Dictionary<string, string>
        {
            {"BLOOMFILTER", "ROW"},
            {"VERSIONS", "3"},
            {"IN_MEMORY", "false"},
            {"KEEP_DELETED_CELLS", "FALSE"},
            {"DATA_BLOCK_ENCODING", "FAST_DIFF"},
            {"TTL", "2147483647"},
            {"COMPRESSION", "NONE"},
            {"MIN_VERSIONS", "0"},
            {"BLOCKCACHE", "true"},
            {"BLOCKSIZE", "65536"},
            {"REPLICATION_SCOPE", "0"},
        };

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
                var attrs = defaultAttributes.Select(t =>
                    new BytesBytesPair
                    {
                        First = ByteString.CopyFromUtf8(t.Key),
                        Second = ByteString.CopyFromUtf8(t.Value)
                    }).ToArray();

                foreach (var col in Columns)
                {
                    var cfs = new ColumnFamilySchema
                    {
                        Name = ByteString.CopyFromUtf8(col),
                    };
                    cfs.Attributes.AddRange(attrs);
                    cTable.TableSchema.ColumnFamilies.Add(cfs);
                }
            }

            return cTable.ToByteArray();
        }

        public override IMessage ParseResponseFrom(byte[] bts)
        {
            return bts.TryParseTo(CreateTableResponse.Parser.ParseFrom);
        }
    }
}