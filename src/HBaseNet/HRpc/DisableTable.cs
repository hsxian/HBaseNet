using Google.Protobuf;
using HBaseNet.Utility;
using Pb;

namespace HBaseNet.HRpc
{
    public class DisableTableCall : BaseCall
    {
        public override string Name => "DisableTable";

        public DisableTableCall(byte[] table)
        {
            Table = table;
        }

        public DisableTableCall(string table) : this(table.ToUtf8Bytes())
        {
        }

        public override byte[] Serialize()
        {
            var dTable = new Pb.DisableTableRequest
            {
                TableName = new TableName
                {
                    Namespace = ByteString.CopyFromUtf8("default"),
                    Qualifier = ByteString.CopyFrom(Table)
                }
            };
            return dTable.ToByteArray();
        }

        public override IMessage ParseResponseFrom(byte[] bts)
        {
            return bts.TryParseTo(DisableTableResponse.Parser.ParseFrom);
        }
    }
}