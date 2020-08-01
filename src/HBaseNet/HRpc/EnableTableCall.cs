using Google.Protobuf;
using HBaseNet.Utility;
using Pb;

namespace HBaseNet.HRpc
{
    public class EnableTableCall : BaseCall
    {
        public override string Name => "EnableTable";

        public EnableTableCall(byte[] table)
        {
            Table = table;
        }

        public EnableTableCall(string table) : this(table.ToUtf8Bytes())
        {
        }

        public override byte[] Serialize()
        {
            var eTable = new Pb.EnableTableRequest()
            {
                TableName = new TableName
                {
                    Namespace = ByteString.CopyFromUtf8("default"),
                    Qualifier = ByteString.CopyFrom(Table)
                }
            };
            return eTable.ToByteArray();
        }

        public override IMessage ParseResponseFrom(byte[] bts)
        {
            return bts.TryParseTo(EnableTableResponse.Parser.ParseFrom);
        }
    }
}