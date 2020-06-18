using Google.Protobuf;
using HBaseNet.Utility;
using Pb;

namespace HBaseNet.HRpc
{
    public class DeleteTableCall : BaseCall
    {
        public override string Name => "DeleteTable";

        public DeleteTableCall(byte[] table)
        {
            Table = table;
        }

        public override byte[] Serialize()
        {
            var dTable = new Pb.DeleteTableRequest
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
            return bts.TryParseTo(DeleteTableResponse.Parser.ParseFrom);
        }
    }
}