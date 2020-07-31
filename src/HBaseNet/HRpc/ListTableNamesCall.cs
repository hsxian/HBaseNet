using Google.Protobuf;
using HBaseNet.Utility;
using Pb;

namespace HBaseNet.HRpc
{
    public class ListTableNamesCall : BaseCall
    {
        public override string Name => "GetTableNames";

        public bool IncludeSysTables { get; set; }
        public string Regex { get; set; } = ".*";
        public string Namespace { get; set; } = string.Empty;

        public override IMessage ParseResponseFrom(byte[] bts)
        {
            return bts.TryParseTo(GetTableNamesResponse.Parser.ParseFrom);
        }

        public override byte[] Serialize()
        {
            return new GetTableNamesRequest
            {
                IncludeSysTables = IncludeSysTables,
                Regex = Regex,
                Namespace = Namespace
            }.ToByteArray();
        }
    }
}