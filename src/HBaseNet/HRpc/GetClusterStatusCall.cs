using Google.Protobuf;
using HBaseNet.Utility;
using Pb;

namespace HBaseNet.HRpc
{
    public class GetClusterStatusCall : BaseCall
    {
        public override string Name => "GetClusterStatus";

        public override IMessage ParseResponseFrom(byte[] bts)
        {
            return bts.TryParseTo(GetClusterStatusResponse.Parser.ParseFrom);
        }

        public override byte[] Serialize()
        {
            var req = new GetClusterStatusRequest();
            return req.ToByteArray();
        }
    }
}