using System;
using Google.Protobuf;
using HBaseNet.Utility;
using Pb;

namespace HBaseNet.HRpc
{
    public class GetProcedureStateCall : BaseCall
    {
        public override string Name => "getProcedureResult";
        public ulong ProcId { get; }
        public GetProcedureStateCall(ulong procId)
        {
            ProcId = procId;
        }

        public override byte[] Serialize()
        {
            var req = new GetProcedureResultRequest {ProcId = ProcId};
            return req.ToByteArray();
        }

        public override IMessage ParseResponseFrom(byte[] bts)
        {
            return bts.TryParseTo(GetProcedureResultResponse.Parser.ParseFrom);
        }
    }
}