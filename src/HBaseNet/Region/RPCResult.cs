using System;
using System.IO;
using Google.Protobuf;

namespace HBaseNet.Region
{
    public class RPCResult
    {
        public uint CallId { get; set; }
        public IMessage Msg { get; set; }
        public Exception Error { get; set; }
    }
}