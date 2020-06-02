using System;
using System.IO;
using Google.Protobuf;

namespace HBaseNet.Region
{
    public class RPCResult
    {
        public IMessage Msg { get; set; }
        public Exception Erroe { get; set; }
    }
}