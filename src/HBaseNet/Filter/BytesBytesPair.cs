using System.CodeDom.Compiler;
using Google.Protobuf;
using Pb;

namespace HBaseNet.Filter
{
    public class BytesBytesPair
    {
        public byte[] First { get; set; }
        public byte[] Second { get; set; }

        public Pb.BytesBytesPair ConvertToPB()
        {
            return new Pb.BytesBytesPair
            {
                First = ByteString.CopyFrom(First),
                Second = ByteString.CopyFrom(Second)
            };
        }
    }
}