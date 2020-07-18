using Google.Protobuf;

namespace HBaseNet.Comparator
{
    public class ByteArrayComparable
    {
        public byte[] Value { get; }

        public ByteArrayComparable(byte[] value)
        {
            Value = value;
        }

        public Pb.ByteArrayComparable ConvertToPB()
        {
            var pbVersion = new Pb.ByteArrayComparable();
            if (Value != null)
            {
                pbVersion.Value = ByteString.CopyFrom(Value);
            }
            return pbVersion;
        }
    }
}