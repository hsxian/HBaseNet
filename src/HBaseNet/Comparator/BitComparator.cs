using Google.Protobuf;
using HBaseNet.Const;

namespace HBaseNet.Comparator
{
    public class BitComparator : IComparator
    {
        public string Name { get; }
        public Pb.ByteArrayComparable Comparable { get; }
        public Pb.BitComparator.Types.BitwiseOp BitwiseOp { get; }

        public BitComparator(Pb.BitComparator.Types.BitwiseOp bitwiseOp, Pb.ByteArrayComparable comparable)
        {
            BitwiseOp = bitwiseOp;
            Comparable = comparable;
            Name = ConstString.ComparatorPath + nameof(BitComparator);
        }

        public Pb.Comparator ConvertToPBComparator()
        {
            var internalComparator = new Pb.BitComparator
            {
                BitwiseOp = BitwiseOp,
                Comparable = Comparable
            };
            var comparator = new Pb.Comparator
            {
                Name = Name,
                SerializedComparator = internalComparator.ToByteString()
            };
            return comparator;
        }
    }
}