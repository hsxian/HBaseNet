using Google.Protobuf;
using HBaseNet.Const;

namespace HBaseNet.Comparator
{
    public class BinaryComparator : IComparator
    {
        public string Name { get; }

        public Pb.ByteArrayComparable Comparable { get; }

        public BinaryComparator(Pb.ByteArrayComparable comparable)
        {
            Comparable = comparable;
            Name = ConstString.ComparatorPath + nameof(BinaryComparator);
        }

        public Pb.Comparator ConvertToPBComparator()
        {
            var internalComparator = new Pb.BinaryComparator
            {
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