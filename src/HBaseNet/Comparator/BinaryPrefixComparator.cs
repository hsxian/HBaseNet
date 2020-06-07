using Google.Protobuf;
using HBaseNet.Const;

namespace HBaseNet.Comparator
{
    public class BinaryPrefixComparator : IComparator
    {
        public string Name { get; }
        public Pb.ByteArrayComparable Comparable { get; }

        public BinaryPrefixComparator(Pb.ByteArrayComparable comparable)
        {
            Comparable = comparable;
            Name = ConstString.ComparatorPath + nameof(BinaryPrefixComparator);
        }

        public Pb.Comparator ConvertToPBComparator()
        {
            var internalComparator = new Pb.BinaryPrefixComparator
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