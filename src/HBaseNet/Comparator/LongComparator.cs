using Google.Protobuf;
using HBaseNet.Const;

namespace HBaseNet.Comparator
{
    public class LongComparator : IComparator
    {
        public string Name { get; }
        public Pb.ByteArrayComparable Comparable { get; }

        public LongComparator(Pb.ByteArrayComparable comparable)
        {
            Comparable = comparable;
            Name = ConstString.ComparatorPath + nameof(LongComparator);
        }

        public Pb.Comparator ConvertToPBComparator()
        {
            var internalComparator = new Pb.LongComparator
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