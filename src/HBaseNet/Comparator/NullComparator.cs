using Google.Protobuf;
using HBaseNet.Const;

namespace HBaseNet.Comparator
{
    public class NullComparator : IComparator
    {
        public string Name { get; }
        public Pb.ByteArrayComparable Comparable { get; }

        public NullComparator()
        {
            Name = ConstString.ComparatorPath + nameof(NullComparator);
        }

        public Pb.Comparator ConvertToPBComparator()
        {
            var internalComparator = new Pb.NullComparator();
            var comparator = new Pb.Comparator
            {
                Name = Name,
                SerializedComparator = internalComparator.ToByteString()
            };
            return comparator;
        }
    }
}