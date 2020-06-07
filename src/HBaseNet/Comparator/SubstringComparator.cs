using Google.Protobuf;
using HBaseNet.Const;

namespace HBaseNet.Comparator
{
    public class SubstringComparator : IComparator
    {
        public string Name { get; }
        public string Substr { get; }
        public Pb.ByteArrayComparable Comparable { get; }

        public SubstringComparator(string substr)
        {
            Substr = substr;
            Name = ConstString.ComparatorPath + nameof(SubstringComparator);
        }

        public Pb.Comparator ConvertToPBComparator()
        {
            var internalComparator = new Pb.SubstringComparator
            {
                Substr = Substr
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