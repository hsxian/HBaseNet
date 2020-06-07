using Google.Protobuf;
using HBaseNet.Const;

namespace HBaseNet.Comparator
{
    public class RegexStringComparator : IComparator
    {
        public string Name { get; }
        public string Pattern { get; }
        public int PatternFlags { get; }
        public string Charset { get; }
        public string Engine { get; }
        public Pb.ByteArrayComparable Comparable { get; }

        public RegexStringComparator(
            string pattern,
            int patternFlags,
            string charset,
            string engine
        )
        {
            Pattern = pattern;
            PatternFlags = patternFlags;
            Charset = charset;
            Engine = engine;
            Name = ConstString.ComparatorPath + nameof(RegexStringComparator);
        }

        public Pb.Comparator ConvertToPBComparator()
        {
            var internalComparator = new Pb.RegexStringComparator
            {
                Pattern = Pattern,
                PatternFlags = PatternFlags,
                Charset = Charset,
                Engine = Engine
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