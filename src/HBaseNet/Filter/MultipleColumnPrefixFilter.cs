using System.Linq;
using Google.Protobuf;
using HBaseNet.Const;

namespace HBaseNet.Filter
{
    public class MultipleColumnPrefixFilter : IFilter
    {
        public byte[][] SortedPrefixes { get; }
        public string Name { get; }

        public MultipleColumnPrefixFilter(byte[][] sortedPrefixes)
        {
            SortedPrefixes = sortedPrefixes;
            Name = ConstString.FilterPath + nameof(MultipleColumnPrefixFilter);
        }

        public Pb.Filter ConvertToPBFilter()
        {
            var internalFilter = new Pb.MultipleColumnPrefixFilter();
            if (SortedPrefixes?.Any() == true)
            {
                internalFilter.SortedPrefixes.AddRange(SortedPrefixes.Select(ByteString.CopyFrom).ToArray());
            }

            var filter = new Pb.Filter
            {
                Name = Name,
                SerializedFilter = internalFilter.ToByteString()
            };
            return filter;
        }
    }
}