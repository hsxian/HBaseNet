using Google.Protobuf;
using HBaseNet.Const;

namespace HBaseNet.Filter
{
    public class ColumnPrefixFilter : IFilter
    {
        public string Name { get; }
        public byte[] Prefix { get; }

        public ColumnPrefixFilter(byte[] prefix)
        {
            Prefix = prefix;
            Name = ConstString.FilterPath + nameof(ColumnPrefixFilter);
        }

        public Pb.Filter ConvertToPBFilter()
        {
            var internalFilter = new Pb.ColumnPrefixFilter
            {
                Prefix = ByteString.CopyFrom(Prefix)
            };
            var filter = new Pb.Filter
            {
                Name = Name,
                SerializedFilter = internalFilter.ToByteString()
            };
            return filter;
        }
    }
}