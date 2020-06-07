using Google.Protobuf;
using HBaseNet.Const;

namespace HBaseNet.Filter
{
    public class PrefixFilter : IFilter
    {
        public byte[] Prefix { get; }
        public string Name { get; }

        public PrefixFilter(byte[] prefix)
        {
            Prefix = prefix;
            Name = ConstString.FilterPath + nameof(PrefixFilter);
        }

        public Pb.Filter ConvertToPBFilter()
        {
            var internalFilter = new Pb.PrefixFilter
            {
                Prefix = ByteString.CopyFrom(Prefix)
            };
            var filter = new Pb.Filter()
            {
                Name = Name,
                SerializedFilter = internalFilter.ToByteString()
            };
            return filter;
        }
    }
}