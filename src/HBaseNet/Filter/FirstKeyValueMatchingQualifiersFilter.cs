using System.Linq;
using Google.Protobuf;
using HBaseNet.Const;

namespace HBaseNet.Filter
{
    public class FirstKeyValueMatchingQualifiersFilter : IFilter
    {
        public byte[][] Qualifiers { get; }
        public string Name { get; }

        public FirstKeyValueMatchingQualifiersFilter(byte[][] qualifiers)
        {
            Qualifiers = qualifiers;
            Name = ConstString.FilterPath + nameof(FirstKeyValueMatchingQualifiersFilter);
        }

        public Pb.Filter ConvertToPBFilter()
        {
            var internalFilter = new Pb.FirstKeyValueMatchingQualifiersFilter();
            if (Qualifiers?.Any() == true)
            {
                internalFilter.Qualifiers.AddRange(Qualifiers.Select(ByteString.CopyFrom).ToArray());
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