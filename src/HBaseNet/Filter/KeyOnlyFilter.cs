using Google.Protobuf;
using HBaseNet.Const;

namespace HBaseNet.Filter
{
    public class KeyOnlyFilter : IFilter
    {
        public bool LenAsVal { get; }
        public string Name { get; }

        public KeyOnlyFilter(bool lenAsVal)
        {
            LenAsVal = lenAsVal;
            Name = ConstString.FilterPath + nameof(KeyOnlyFilter);
        }

        public Pb.Filter ConvertToPBFilter()
        {
            var internalFilter = new Pb.KeyOnlyFilter {LenAsVal = LenAsVal};
            var filter = new Pb.Filter
            {
                Name = Name,
                SerializedFilter = internalFilter.ToByteString()
            };
            return filter;
        }
    }
}