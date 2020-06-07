using Google.Protobuf;
using HBaseNet.Const;

namespace HBaseNet.Filter
{
    public class FilterAllFilter : IFilter
    {
        public string Name { get; }

        public FilterAllFilter()
        {
            Name = ConstString.FilterPath + nameof(FilterAllFilter);
        }

        public Pb.Filter ConvertToPBFilter()
        {
            var internalFilter = new Pb.FilterAllFilter();
            var filter = new Pb.Filter
            {
                Name = Name,
                SerializedFilter = internalFilter.ToByteString()
            };
            return filter;
        }
    }
}