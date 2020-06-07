using Google.Protobuf;
using HBaseNet.Const;

namespace HBaseNet.Filter
{
    public class FilterWrapper : IFilter
    {
        public IFilter WrappedFilter { get; }
        public string Name { get; }

        public FilterWrapper(IFilter wrappedFilter)
        {
            WrappedFilter = wrappedFilter;
            Name = ConstString.FilterPath + nameof(FilterWrapper);
        }

        public Pb.Filter ConvertToPBFilter()
        {
            var internalFilter = new Pb.FilterWrapper
            {
                Filter = WrappedFilter.ConvertToPBFilter()
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