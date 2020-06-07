using Google.Protobuf;
using HBaseNet.Const;

namespace HBaseNet.Filter
{
    public class PageFilter : IFilter
    {
        public long PageSize { get; }
        public string Name { get; }

        public PageFilter(long pageSize)
        {
            PageSize = pageSize;
            Name = ConstString.FilterPath + nameof(PageFilter);
        }

        public Pb.Filter ConvertToPBFilter()
        {
            var internalFilter = new Pb.PageFilter {PageSize = PageSize};
            var filter = new Pb.Filter
            {
                Name = Name,
                SerializedFilter = internalFilter.ToByteString()
            };
            return filter;
        }
    }
}