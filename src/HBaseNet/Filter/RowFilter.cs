using Google.Protobuf;
using HBaseNet.Const;

namespace HBaseNet.Filter
{
    public class RowFilter : IFilter
    {
        public CompareFilter CompareFilterObj { get; }
        public string Name { get; }

        public RowFilter(CompareFilter compareFilter)
        {
            CompareFilterObj = compareFilter;
            Name = ConstString.FilterPath + nameof(RowFilter);
        }

        public Pb.Filter ConvertToPBFilter()
        {
            var internalFilter = new Pb.RowFilter {CompareFilter = CompareFilterObj.ConvertToPB()};
            var filter = new Pb.Filter
            {
                Name = Name,
                SerializedFilter = internalFilter.ToByteString()
            };
            return filter;
        }
    }
}