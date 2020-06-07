using System.Linq;
using Google.Protobuf;
using HBaseNet.Const;

namespace HBaseNet.Filter
{
    public class MultiRowRangeFilter : IFilter
    {
        public RowRange[] RowRangeList { get; }
        public string Name { get; }

        public MultiRowRangeFilter(RowRange[] rowRangeList)
        {
            RowRangeList = rowRangeList;
            Name = ConstString.FilterPath + nameof(MultiRowRangeFilter);
        }

        public Pb.Filter ConvertToPBFilter()
        {
            var internalFilter = new Pb.MultiRowRangeFilter();
            if (RowRangeList?.Any() == true)
            {
                internalFilter.RowRangeList.AddRange(RowRangeList.Select(t => t.ConvertToPB()).ToArray());
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