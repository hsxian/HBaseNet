using Google.Protobuf;
using HBaseNet.Comparator;
using HBaseNet.Const;
using Pb;

namespace HBaseNet.Filter
{
    public class CompareFilter : IFilter
    {
        public string Name { get; }
        public CompareType CompareOp { get; }
        public IComparator ComparatorObj { get; }

        public CompareFilter(CompareType compareOp, IComparator comparatorObj)
        {
            CompareOp = compareOp;
            ComparatorObj = comparatorObj;
            Name = ConstString.FilterPath + nameof(CompareFilter);
        }

        public Pb.CompareFilter ConvertToPB()
        {
            var internalFilter = new Pb.CompareFilter
            {
                CompareOp = CompareOp,
                Comparator = ComparatorObj.ConvertToPBComparator()
            };
            return internalFilter;
        }

        public Pb.Filter ConvertToPBFilter()
        {
            var filter = new Pb.Filter
            {
                Name = Name,
                SerializedFilter = ConvertToPB().ToByteString()
            };
            return filter;
        }
    }
}