using Google.Protobuf;
using HBaseNet.Const;

namespace HBaseNet.Filter
{
    public class QualifierFilter : IFilter
    {
        public CompareFilter CompareFilterObj { get; }
        public string Name { get; }

        public QualifierFilter(CompareFilter compareFilter)
        {
            CompareFilterObj = compareFilter;
            Name = ConstString.FilterPath + nameof(QualifierFilter);
        }

        public Pb.Filter ConvertToPBFilter()
        {
            var internalFilter = new Pb.QualifierFilter
            {
                CompareFilter = CompareFilterObj.ConvertToPB()
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