using Google.Protobuf;
using HBaseNet.Const;

namespace HBaseNet.Filter
{
    public class ValueFilter : IFilter
    {
        public CompareFilter CompareFilterObj { get; }
        public string Name { get; }

        public ValueFilter(CompareFilter compareFilter)
        {
            CompareFilterObj = compareFilter;
            Name = ConstString.FilterPath + nameof(ValueFilter);
        }

        public Pb.Filter ConvertToPBFilter()
        {
            var internalFilter = new Pb.ValueFilter
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