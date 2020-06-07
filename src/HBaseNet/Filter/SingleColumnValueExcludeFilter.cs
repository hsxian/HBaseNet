using Google.Protobuf;
using HBaseNet.Const;

namespace HBaseNet.Filter
{
    public class SingleColumnValueExcludeFilter : IFilter
    {
        public SingleColumnValueFilter SingleColumnValueFilterObj { get; }
        public string Name { get; }

        public SingleColumnValueExcludeFilter(SingleColumnValueFilter filter)
        {
            SingleColumnValueFilterObj = filter;
            Name = ConstString.FilterPath + nameof(SingleColumnValueExcludeFilter);
        }

        public Pb.Filter ConvertToPBFilter()
        {
            var internalFilter = new Pb.SingleColumnValueExcludeFilter
            {
                SingleColumnValueFilter = SingleColumnValueFilterObj.ConvertToPB()
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