using Google.Protobuf;
using HBaseNet.Const;

namespace HBaseNet.Filter
{
    public class ColumnCountGetFilter : IFilter
    {
        public string Name { get; }
        public int Limit { get; }

        public ColumnCountGetFilter(int limit)
        {
            Name = ConstString.FilterPath + nameof(ColumnCountGetFilter);
            Limit = limit;
        }

        public Pb.Filter ConvertToPBFilter()
        {
            var internalFilter = new Pb.ColumnCountGetFilter
            {
                Limit = Limit,
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