using Google.Protobuf;
using HBaseNet.Const;

namespace HBaseNet.Filter
{
    public class ColumnPaginationFilter : IFilter
    {
        public string Name { get; }
        public int Limit { get; }
        public int Offset { get; }
        public byte[] ColumnOffset { get; }

        public ColumnPaginationFilter(int limit, int offset, byte[] columnOffset)
        {
            Name = ConstString.FilterPath + nameof(ColumnPaginationFilter);
            Limit = limit;
            Offset = offset;
            ColumnOffset = columnOffset;
        }

        public Pb.Filter ConvertToPBFilter()
        {
            var internalFilter = new Pb.ColumnPaginationFilter
            {
                Limit = Limit,
                Offset = Offset,
                ColumnOffset = ByteString.CopyFrom(ColumnOffset)
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