using Google.Protobuf;
using HBaseNet.Const;

namespace HBaseNet.Filter
{
    public class ColumnRangeFilter : IFilter
    {
        public string Name { get; }
        public byte[] MinColumn { get; }
        public bool MinColumnInclusive { get; }
        public byte[] MaxColumn { get; }
        public bool MaxColumnInclusive { get; }

        public ColumnRangeFilter(byte[] minColumn, byte[] maxColumn, bool minColumnInclusive, bool maxColumnInclusive)
        {
            MinColumn = minColumn;
            MaxColumn = maxColumn;
            MinColumnInclusive = minColumnInclusive;
            MaxColumnInclusive = maxColumnInclusive;
            Name = ConstString.FilterPath + nameof(ColumnRangeFilter);
        }

        public Pb.Filter ConvertToPBFilter()
        {
            var internalFilter = new Pb.ColumnRangeFilter
            {
                MinColumn = ByteString.CopyFrom(MinColumn),
                MaxColumn = ByteString.CopyFrom(MaxColumn),
                MinColumnInclusive = MinColumnInclusive,
                MaxColumnInclusive = MaxColumnInclusive
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