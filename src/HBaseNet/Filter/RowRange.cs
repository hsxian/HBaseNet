using Google.Protobuf;
using HBaseNet.Const;

namespace HBaseNet.Filter
{
    public class RowRange : IFilter
    {
        public byte[] StartRow { get; }
        public byte[] StopRow { get; }
        public bool StartRowInclusive { get; }
        public bool StopRowInclusive { get; }
        public string Name { get; }

        public RowRange(
            byte[] startRow,
            byte[] stopRow,
            bool startRowInclusive,
            bool stopRowInclusive
        )
        {
            StartRow = startRow;
            StopRow = stopRow;
            StartRowInclusive = startRowInclusive;
            StopRowInclusive = stopRowInclusive;
            Name = ConstString.FilterPath + nameof(RowRange);
        }

        public Pb.RowRange ConvertToPB()
        {
            return new Pb.RowRange
            {
                StartRow = ByteString.CopyFrom(StartRow),
                StopRow = ByteString.CopyFrom(StopRow),
                StartRowInclusive = StartRowInclusive,
                StopRowInclusive = StopRowInclusive
            };
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