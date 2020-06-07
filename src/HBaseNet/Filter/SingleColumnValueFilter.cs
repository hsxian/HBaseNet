using Google.Protobuf;
using HBaseNet.Comparator;
using HBaseNet.Const;
using Pb;

namespace HBaseNet.Filter
{
    public class SingleColumnValueFilter : IFilter
    {
        public byte[] ColumnFamily { get; }
        public byte[] ColumnQualifier { get; }
        public CompareType CompareOp { get; }
        public IComparator ComparatorObj { get; }
        public bool FilterIfMissing { get; }
        public bool LatestVersionOnly { get; }
        public string Name { get; }

        public SingleColumnValueFilter(
            byte[] columnFamily,
            byte[] columnQualifier,
            CompareType compareOp,
            IComparator comparatorObj,
            bool filterIfMissing,
            bool latestVersionOnly
        )
        {
            ColumnFamily = columnFamily;
            ColumnQualifier = columnQualifier;
            CompareOp = compareOp;
            ComparatorObj = comparatorObj;
            FilterIfMissing = filterIfMissing;
            LatestVersionOnly = latestVersionOnly;
            Name = ConstString.FilterPath + nameof(SingleColumnValueFilter);
        }

        public Pb.SingleColumnValueFilter ConvertToPB()
        {
            return new Pb.SingleColumnValueFilter
            {
                ColumnFamily = ByteString.CopyFrom(ColumnFamily),
                ColumnQualifier = ByteString.CopyFrom(ColumnQualifier),
                CompareOp = CompareOp,
                Comparator = ComparatorObj.ConvertToPBComparator(),
                FilterIfMissing = FilterIfMissing,
                LatestVersionOnly = LatestVersionOnly
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