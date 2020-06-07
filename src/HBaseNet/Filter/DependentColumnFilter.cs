using Google.Protobuf;
using HBaseNet.Const;

namespace HBaseNet.Filter
{
    public class DependentColumnFilter : IFilter
    {
        public CompareFilter CompareFilter { get; }
        public string Name { get; }
        public CompareFilter CompareFilterObj { get; }
        public byte[] ColumnFamily { get; }
        public byte[] ColumnQualifier { get; }
        public bool DropDependentColumn { get; }

        public DependentColumnFilter(
            CompareFilter compareFilter,
            byte[] columnFamily,
            byte[] columnQualifier,
            bool dropDependentColumn
        )
        {
            CompareFilter = compareFilter;
            ColumnFamily = columnFamily;
            ColumnQualifier = columnQualifier;
            DropDependentColumn = dropDependentColumn;
            Name = ConstString.FilterPath + nameof(DependentColumnFilter);
        }

        public Pb.Filter ConvertToPBFilter()
        {
            var internalFilter = new Pb.DependentColumnFilter
            {
                CompareFilter = CompareFilter.ConvertToPB(),
                ColumnFamily = ByteString.CopyFrom(ColumnFamily),
                ColumnQualifier = ByteString.CopyFrom(ColumnQualifier),
                DropDependentColumn = DropDependentColumn
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