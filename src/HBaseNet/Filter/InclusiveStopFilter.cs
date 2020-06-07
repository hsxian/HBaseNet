using Google.Protobuf;
using HBaseNet.Const;

namespace HBaseNet.Filter
{
    public class InclusiveStopFilter : IFilter
    {
        public byte[] StopRowKey { get; }
        public string Name { get; }

        public InclusiveStopFilter(byte[] stopRowKey)
        {
            StopRowKey = stopRowKey;
            Name = ConstString.FilterPath + nameof(InclusiveStopFilter);
        }

        public Pb.Filter ConvertToPBFilter()
        {
            var internalFilter = new Pb.InclusiveStopFilter
            {
                StopRowKey = ByteString.CopyFrom(StopRowKey)
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