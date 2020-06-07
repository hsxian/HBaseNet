using System.Linq;
using Google.Protobuf;
using HBaseNet.Const;

namespace HBaseNet.Filter
{
    public class TimestampsFilter : IFilter
    {
        public long[] Timestamps { get; }
        public string Name { get; }

        public TimestampsFilter(long[] timestamps)
        {
            Timestamps = timestamps;
            Name = ConstString.FilterPath + nameof(TimestampsFilter);
        }

        public Pb.Filter ConvertToPBFilter()
        {
            var internalFilter = new Pb.TimestampsFilter();
            if (Timestamps?.Any() == true)
            {
                internalFilter.Timestamps.AddRange(Timestamps);
            }

            var filter = new Pb.Filter
            {
                Name = Name,
                SerializedFilter = internalFilter.ToByteString()
            };
            return filter;
        }
    }
}