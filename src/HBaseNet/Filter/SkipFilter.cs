using Google.Protobuf;
using HBaseNet.Const;

namespace HBaseNet.Filter
{
    public class SkipFilter : IFilter
    {
        public IFilter SkippingFilter { get; }
        public string Name { get; }

        public SkipFilter(IFilter skippingFilter)
        {
            SkippingFilter = skippingFilter;
            Name = ConstString.FilterPath + nameof(SkipFilter);
        }

        public Pb.Filter ConvertToPBFilter()
        {
            var internalFilter = new Pb.SkipFilter
            {
                Filter = SkippingFilter.ConvertToPBFilter()
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