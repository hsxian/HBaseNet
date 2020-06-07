using Google.Protobuf;
using HBaseNet.Const;

namespace HBaseNet.Filter
{
    public class WhileMatchFilter : IFilter
    {
        public IFilter MatchingFilter { get; }
        public string Name { get; }

        public WhileMatchFilter(IFilter matchingFilter)
        {
            MatchingFilter = matchingFilter;
            Name = ConstString.FilterPath + nameof(WhileMatchFilter);
        }

        public Pb.Filter ConvertToPBFilter()
        {
            var internalFilter = new Pb.WhileMatchFilter
            {
                Filter = MatchingFilter.ConvertToPBFilter()
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