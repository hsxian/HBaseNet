using Google.Protobuf;
using HBaseNet.Const;

namespace HBaseNet.Filter
{
    public class FirstKeyOnlyFilter : IFilter
    {
        public string Name { get; }

        public FirstKeyOnlyFilter()
        {
            Name = ConstString.FilterPath + nameof(FirstKeyOnlyFilter);
        }

        public Pb.Filter ConvertToPBFilter()
        {
            var internalFilter = new Pb.FirstKeyOnlyFilter();
            var filter = new Pb.Filter
            {
                Name = Name,
                SerializedFilter = internalFilter.ToByteString()
            };
            return filter;
        }
    }
}