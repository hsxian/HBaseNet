using Google.Protobuf;
using HBaseNet.Const;

namespace HBaseNet.Filter
{
    public class FamilyFilter : IFilter
    {
        public string Name { get; }
        public CompareFilter CompareFilterObj { get; }

        public FamilyFilter(CompareFilter compareFilterObj)
        {
            CompareFilterObj = compareFilterObj;
            Name = ConstString.FilterPath + nameof(FamilyFilter);
        }

        public Pb.Filter ConvertToPBFilter()
        {
            var internalFilter = new Pb.FamilyFilter
            {
                CompareFilter = CompareFilterObj.ConvertToPB()
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