using Google.Protobuf;
using HBaseNet.Const;

namespace HBaseNet.Filter
{
    public class RandomRowFilter : IFilter
    {
        public float Chance { get; }
        public string Name { get; }

        public RandomRowFilter(float chance)
        {
            Chance = chance;
            Name = ConstString.FilterPath + nameof(RandomRowFilter);
        }

        public Pb.Filter ConvertToPBFilter()
        {
            var internalFilter = new Pb.RandomRowFilter {Chance = Chance};
            var filter = new Pb.Filter
            {
                Name = Name,
                SerializedFilter = internalFilter.ToByteString()
            };
            return filter;
        }
    }
}