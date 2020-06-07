using System.Linq;
using Google.Protobuf;
using HBaseNet.Const;

namespace HBaseNet.Filter
{
    public class FuzzyRowFilter : IFilter
    {
        public string Name { get; }
        public BytesBytesPair[] FuzzyKeysData { get; }

        public FuzzyRowFilter(BytesBytesPair[] fuzzyKeysData)
        {
            FuzzyKeysData = fuzzyKeysData;
            Name = ConstString.FilterPath + nameof(FuzzyRowFilter);
        }

        public Pb.Filter ConvertToPBFilter()
        {
            var internalFilter = new Pb.FuzzyRowFilter();
            if (FuzzyKeysData?.Any() == true)
            {
                internalFilter.FuzzyKeysData.AddRange(FuzzyKeysData
                    .Select(t => t.ConvertToPB())
                    .ToArray());
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