using System.Collections.Generic;
using System.Linq;
using Google.Protobuf;
using HBaseNet.Const;

namespace HBaseNet.Filter
{
    public class FilterList : IFilter
    {
        public Pb.FilterList.Types.Operator Operator { get; }
        public List<IFilter> Filters { get; }
        public string Name { get; }

        public FilterList(Pb.FilterList.Types.Operator @operator, params IFilter[] filters)
        {
            Operator = @operator;
            Filters = filters.ToList();
            Name = ConstString.FilterPath + nameof(FilterList);
        }

        public void AddFilters(params IFilter[] filters)
        {
            Filters.AddRange(filters);
        }

        public Pb.Filter ConvertToPBFilter()
        {
            var filterList = new Pb.FilterList
            {
                Operator = Operator,
            };
            if (Filters?.Any() == true)
            {
                var filterArray = Filters.Select(t => t.ConvertToPBFilter()).ToArray();
                filterList.Filters.AddRange(filterArray);
            }

            var filter = new Pb.Filter
            {
                Name = Name,
                SerializedFilter = filterList.ToByteString()
            };
            return filter;
        }
    }
}