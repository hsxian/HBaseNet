using System;
using System.Collections.Generic;
using System.Linq;
using Google.Protobuf;
using Pb;
using RegionInfo = HBaseNet.Region.RegionInfo;

namespace HBaseNet.HRpc
{
    public abstract class BaseCall : ICall
    {
        public uint CallId { get; set; }
        public uint RetryCount { get; set; }
        public byte[] Table { get; set; }
        public byte[] Key { get; set; }
        public abstract string Name { get; }
        public RegionInfo Info { get; set; }

        public abstract byte[] Serialize();
        public abstract IMessage ParseResponseFrom(byte[] bts);

        protected RegionSpecifier GetRegionSpecifier()
        {
            return new RegionSpecifier
            {
                Type = RegionSpecifier.Types.RegionSpecifierType.RegionName,
                Value = ByteString.CopyFrom(Info.Name)
            };
        }

        protected IEnumerable<Column> ConvertToColumns(IDictionary<string, string[]> families)
        {
            var columns = families
                .Select(t =>
                {
                    var col = new Column
                    {
                        Family = ByteString.CopyFromUtf8(t.Key),
                    };
                    if (t.Value != null)
                    {
                        col.Qualifier.AddRange(t.Value.Select(ByteString.CopyFromUtf8).ToList());
                    }

                    return col;
                })
                .ToArray();
            return columns;
        }
    }
}