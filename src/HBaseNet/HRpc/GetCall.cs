using System;
using System.Collections.Generic;
using System.Linq;
using Google.Protobuf;
using HBaseNet.Utility;
using Pb;

namespace HBaseNet.HRpc
{
    public class GetCall : BaseCall
    {
        private bool IsClosestBefore { get; set; }

        /// <summary>
        /// family, qualifiers 
        /// </summary>
        public IDictionary<string, string[]> Families { get; set; }

        public Filter.IFilter Filters { get; set; }
        public bool IsExistsOnly { get; set; }
        public TimeRange TimeRange { get; set; }
        public uint MaxVersions { get; set; } = 1;



        public GetCall(byte[] table, byte[] key, bool isClosestBefore = false)
        {
            IsClosestBefore = isClosestBefore;
            Table = table;
            Key = key;
        }

        public GetCall(string table, string key) : this(table.ToUtf8Bytes(), key.ToUtf8Bytes())
        {
        }

        public override string Name => "Get";

        public override byte[] Serialize()
        {
            var get = new GetRequest
            {
                Region = GetRegionSpecifier(),
                Get = new Pb.Get
                {
                    Row = ByteString.CopyFrom(Key),
                    ExistenceOnly = IsExistsOnly,
                    ClosestRowBefore = IsClosestBefore,
                    Filter = Filters?.ConvertToPBFilter(),
                    TimeRange = TimeRange,
                    MaxVersions = MaxVersions,
                },
            };

            if (Families?.Any() == true)
            {
                get.Get.Column.AddRange(ConvertToColumns(Families));
            }

            return get.ToByteArray();
        }


        public override IMessage ParseResponseFrom(byte[] bts)
        {
            return bts.TryParseTo(GetResponse.Parser.ParseFrom);
        }
    }
}