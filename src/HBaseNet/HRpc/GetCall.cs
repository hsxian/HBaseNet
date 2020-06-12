using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using Google.Protobuf;
using Google.Protobuf.Collections;
using Google.Protobuf.WellKnownTypes;
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

        public GetCall(string table, string key)
        {
            Table = table.ToUtf8Bytes();
            Key = key.ToUtf8Bytes();
        }

        public GetCall(byte[] table, byte[] key)
        {
            Table = table;
            Key = key;
        }

        public static GetCall CreateGetBefore(byte[] table, byte[] key)
        {
            return new GetCall(table.ToUtf8String(), key.ToUtf8String())
            {
                IsClosestBefore = true
            };
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
                    Filter = Filters?.ConvertToPBFilter()
                }
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