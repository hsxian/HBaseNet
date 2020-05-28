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
        public bool IsClosestBefore { get; set; }

        /// <summary>
        /// family, qualifiers 
        /// </summary>
        public IDictionary<string, string[]> Family { get; protected set; }

        public GetCall(string table, string key, IDictionary<string, string[]> family)
        {
            Table = table.ToUtf8Bytes();
            Key = key.ToUtf8Bytes();
            Family = family;
        }

        public static GetCall CreateGetBefore(byte[] table, byte[] key, IDictionary<string, string[]> family)
        {
            return new GetCall(table.ToUtf8String(), key.ToUtf8String(), family)
            {
                IsClosestBefore = false
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
                    Row = ByteString.CopyFrom(Key)
                }
            };
            if (Family?.Any() == true)
            {
                get.Get.Column.AddRange(ConvertToColumns(Family));
            }

            if (IsClosestBefore)
            {
                get.Get.ClosestRowBefore = true;
            }

            return get.ToByteArray();
        }
        

        public override IMessage ResponseParseFrom(byte[] bts)
        {
            return bts.TryParseTo(GetResponse.Parser.ParseFrom);
        }
    }
}