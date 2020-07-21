using System;
using System.Collections.Generic;
using System.Linq;
using BitConverter;
using Google.Protobuf;
using HBaseNet.Utility;
using Pb;

namespace HBaseNet.HRpc
{
    public class MutateCall : BaseCall
    {
        public IDictionary<string, IDictionary<string, byte[]>> Values { get; }
        public DateTime? Timestamp { get; set; }
        public MutationProto.Types.Durability Durability { get; set; } = MutationProto.Types.Durability.UseDefault;
        public MutationProto.Types.MutationType MutationType { get; set; }

        public MutateCall(string table, string key, IDictionary<string, IDictionary<string, byte[]>> values)
            : this(table.ToUtf8Bytes(), key.ToUtf8Bytes(), values)
        {
        }
        public MutateCall(byte[] table, byte[] key, IDictionary<string, IDictionary<string, byte[]>> values)
        {
            Table = table;
            Key = key;
            Values = values;
        }
        public MutateCall(byte[] table, byte[] key, string family, string qualifier, long increment)
        {
            Table = table;
            Key = key;
            Values = new Dictionary<string, IDictionary<string, byte[]>>
            {
                {family,  new Dictionary<string ,byte[]> {{qualifier,EndianBitConverter.BigEndian.GetBytes(increment)}}}
            };
        }
        public MutateCall(string table, string key, string family, string qualifier, long increment)
            : this(table.ToUtf8Bytes(), key.ToUtf8Bytes(), family, qualifier, increment)
        {
        }
        public MutateCall(byte[] table, byte[] key, string family, string qualifier, string append)
        {
            Table = table;
            Key = key;
            Values = new Dictionary<string, IDictionary<string, byte[]>>
            {
                {family,  new Dictionary<string ,byte[]> {{qualifier,append.ToUtf8Bytes()}}}
            };
        }
        public MutateCall(string table, string key, string family, string qualifier, string append)
            : this(table.ToUtf8Bytes(), key.ToUtf8Bytes(), family, qualifier, append)
        {
        }

        public override string Name => "Mutate";

        public MutateRequest SerializeToProto()
        {
            var result = new MutateRequest
            {
                Region = GetRegionSpecifier(),
                Mutation = new MutationProto
                {
                    Row = ByteString.CopyFrom(Key),
                    MutateType = MutationType,
                    Durability = Durability
                }
            };

            if (Timestamp != null)
            {
                result.Mutation.Timestamp = Timestamp.Value.ToUnixU13();
            }

            if (Values?.Any() != true) return result;

            var columns = Values
                .Select(t =>
                {
                    var clo = new MutationProto.Types.ColumnValue
                    {
                        Family = ByteString.CopyFromUtf8(t.Key),
                    };
                    if (t.Value?.Any() != true) return clo;
                    var quals = t.Value.Select(tt =>
                        {
                            var quail = new MutationProto.Types.ColumnValue.Types.QualifierValue
                            {
                                Qualifier = ByteString.CopyFromUtf8(tt.Key),
                                Value = ByteString.CopyFrom(tt.Value)
                            };
                            if (MutationType == MutationProto.Types.MutationType.Delete)
                            {
                                quail.DeleteType = MutationProto.Types.DeleteType.DeleteMultipleVersions;
                            }

                            return quail;
                        })
                        .ToArray();
                    clo.QualifierValue.AddRange(quals);

                    return clo;
                }).ToArray();

            result.Mutation.ColumnValue.AddRange(columns);
            return result;
        }

        public override byte[] Serialize()
        {
            return SerializeToProto().ToByteArray();
        }

        public override IMessage ParseResponseFrom(byte[] bts)
        {
            return bts.TryParseTo(MutateResponse.Parser.ParseFrom);
        }
    }
}