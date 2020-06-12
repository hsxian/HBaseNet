using System.Collections.Generic;
using System.Linq;
using Google.Protobuf;
using Google.Protobuf.Collections;
using HBaseNet.Const;
using HBaseNet.Utility;
using Pb;

namespace HBaseNet.HRpc
{
    public class MutateCall : BaseCall
    {
        public IDictionary<string, IDictionary<string, byte[]>> Values { get; set; }
        public MutationProto.Types.MutationType MutationType { get; set; }
        
        public MutateCall(string table, string key, IDictionary<string, IDictionary<string, byte[]>> values)
        {
            Table = table.ToUtf8Bytes();
            Key = key.ToUtf8Bytes();
            Values = values;
        }

        public override string Name => "Mutate";

        public override byte[] Serialize()
        {
            var result = new MutateRequest
            {
                Region = GetRegionSpecifier(),
                Mutation = new MutationProto
                {
                    Row = ByteString.CopyFrom(Key),
                    MutateType = MutationType
                }
            };

            if (Values?.Any() != true) return result.ToByteArray();

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

            return result.ToByteArray();
        }

        public override IMessage ParseResponseFrom(byte[] bts)
        {
            return bts.TryParseTo(MutateResponse.Parser.ParseFrom);
        }
    }
}