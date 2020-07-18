using System;
using Google.Protobuf;
using HBaseNet.Utility;
using Pb;
using static Pb.MutationProto.Types;
using BinaryComparator = HBaseNet.Comparator.BinaryComparator;
using ByteArrayComparable = HBaseNet.Comparator.ByteArrayComparable;

namespace HBaseNet.HRpc
{
    public class CheckAndPutCall : MutateCall
    {
        public string Family { get; }
        public string Qualifier { get; }
        public byte[] ExpectedValue { get; }

        public CheckAndPutCall(MutateCall put, string family, string qualifier, byte[] expectedValue)
            : base(put.Table, put.Key, put.Values)
        {
            MutationType = MutationType.Put;
            Key = put.Key;
            Table = put.Table;
            Family = family;
            Qualifier = qualifier;
            ExpectedValue = expectedValue;
        }

        public override byte[] Serialize()
        {
            var bac = new ByteArrayComparable(ExpectedValue);
            var cmp = new BinaryComparator(bac.ConvertToPB());
            var comparator = cmp.ConvertToPBComparator();
            var mutateRequest = SerializeToProto();
            mutateRequest.Condition = new Condition
            {
                Row = ByteString.CopyFrom(Key),
                Family = ByteString.CopyFromUtf8(Family),
                Qualifier = ByteString.CopyFromUtf8(Qualifier),
                CompareType = Pb.CompareType.Equal,
                Comparator = comparator
            };
            return mutateRequest.ToByteArray();
        }

        public override IMessage ParseResponseFrom(byte[] bts)
        {
            return bts.TryParseTo(MutateResponse.Parser.ParseFrom);
        }
    }
}