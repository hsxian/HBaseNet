using System.Collections.Generic;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;

namespace HBaseNet.Utility
{
    public static class ProtoBufEx
    {
        private static void Adds<T>(this List<T> l, params T[] v)
        {
            l.AddRange(v);
        }

        public static byte[] EncodeVarint(ulong v)
        {
            return new UInt64Value {Value = v}.ToByteArray()[1..];
        }

        /// <summary>
        /// returning the integer value and the length of the varint.
        /// </summary>
        /// <param name="buf"></param>
        /// <returns></returns>
        public static (ulong value, int length) DecodeVarint(byte[] buf)
        {
            using var ms = new CodedInputStream(buf);
            var v = ms.ReadUInt64();
            var nb = CodedOutputStream.ComputeUInt64Size(v);
            return (v, nb);
        }
    }
}