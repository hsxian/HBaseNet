using System.Buffers.Binary;
using System;
using System.Linq;
using System.Text;
using System.Collections.Generic;

namespace HBaseNet.Utility
{
    public static class BinaryEx
    {
        public static byte[] Initialize(this byte[] arr, byte value)
        {
            for (var i = 0; i < arr.Length; i++)
            {
                arr[i] = value;
            }

            return arr;
        }

        public static byte[] ConcatInOrder(params byte[][] item)
        {
            var len = item.Sum(t => t.Length);
            var result = new byte[len];
            var pos = 0;
            foreach (var arr in item)
            {
                arr.CopyTo(result, pos);
                pos += arr.Length;
            }

            return result;
        }

        public static byte[] ToUtf8Bytes(this string str)
        {
            return Encoding.UTF8.GetBytes(str);
        }

        public static string ToUtf8String(this byte[] arr)
        {
            return Encoding.UTF8.GetString(arr);
        }

        public static int GetBinaryHashCode(this IEnumerable<byte> arr)
        {
            return arr?.Any() != true ? 0 : arr.Aggregate(17, (current, @by) => current << 31 + @by);
        }
    }
}