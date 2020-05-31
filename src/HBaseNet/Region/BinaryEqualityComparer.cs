using System;
using System.Collections.Generic;
using System.Linq;
using HBaseNet.Utility;

namespace HBaseNet.Region
{
    public class BinaryEqualityComparer : IEqualityComparer<byte[]>
    {
        public bool Equals(byte[] x, byte[] y)
        {
            return x.SequenceEqual(y);
        }

        public int GetHashCode(byte[] arr)
        {
            return arr.GetHash();
        }
    }
}