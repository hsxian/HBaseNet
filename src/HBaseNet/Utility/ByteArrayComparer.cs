using System;
using System.Collections;
using System.Collections.Generic;

namespace HBaseNet.Utility
{
    public class ByteArrayComparer: IEqualityComparer<byte[]>
    {
        private static readonly Lazy<ByteArrayComparer> _default = new Lazy<ByteArrayComparer>(() => new ByteArrayComparer());
        public static ByteArrayComparer Default => _default.Value;
        
        public bool Equals(byte[] x, byte[] y)
        {
            return StructuralComparisons.StructuralEqualityComparer.Equals(x, y);
        }

        public int GetHashCode(byte[] obj)
        {
            return StructuralComparisons.StructuralEqualityComparer.GetHashCode(obj);
        }
    }
}