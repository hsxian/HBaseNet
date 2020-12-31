using System;
using System.Collections;
using System.Collections.Generic;
using Google.Protobuf;

namespace HBaseNet.Utility
{
    public class ByteStringComparer : IEqualityComparer<ByteString>
    {
        private static readonly Lazy<ByteStringComparer> _default = new Lazy<ByteStringComparer>(() => new ByteStringComparer());
        public static ByteStringComparer Default => _default.Value;

        public bool Equals(ByteString x, ByteString y)
        {
            return StructuralComparisons.StructuralEqualityComparer.Equals(x, y);
        }

        public int GetHashCode(ByteString obj)
        {
            return StructuralComparisons.StructuralEqualityComparer.GetHashCode(obj);
        }
    }
}