using System;
using HBaseNet.Const;
using HBaseNet.Utility;

namespace HBaseNet.Metadata.Annotations
{
    public class HBasePropertyAttribute : Attribute
    {
        public bool IsIgnore { get; set; }
        public byte[] Family { get; set; }
        public byte[] Qualifier { get; set; }
        public HBasePropertyAttribute(byte[] family, byte[] qualifier)
        {
            Family = family ?? ConstByte.DefaultFamily;
            Qualifier = qualifier;
        }
        public HBasePropertyAttribute(byte[] qualifier) : this(null, qualifier) { }
        public HBasePropertyAttribute(string family = null, string qualifier = null) : this(family?.ToUtf8Bytes(), qualifier?.ToUtf8Bytes()) { }
    }
}