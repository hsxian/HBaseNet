using System;
using HBaseNet.Const;
using HBaseNet.Utility;

namespace HBaseNet.Metadata.Annotations
{
    public class HBasePropertyAttribute : Attribute
    {
        public bool IsIgnore { get; set; }
        public string Family { get; set; }
        public string Qualifier { get; set; }
        public HBasePropertyAttribute(string qualifier)
        {
            Family = ConstString.DefaultFamily;
            Qualifier = qualifier;
        }
    }
}