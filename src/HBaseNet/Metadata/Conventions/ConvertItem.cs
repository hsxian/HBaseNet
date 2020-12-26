using System;
using Google.Protobuf;
using System.Reflection;

namespace HBaseNet.Metadata.Conventions
{
    public class ConvertItem<T>
    {
        public PropertyInfo Property { get; set; }

        public Action<T, ByteString> Convert { get; set; }
    }
}