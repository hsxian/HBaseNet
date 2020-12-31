using System;
using Google.Protobuf;
using System.Reflection;

namespace HBaseNet.Metadata.Conventions
{
    public class ConvertItem
    {
        public byte[] Family { get; set; }
        public string Family2 { get; set; }
        public byte[] Qualifier { get; set; }
        public string Qualifier2 { get; set; }
        public PropertyInfo Property { get; set; }
        public Converter<ByteString, object> ReadConvert { get; set; }
        public Converter<object, byte[]> WriteConvert { get; set; }
    }
}