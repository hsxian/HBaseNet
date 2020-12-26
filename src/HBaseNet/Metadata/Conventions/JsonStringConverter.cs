using System;
using BitConverter;
using HBaseNet.Utility;
using Newtonsoft.Json;

namespace HBaseNet.Metadata.Conventions
{
    public class JsonStringConverter : IByteArrayConverter
    {
        public object Read(byte[] binary, Type objectType, params object[] parameters)
        {
            return binary == null ? null : JsonConvert.DeserializeObject(binary.ToUtf8String(), objectType);
        }

        public byte[] Write(object value, params object[] parameters)
        {
            return value == null ? null : JsonConvert.SerializeObject(value).ToUtf8Bytes();
        }
    }
}