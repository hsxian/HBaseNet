using System;
using BitConverter;
using HBaseNet.Utility;

namespace HBaseNet.Metadata.Conventions
{
    public class DateTimeUnix13Converter : IByteArrayConverter
    {
        public object Read(byte[] binary, Type objectType, params object[] parameters)
        {
            if (binary == null) return null;
            return EndianBitConverter.BigEndian.ToUInt64(binary, 0).FromUnixU13();
        }

        public byte[] Write(object value, params object[] parameters)
        {
            if (value is DateTime time)
            {
                return EndianBitConverter.BigEndian.GetBytes(time.ToUnixU13());
            }

            return null;
        }
    }
}
