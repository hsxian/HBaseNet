using System;

namespace HBaseNet.Metadata.Conventions
{
    public interface IByteArrayConverter 
    {
         object Read(byte[] binary,Type objectType, params object[]parameters);
         byte[] Write(object value,params object[]parameters);
    }
}