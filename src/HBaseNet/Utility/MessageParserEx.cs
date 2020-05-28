using System;
using System.Diagnostics;
using Google.Protobuf;

namespace HBaseNet.Utility
{
    public static class MessageParserEx
    {
        public static T TryParseTo<T>(this byte[] arr, Func<byte[], T> parser) where T : IMessage<T>
        {
            var result = default(T);
            try
            {
                result = parser(arr);
            }
            catch (Exception e)
            {
                Debug.WriteLine(e);
            }

            return result;
        }
    }
}