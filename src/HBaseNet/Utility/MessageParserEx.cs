using System;
using Google.Protobuf;
using Microsoft.Extensions.Logging;

namespace HBaseNet.Utility
{
    public static class MessageParserEx
    {
        private static readonly ILogger _logger;

        static MessageParserEx()
        {
            _logger = HBaseConfig.Instance.LoggerFactory.CreateLogger(nameof(MessageParserEx));
        }

        public static T TryParseTo<T>(this byte[] arr, Func<byte[], T> parser) where T : IMessage<T>
        {
            var result = default(T);
            try
            {
                result = parser(arr);
            }
            catch (Exception e)
            {
                _logger.LogError($"TryParseTo {typeof(T)} failed", e);
            }

            return result;
        }
    }
}