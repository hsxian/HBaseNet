using System;
using System.Collections.Generic;

namespace HBaseNet.Region.Exceptions
{
    public class ExceptionMap
    {
        public static bool IsMatch<T>(string javaCallName) where T : Exception
        {
            return JavaExceptionMap.TryGetValue(javaCallName, out var type) && type == typeof(T);
        }

        public static Dictionary<string, Type> JavaExceptionMap { get; } = new Dictionary<string, Type>
        {
            {"org.apache.hadoop.hbase.exceptions.FailedSanityCheckException", typeof(RetryableException)},
            {"org.apache.hadoop.hbase.NotServingRegionException", typeof(RetryableException)},
            {"org.apache.hadoop.hbase.exceptions.RegionMovedException", typeof(RetryableException)},
            {"org.apache.hadoop.hbase.exceptions.RegionOpeningException", typeof(RetryableException)},
            {"org.apache.hadoop.hbase.CallQueueTooBigException", typeof(CallQueueTooBigException)},
            {"", typeof(Exception)},
        };
    }
}