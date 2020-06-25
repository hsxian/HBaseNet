using System;

namespace HBaseNet.Utility
{
    public static class DateTimeEx
    {
        public static long ToUnix(this DateTime time)
        {
            return (long) time.ToUniversalTime().Subtract(new DateTime(1970, 1, 1)).TotalSeconds;
        }
    }
}