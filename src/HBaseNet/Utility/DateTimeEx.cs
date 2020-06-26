using System;

namespace HBaseNet.Utility
{
    public static class DateTimeEx
    {
        private static readonly DateTime UnixStart = new DateTime(1970, 1, 1);

        /// <summary>
        /// 13-bit Unix timestamp(ulong)
        /// </summary>
        /// <param name="time"></param>
        /// <returns></returns>
        public static ulong ToUnixU13(this DateTime time)
        {
            return (ulong) (time.ToUniversalTime() - UnixStart).TotalMilliseconds;
        }

        public static DateTime FromUnixU13(this ulong stamp)
        {
            return UnixStart.ToLocalTime() + TimeSpan.FromMilliseconds(stamp);
        }
    }
}