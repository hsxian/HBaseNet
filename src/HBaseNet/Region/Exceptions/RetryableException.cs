using System;

namespace HBaseNet.Region.Exceptions
{
    public class RetryableException : Exception
    {
        public RetryableException(string message) : base(message)
        {
        }
    }
}