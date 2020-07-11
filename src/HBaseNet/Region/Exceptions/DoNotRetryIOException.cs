using System;

namespace HBaseNet.Region.Exceptions
{
    public class DoNotRetryIOException : Exception
    {
        public DoNotRetryIOException(string message) : base(message)
        {
        }
    }
}