using System;
using Microsoft.Extensions.Logging;

namespace HBaseNet.Logging
{
    public class DebugLogger<TCategoryName> : ILogger<TCategoryName>
    {
        public IDisposable BeginScope<TState>(TState state)
        {
            return null;
        }

        public bool IsEnabled(LogLevel logLevel)
        {
            return true;
        }

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception exception, Func<TState, Exception, string> formatter)
        {
            System.Diagnostics.Debug.WriteLine($"[{DateTime.Now} {logLevel}] {formatter(state, exception)}");
        }
    }
}