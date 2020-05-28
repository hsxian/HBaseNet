using System;
using Microsoft.Extensions.Logging;

namespace HBaseNet
{
    public class HBaseConfig
    {
        public IServiceProvider ServiceProvider { get; set; }
        private ILoggerFactory _loggerFactory;
        public ILoggerFactory LoggerFactory
        {
            get => _loggerFactory ?? (ILoggerFactory)this.ServiceProvider?.GetService(typeof(ILoggerFactory));
            set => _loggerFactory = value ?? throw new ArgumentNullException(nameof(LoggerFactory));
        }
    }
}