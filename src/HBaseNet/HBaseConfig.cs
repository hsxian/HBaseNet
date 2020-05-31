using System;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace HBaseNet
{
    public class HBaseConfig
    {
        private static readonly Lazy<HBaseConfig> _lazy = new Lazy<HBaseConfig>(() => new HBaseConfig());
        public static HBaseConfig Instance => _lazy.Value;

        public HBaseConfig()
        {
            ServiceProvider = new ServiceCollection()
                .AddLogging()
                .BuildServiceProvider();
        }

        public IServiceProvider ServiceProvider { get; set; }
        private ILoggerFactory _loggerFactory;

        public ILoggerFactory LoggerFactory
        {
            get => _loggerFactory ?? (ILoggerFactory) ServiceProvider?.GetService(typeof(ILoggerFactory));
            set => _loggerFactory = value ?? throw new ArgumentNullException(nameof(LoggerFactory));
        }
    }
}