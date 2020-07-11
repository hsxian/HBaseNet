using System;
using System.Threading;
using System.Threading.Tasks;
using HBaseNet.Utility;
using HBaseNet.Zk;
using Microsoft.Extensions.Logging;

namespace HBaseNet
{
    public abstract class CommonClient
    {
        protected string ZkQuorum { get; set; }
        protected readonly ILogger<CommonClient> _logger;
        private readonly ZkHelper _zkHelper;
        protected TimeSpan BackoffStart { get; set; } = TimeSpan.FromMilliseconds(16);
        protected TimeSpan BackoffIncrease { get; set; } = TimeSpan.FromSeconds(5);
        protected TimeSpan Timeout { get; set; } = TimeSpan.FromSeconds(30);
        protected int RetryCount { get; set; } = 5;
        protected CancellationTokenSource DefaultCancellationSource { get; }
        
        protected CommonClient()
        {
            _logger = HBaseConfig.Instance.LoggerFactory.CreateLogger<CommonClient>();
            _zkHelper = new ZkHelper();
            DefaultCancellationSource = new CancellationTokenSource();
        }

        protected async Task<TResult> TryLocateResource<TResult>(string resource,
            Func<byte[], TResult> getResultFunc, CancellationToken token)
        {
            var zkc = _zkHelper.CreateClient(ZkQuorum, Timeout);
            var backoff = BackoffStart;
            var result = default(TResult);
            for (var i = 0; i < RetryCount && token.IsCancellationRequested == false; i++)
            {
                result = await _zkHelper.LocateResource(zkc, resource, getResultFunc);
                if (result == null)
                {
                    _logger.LogWarning(
                        $"Locate {resource} failed in {i + 1}th，try the locate again after {backoff}.");
                    backoff = await TaskEx.SleepAndIncreaseBackoff(backoff, BackoffIncrease, token);
                }
                else
                {
                    break;
                }
            }

            await zkc.closeAsync();
            if (result == null)
                _logger.LogWarning(
                    $"Locate {resource} failed in {RetryCount}th, please check your network.");
            return result;
        }
    }
}