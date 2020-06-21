using System;
using System.Threading;
using System.Threading.Tasks;

namespace HBaseNet.Utility
{
    public static class TaskEx
    {
        public static async Task<TimeSpan> SleepAndIncreaseBackoff(TimeSpan backoff, TimeSpan increase,
            CancellationToken token)
        {
            await Task.Delay(backoff, token);
            return backoff < increase ? backoff * 2 : backoff + increase;
        }

        public static async Task WaitOn(Func<bool> condition, int millisecondsCheck = 50,
            int millisecondsTimeout = 3000)
        {
            var oldTime = DateTime.Now;
            while (condition() && (DateTime.Now - oldTime).TotalMilliseconds < millisecondsTimeout)
            {
                await Task.Delay(millisecondsCheck);
            }
        }
    }
}