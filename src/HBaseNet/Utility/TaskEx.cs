using System;
using System.Threading.Tasks;

namespace HBaseNet.Utility
{
    public class TaskEx
    {
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