using System.Threading.Tasks;
using org.apache.zookeeper;

namespace HBaseNet.Zk
{
    public class ZkLogWatcher : Watcher
    {
        public override async Task process(WatchedEvent @event)
        {
            await Task.CompletedTask;
        }
    }
}