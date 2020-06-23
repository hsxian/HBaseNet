using System.Threading;
using System.Threading.Tasks;
using HBaseNet.HRpc;
using HBaseNet.Region;
using HBaseNet.Zk;
using Microsoft.Extensions.Logging;
using Pb;

namespace HBaseNet
{
    public class AdminClient : CommonClient, IAdminClient
    {
        private RegionClient _adminClient;

        public AdminClient(string zkQuorum)
        {
            ZkQuorum = zkQuorum;
        }

        public async Task<IAdminClient> Build(CancellationToken? token = null)
        {
            token ??= DefaultCancellationSource.Token;
            await LocateMasterClient(token.Value);
            return this;
        }

        private async Task LocateMasterClient(CancellationToken token)
        {
            if (_adminClient != null) return;
            var master = await TryLocateResource(ZkHelper.HBaseMaster, Master.Parser.ParseFrom,
                token);

            _adminClient = await new RegionClient(master.Master_.HostName, (ushort) master.Master_.Port,
                    RegionType.MasterService)
                .Build(RetryCount, token);
            if (_adminClient != null)
                _logger.LogInformation($"Locate master server at : {_adminClient.Host}:{_adminClient.Port}");
        }

        public async Task<bool> CreateTable(CreateTableCall t, CancellationToken? token = null)
        {
            token ??= DefaultCancellationSource.Token;
            await _adminClient.QueueRPC(t);
            var res = await _adminClient.GetRPCResult(t.CallId);
            return res.Error == null && res.Msg is CreateNamespaceResponse;
        }

        public async Task<bool> DeleteTable(DeleteTableCall t, CancellationToken? token = null)
        {
            token ??= DefaultCancellationSource.Token;
            await _adminClient.QueueRPC(t);
            var res = await _adminClient.GetRPCResult(t.CallId);
            return res.Error == null && res.Msg is DeleteTableResponse;
        }

        public async Task<bool> EnableTable(EnableTableCall t, CancellationToken? token = null)
        {
            token ??= DefaultCancellationSource.Token;
            await _adminClient.QueueRPC(t);
            var res = await _adminClient.GetRPCResult(t.CallId);
            return res.Error == null && res.Msg is EnableTableResponse;
        }

        public async Task<bool> DisableTable(DisableTableCall t, CancellationToken? token = null)
        {
            token ??= DefaultCancellationSource.Token;
            await _adminClient.QueueRPC(t);
            var res = await _adminClient.GetRPCResult(t.CallId);
            return res.Error == null && res.Msg is DisableTableResponse;
        }

        public void Dispose()
        {
            _adminClient?.Dispose();
            DefaultCancellationSource.Cancel();
        }
    }
}