using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using HBaseNet.Const;
using HBaseNet.HRpc;
using HBaseNet.Region;
using HBaseNet.Region.Exceptions;
using HBaseNet.Utility;
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
            var res = await LocateMasterClient(token.Value);
            return res ? this : null;
        }

        private async Task<bool> LocateMasterClient(CancellationToken token)
        {
            if (_adminClient != null) return true;
            var master = await TryLocateResource(ZkRoot + ConstString.Master, Master.Parser.ParseFrom, token);

            if (master == null) return false;

            _adminClient = await new RegionClient(master.Master_.HostName, (ushort)master.Master_.Port, RegionType.MasterService)
            {
                TimeOut = Timeout,
                EffectiveUser = EffectiveUser
            }
                .Build(RetryCount, token);
            if (_adminClient != null)
                _logger.LogInformation($"Locate master server at : {_adminClient.Host}:{_adminClient.Port}");

            return _adminClient != null;
        }

        private async Task<bool> CheckProcedureWithBackoff(ulong procId, CancellationToken token)
        {
            var backoff = BackoffStart;
            var oldTime = DateTime.Now;
            while (DateTime.Now - oldTime < Timeout)
            {
                var req = new GetProcedureStateCall(procId);
                await _adminClient.QueueRPC(req);
                var res = await _adminClient.GetRPCResult(req.CallId);
                if (res == null) return false;

                if (res.Msg is GetProcedureResultResponse rep)
                {
                    switch (rep.State)
                    {
                        case GetProcedureResultResponse.Types.State.NotFound:
                            return false;
                        case GetProcedureResultResponse.Types.State.Finished:
                            return true;
                    }
                }
                else if (res.Error != null)
                {
                    switch (res.Error)
                    {
                        case DoNotRetryIOException _:
                            return false;
                    }
                }
                backoff = await TaskEx.SleepAndIncreaseBackoff(backoff, BackoffIncrease, token);
            }

            return false;
        }
        private async Task<RPCResult> SendRPC(ICall t, CancellationToken? token = null)
        {
            await _adminClient.QueueRPC(t);
            var res = await _adminClient.GetRPCResult(t.CallId);
            return res;
        }
        public async Task<bool> CreateTable(CreateTableCall t, CancellationToken? token = null)
        {
            token ??= DefaultCancellationSource.Token;
            var res = await SendRPC(t, token);
            if (res?.Msg is CreateTableResponse create)
            {
                return await CheckProcedureWithBackoff(create.ProcId, token.Value);
            }

            return false;
        }

        public async Task<bool> DeleteTable(DeleteTableCall t, CancellationToken? token = null)
        {
            token ??= DefaultCancellationSource.Token;
            var res = await SendRPC(t, token);
            if (res?.Msg is DeleteTableResponse del)
            {
                return await CheckProcedureWithBackoff(del.ProcId, token.Value);
            }

            return false;
        }

        public async Task<bool> EnableTable(EnableTableCall t, CancellationToken? token = null)
        {
            token ??= DefaultCancellationSource.Token;
            var res = await SendRPC(t, token);
            if (res?.Msg is EnableTableResponse enb)
            {
                return await CheckProcedureWithBackoff(enb.ProcId, token.Value);
            }

            return false;
        }

        public async Task<bool> DisableTable(DisableTableCall t, CancellationToken? token = null)
        {
            token ??= DefaultCancellationSource.Token;
            var res = await SendRPC(t, token);
            if (res?.Msg is DisableTableResponse dis)
            {
                return await CheckProcedureWithBackoff(dis.ProcId, token.Value);
            }

            return false;
        }

        public async Task<TableName[]> ListTableNames(ListTableNamesCall t, CancellationToken? token = null)
        {
            var res = await SendRPC(t, token);
            if (res?.Msg is GetTableNamesResponse resp)
            {
                return resp.TableNames.ToArray();
            }
            return null;
        }

        public void Dispose()
        {
            _adminClient?.Dispose();
            DefaultCancellationSource.Cancel();
        }
    }
}