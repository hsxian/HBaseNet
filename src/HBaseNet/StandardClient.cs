using System;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using CSharpTest.Net.IO;
using Google.Protobuf;
using HBaseNet.Const;
using HBaseNet.HRpc;
using HBaseNet.Region;
using HBaseNet.Region.Exceptions;
using HBaseNet.Utility;
using Microsoft.Extensions.Logging;
using Pb;
using RegionInfo = HBaseNet.Region.RegionInfo;

namespace HBaseNet
{
    public class StandardClient : CommonClient, IStandardClient
    {
        private readonly byte[] _metaTableName;
        private readonly Dictionary<string, string[]> _infoFamily;
        private readonly RegionInfo _metaRegionInfo;
        private RegionClient _metaClient;
        private readonly RegionCache _cache;
        private ConcurrentQueue<ICall> _loadRegionQueue;
        public int CallQueueSize { get; set; } = 150;
        public async Task<IStandardClient> Build(CancellationToken? token = null)
        {
            token ??= DefaultCancellationSource.Token;
            var res = await LocateMetaClient(token.Value);
            return res ? this : null;
        }

        public StandardClient(string zkQuorum)
        {
            ZkQuorum = zkQuorum;
            _infoFamily = new Dictionary<string, string[]>
            {
                {"info", null}
            };
            _metaTableName = "hbase:meta".ToUtf8Bytes();
            _metaRegionInfo = new RegionInfo(0, "hbase".ToUtf8Bytes(), "meta".ToUtf8Bytes(), "hbase:meta,,1".ToUtf8Bytes(), null);
            _cache = new RegionCache();
            _loadRegionQueue = new ConcurrentQueue<ICall>();
            ProcessResolveRegionTask();
        }

        public async Task<bool> CheckAndPut(MutateCall put, string family, string qualifier,
            byte[] expectedValue, CancellationToken? token = null)
        {
            var cas = new CheckAndPutCall(put, family, qualifier, expectedValue);
            var res = await SendRPCToRegion<MutateResponse>(cas, token);
            return res != null && res.Processed;
        }

        public IScanner Scan(ScanCall scan, CancellationToken? token = null)
        {
            return new Scanner(this, scan);
        }

        public async Task<GetResponse> Get(GetCall get, CancellationToken? token = null)
        {
            var res = await SendRPCToRegion<GetResponse>(get, token);
            return res;
        }

        public async Task<MutateResponse> Put(MutateCall put, CancellationToken? token = null)
        {
            put.MutationType = MutationProto.Types.MutationType.Put;
            var res = await SendRPCToRegion<MutateResponse>(put, token);
            return res;
        }

        public async Task<MutateResponse> Delete(MutateCall del, CancellationToken? token = null)
        {
            del.MutationType = MutationProto.Types.MutationType.Delete;
            var res = await SendRPCToRegion<MutateResponse>(del, token);
            return res;
        }

        public async Task<MutateResponse> Append(MutateCall apd, CancellationToken? token = null)
        {
            apd.MutationType = MutationProto.Types.MutationType.Append;
            var res = await SendRPCToRegion<MutateResponse>(apd, token);
            return res;
        }

        public async Task<long?> Increment(MutateCall inc, CancellationToken? token = null)
        {
            inc.MutationType = MutationProto.Types.MutationType.Increment;
            var res = await SendRPCToRegion<MutateResponse>(inc, token);
            if (res?.Result?.Cell?.Count == 1)
            {
                return (long)BinaryPrimitives.ReadUInt64BigEndian(res?.Result?.Cell[0].Value.ToByteArray());
            }

            return null;
        }

        private async Task<RegionClient> QueueRPC(ICall rpc, CancellationToken token)
        {
            var (client, reg) = await ResolveRegion(rpc, token);
            if (client == null || reg == null)
            {
                return null;
            }

            rpc.Info = reg;
            await client.QueueRPC(rpc);
            return client;
        }

        private void ProcessResolveRegionTask()
        {
            Task.Factory.StartNew(async () =>
            {
                while (DefaultCancellationSource.IsCancellationRequested == false)
                {
                    await Task.Delay(10);

                    while (_loadRegionQueue.TryDequeue(out var rpc))
                    {
                        var reg1 = GetInfoFromCache(rpc.Table, rpc.Key);
                        if (reg1?.Client != null) continue;

                        var (client, reg) = await TryLocateRegion(rpc, DefaultCancellationSource.Token);
                        if (client != null)
                        {
                            _cache.Add(reg);
                            _cache.Add(client);
                        }
                    }
                }
            }, TaskCreationOptions.LongRunning);
        }
        private async Task<(RegionClient client, RegionInfo info)> ResolveRegion(ICall rpc, CancellationToken token)
        {
            RegionInfo reg = null;
            RegionClient client = null;
            var millisecondsTimeout = 60000;
            var oldTime = DateTime.Now;
            reg = GetInfoFromCache(rpc.Table, rpc.Key);
            client = reg?.Client;
            if (client == null)
            {
                _loadRegionQueue.Enqueue(rpc);
                await TaskEx.WaitOn(() =>
                {
                    reg = GetInfoFromCache(rpc.Table, rpc.Key);
                    client = reg?.Client;
                    return client == null && rpc.FindRegionRetryCount < RetryCount;
                }, 5, millisecondsTimeout);
            }
            return (client, reg);
        }

        public async Task<TResponse> SendRPCToRegion<TResponse>(ICall rpc, CancellationToken? token) where TResponse : class, IMessage
        {
            token ??= DefaultCancellationSource.Token;
            while (token.Value.IsCancellationRequested == false)
            {
                if (rpc.RetryCount > RetryCount)
                {
                    _logger.LogError($"RPC ({rpc.Name}) retries more than {RetryCount} times and will not try again.");
                    return null;
                }

                var client = await QueueRPC(rpc, token.Value);
                if (client == null)
                {
                    _logger.LogError("queue rpc return none client.");
                    return null;
                }

                var result = await client.GetRPCResult(rpc.CallId);
                if (result.Msg is TResponse res)
                {
                    return res;
                }

                if (result.Msg != null)
                {
                    _logger.LogWarning($"Generic result parameter types do not match, HBase return type is {result.Msg.Descriptor.FullName}, but here is given {typeof(TResponse).FullName}");
                }

                switch (result.Error)
                {
                    case CallQueueTooBigException _:
                        rpc.RetryCount++;
                        continue;
                    case RetryableException _:
                        rpc.RetryCount++;
                        continue;
                    case TimeoutException _:
                        _cache.ClientDown(rpc.Info);
                        return null;
                    case DoNotRetryIOException _:
                        return null;
                    default:
                        return null;
                }
            }

            return null;
        }

        private async Task<(RegionClient client, RegionInfo info)> TryLocateRegion(ICall rpc, CancellationToken token)
        {
            var backoff = BackoffStart;
            while (rpc.FindRegionRetryCount < RetryCount && token.IsCancellationRequested == false)
            {
                rpc.FindRegionRetryCount++;
                var reg = await FindRegionInfoForRPC(rpc.Table, rpc.Key, token);
                if (reg == null)
                {
                    _logger.LogWarning($"Locate region failed in {rpc.FindRegionRetryCount}th，try the locate again after {backoff}.");
                    backoff = await TaskEx.SleepAndIncreaseBackoff(backoff, BackoffIncrease, token);
                    continue;
                }

                var client = _cache.GetClient(reg.Host, reg.Port)
                             ?? await new RegionClient(reg.Host, reg.Port, RegionType.ClientService)
                             {
                                 TimeOut = Timeout,
                                 EffectiveUser = EffectiveUser,
                                 CallQueueSize = CallQueueSize
                             }
                             .Build(RetryCount, token);
                reg.Client = client;
                return (client, reg);
            }
            return (null, null);
        }

        protected async Task<bool> LocateMetaClient(CancellationToken token)
        {
            if (_metaClient != null) return true;
            var meta = await TryLocateResource(ZkRoot + ConstString.MetaRegion, MetaRegionServer.Parser.ParseFrom, token);

            if (meta == null) return false;

            _metaClient = await new RegionClient(meta.Server.HostName, (ushort)meta.Server.Port, RegionType.ClientService)
            {
                TimeOut = Timeout,
                EffectiveUser = EffectiveUser,
                CallQueueSize = CallQueueSize
            }
            .Build(RetryCount, token);
            if (_metaClient != null)
            {
                _logger.LogInformation($"Locate meta server at : {_metaClient.Host}:{_metaClient.Port}");
            }
            return _metaClient != null;
        }

        private RegionInfo GetInfoFromCache(byte[] table, byte[] key)
        {
            if (BinaryComparer.Compare(table, _metaTableName) == 0) return _metaRegionInfo;
            var search = RegionInfo.CreateRegionSearchKey(table, key);
            var info = _cache.GetInfo(search);
            if (info == null || false == BinaryComparer.Equals(table, info.Table)) return null;
            if (info.StopKey.Length > 0 && BinaryComparer.Compare(key, info.StopKey) >= 0) return null;
            return info;
        }

        private async Task<RegionInfo> FindRegionInfoForRPC(byte[] table, byte[] key, CancellationToken token)
        {
            var result = default(RegionInfo);
            var searchKey = RegionInfo.CreateRegionSearchKey(table, key);
            var rpc = new ScanCall(_metaTableName, searchKey, table)
            {
                Families = _infoFamily,
                CloseScanner = true,
                Reversed = true,
                NumberOfRows = 1,
                Info = _metaRegionInfo
            };

            await _metaClient.QueueRPC(rpc);
            var res = await _metaClient.GetRPCResult(rpc.CallId);
            if (res?.Msg is ScanResponse scanResponse)
            {
                result = RegionInfo.ParseFromScanResponse(scanResponse);
            }

            if (result != null)
            {
                _logger.LogInformation($"Find region info :{result.Name.ToUtf8String()}, at {result.Host}:{result.Port}");
            }
            return result;
        }

        private byte[] FullyQualifiedTable(RegionInfo reg)
        {
            if (reg.Namespace?.Any() != true) return reg.Table;
            return BinaryEx.ConcatInOrder(reg.Namespace, new[] { ConstByte.Colon }, reg.Table);
        }

        public void Dispose()
        {
            DefaultCancellationSource.Cancel();
        }
    }
}