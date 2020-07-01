using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using CSharpTest.Net.Collections;
using CSharpTest.Net.IO;
using Google.Protobuf;
using HBaseNet.HRpc;
using HBaseNet.Region;
using HBaseNet.Region.Exceptions;
using HBaseNet.Utility;
using HBaseNet.Zk;
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
        private BTreeDictionary<byte[], RegionInfo> KeyInfoCache { get; }
        private List<RegionClient> ClientCache { get; }
        private volatile bool _isLocatingRegion;

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
            _metaRegionInfo = new RegionInfo(0, "hbase:meta".ToUtf8Bytes(), "hbase:meta,,1".ToUtf8Bytes(), new byte[0])
            {
                StopKey = new byte[0]
            };
            KeyInfoCache = new BTreeDictionary<byte[], RegionInfo>(new RegionNameComparer());
            ClientCache = new List<RegionClient>();
        }

        public async Task<bool> CheckTable(string table, CancellationToken? token = null)
        {
            token ??= DefaultCancellationSource.Token;
            var get = new GetCall(table, "theKey");
            return null != await SendRPCToRegion<GetResponse>(get, token);
        }

        public async Task<bool> CheckAndPut(MutateCall put, string family, string qualifier,
            byte[] expectedValue, CancellationToken? token = null)
        {
            token ??= DefaultCancellationSource.Token;
            var cas = new CheckAndPutCall(put, family, qualifier, expectedValue);
            var res = await SendRPCToRegion<MutateResponse>(cas, token);
            return res != null && res.Processed;
        }

        public async Task<List<Result>> Scan(ScanCall scan, CancellationToken? token = null)
        {
            token ??= DefaultCancellationSource.Token;
            var results = new List<Result>();
            ScanResponse scanres;
            ScanCall rpc = null;
            var table = scan.Table;
            var startRow = scan.StartRow;
            var stopRow = scan.StopRow;
            var families = scan.Families;
            var filters = scan.Filters;
            var timeRange = scan.TimeRange;
            var maxVersion = scan.MaxVersions;
            var numberOfRows = scan.NumberOfRows;
            do
            {
                rpc = new ScanCall(table, families, rpc == null ? startRow : rpc.Info.StopKey, stopRow)
                {
                    Filters = filters,
                    TimeRange = timeRange,
                    MaxVersions = maxVersion,
                    NumberOfRows = numberOfRows,
                };
                scanres = await SendRPCToRegion<ScanResponse>(rpc, token);
                if (scanres?.Results?.Any() != true) break;
                results.AddRange(scanres.Results);
                foreach (var item in scanres.Results)
                {
                    rpc = new ScanCall(table, scanres.ScannerId, rpc.Key, false);
                    scanres = await SendRPCToRegion<ScanResponse>(rpc, token);
                    if (scanres?.Results?.Any() != true) break;
                    results.AddRange(scanres.Results);
                }

                rpc = new ScanCall(table, scanres.ScannerId, rpc.Key, false);
                scanres = await SendRPCToRegion<ScanResponse>(rpc, token);
                if (rpc.Info.StopKey?.Length == 0
                    || stopRow.Length != 0 && BinaryComparer.Compare(stopRow, rpc.Info.StopKey) <= 0)
                    return results;
            } while (true);

            return results;
        }

        public async Task<GetResponse> Get(GetCall get, CancellationToken? token = null)
        {
            token ??= DefaultCancellationSource.Token;
            var res = await SendRPCToRegion<GetResponse>(get, token.Value);
            return res;
        }

        public async Task<MutateResponse> Put(MutateCall put, CancellationToken? token = null)
        {
            token ??= DefaultCancellationSource.Token;
            put.MutationType = MutationProto.Types.MutationType.Put;
            var res = await SendRPCToRegion<MutateResponse>(put, token.Value);
            return res;
        }

        public async Task<MutateResponse> Delete(MutateCall del, CancellationToken? token = null)
        {
            token ??= DefaultCancellationSource.Token;
            del.MutationType = MutationProto.Types.MutationType.Delete;
            var res = await SendRPCToRegion<MutateResponse>(del, token.Value);
            return res;
        }

        public async Task<MutateResponse> Append(MutateCall apd, CancellationToken? token = null)
        {
            token ??= DefaultCancellationSource.Token;
            apd.MutationType = MutationProto.Types.MutationType.Append;
            var res = await SendRPCToRegion<MutateResponse>(apd, token.Value);
            return res;
        }

        public async Task<long?> Increment(MutateCall inc, CancellationToken? token = null)
        {
            token ??= DefaultCancellationSource.Token;
            inc.MutationType = MutationProto.Types.MutationType.Increment;
            var res = await SendRPCToRegion<MutateResponse>(inc, token.Value);
            if (res?.Result?.Cell?.Count == 1)
            {
                return (long) BinaryPrimitives.ReadUInt64BigEndian(res?.Result?.Cell[0].Value.ToByteArray());
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

        private async Task<(RegionClient client, RegionInfo info)> ResolveRegion(ICall rpc, CancellationToken token)
        {
            var reg = GetInfoFromCache(rpc.Table, rpc.Key);
            var client = reg?.Client;
            if (client != null)
                return (client, reg);

            await TaskEx.WaitOn(() => _isLocatingRegion, 1, 20000);
            _isLocatingRegion = true;

            reg = GetInfoFromCache(rpc.Table, rpc.Key);
            client = reg?.Client;

            if (client == null || reg == null)
            {
                (client, reg) = await LocateRegion(rpc.Table, rpc.Key, token);
                if (reg != null)
                {
                    var os = GetOverlaps(reg);
                    foreach (var item in os)
                    {
                        KeyInfoCache.TryRemove(item.Name, out _);
                    }

                    while (KeyInfoCache.ContainsKey(reg.Name) == false)
                    {
                        KeyInfoCache.TryAdd(reg.Name, reg);
                    }

                    if (ClientCache.Any(t => t.Host == reg.Host && t.Port == reg.Port) == false)
                    {
                        ClientCache.Add(client);
                    }
                }
            }

            _isLocatingRegion = false;
            return (client, reg);
        }

        protected async Task<TResponse> SendRPCToRegion<TResponse>(ICall rpc, CancellationToken? token)
            where TResponse : class, IMessage
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
                    _logger.LogWarning("queue rpc return none client.");
                    return null;
                }

                var result = await client.GetRPCResult(rpc.CallId);
                if (result.Msg is TResponse res)
                {
                    return res;
                }

                if (result.Msg != null)
                {
                    _logger.LogWarning(
                        $"Generic result parameter types do not match, HBase return type is {result.Msg.Descriptor.FullName}, but here is given {typeof(TResponse).FullName}");
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
                        ClientDown(rpc.Info);
                        return null;
                    default:
                        return null;
                }
            }

            return null;
        }

        private void ClientDown(RegionInfo reg)
        {
            if (reg == null) return;
            var cs = ClientCache.Where(t => t.Host == reg.Host && t.Port == reg.Port).ToArray();
            foreach (var c in cs)
            {
                ClientCache.Remove(c);
            }
        }

        private async Task<(RegionClient client, RegionInfo info)> TryLocateRegion(byte[] searchKey,
            CancellationToken token)
        {
            var backoff = BackoffStart;
            for (var i = 0; i < RetryCount && token.IsCancellationRequested == false; i++)
            {
                var reg = await FindRegionInfoForRPC(searchKey, token);
                if (reg == null)
                {
                    _logger.LogWarning(
                        $"Locate region failed in {i + 1}th，try the locate again after {backoff}.");
                    backoff = await TaskEx.SleepAndIncreaseBackoff(backoff, BackoffIncrease, token);
                    continue;
                }

                var client = GetRegionFromCache(reg.Host, reg.Port)
                             ?? await new RegionClient(reg.Host, reg.Port, RegionType.ClientService)
                                 .Build(RetryCount, token);
                reg.Client = client;
                return (client, reg);
            }

            return (null, null);
        }

        private async Task<(RegionClient client, RegionInfo info)> LocateRegion(byte[] table, byte[] key,
            CancellationToken token)
        {
            var searchKey = RegionInfo.CreateRegionSearchKey(table, key);
            var result = await TryLocateRegion(searchKey, token);
            return result;
        }


        protected async Task<bool> LocateMetaClient(CancellationToken token)
        {
            if (_metaClient != null) return true;
            var meta = await TryLocateResource(ZkHelper.HBaseMeta, MetaRegionServer.Parser.ParseFrom,
                token);

            _metaClient = await new RegionClient(meta.Server.HostName, (ushort) meta.Server.Port,
                    RegionType.ClientService)
                .Build(RetryCount, token);
            if (_metaClient != null)
                _logger.LogInformation($"Locate meta server at : {_metaClient.Host}:{_metaClient.Port}");
            return _metaClient != null;
        }

        private RegionClient GetRegionFromCache(string host, ushort port)
        {
            return ClientCache.FirstOrDefault(t => t.Host == host && t.Port == port);
        }

        private RegionInfo GetInfoFromCache(byte[] table, byte[] key)
        {
            if (BinaryEx.Compare(table, _metaTableName) == 0) return _metaRegionInfo;
            var search = RegionInfo.CreateRegionSearchKey(table, key);
            var (_, info) = KeyInfoCache.EnumerateFrom(search).FirstOrDefault();
            if (info == null || false == BinaryComparer.Equals(table, info.Table)) return null;
            if (info.StopKey.Length > 0 && BinaryComparer.Compare(key, info.StopKey) > 0) return null;
            _logger.LogDebug(
                $"get region info from cache, search key:{search.ToUtf8String()},match region name:{info.Name.ToUtf8String()}");
            return info;
        }

        private async Task<RegionInfo> FindRegionInfoForRPC(byte[] searchKey,
            CancellationToken token)
        {
            var result = default(RegionInfo);
            var getCall = new GetCall(_metaTableName, searchKey, true) {Families = _infoFamily, Info = _metaRegionInfo};
            await _metaClient.QueueRPC(getCall);
            var resp = await _metaClient.GetRPCResult(getCall.CallId);

            if (resp?.Msg is GetResponse response)
            {
                result = RegionInfo.ParseFromGetResponse(response);
                if (result != null)
                {
                    _logger.LogInformation(
                        $"Find region info :{result.Name.ToUtf8String()}, at {result.Host}:{result.Port}");
                }
            }

            return result;
        }

        public RegionInfo[] GetOverlaps(RegionInfo reg)
        {
            return KeyInfoCache.Values.Where(t => t.IsRegionOverlap(reg)).ToArray();
        }

        public void Dispose()
        {
            DefaultCancellationSource.Cancel();
        }
    }
}