using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using CSharpTest.Net.Collections;
using CSharpTest.Net.IO;
using Google.Protobuf;
using HBaseNet.Const;
using HBaseNet.HRpc;
using HBaseNet.Region;
using HBaseNet.Region.Exceptions;
using HBaseNet.Utility;
using HBaseNet.Zk;
using Microsoft.Extensions.Logging;
using Pb;
using RegionInfo = HBaseNet.Region.RegionInfo;
using RegionClient = HBaseNet.Region.RegionClient;

namespace HBaseNet
{
    public class HBaseClient
    {
        private readonly ILogger<HBaseClient> _logger;
        private readonly string _zkquorum;
        private readonly byte[] _metaTableName;
        private readonly Dictionary<string, string[]> _infoFamily;
        private readonly RegionInfo _metaRegionInfo;
        private readonly RegionInfo _adminRegionInfo;
        private RegionClient _metaClient;
        private RegionClient _adminClient;
        private readonly ZkHelper _zkHelper;
        private ConcurrentDictionary<RegionInfo, RegionClient> InfoRegionCache { get; }
        private BTreeDictionary<byte[], RegionInfo> KeyInfoCache { get; }
        public TimeSpan BackoffStart { get; set; } = TimeSpan.FromMilliseconds(16);
        public int RetryCount { get; set; } = 9;
        public ClientType Type { get; set; }
        public CancellationTokenSource DefaultCancellationSource { get; set; }
        private bool _isLocatingMetaClient;
        private bool _isLocatingMasterClient;
        private bool _isLocatingRegion;

        public HBaseClient(string zkquorum, ClientType type = ClientType.StandardClient)
        {
            Type = type;
            _zkquorum = zkquorum;
            _logger = HBaseConfig.Instance.LoggerFactory.CreateLogger<HBaseClient>();
            _zkHelper = new ZkHelper();
            _infoFamily = new Dictionary<string, string[]>
            {
                {"info", null}
            };
            _metaTableName = "hbase:meta".ToUtf8Bytes();
            _metaRegionInfo = new RegionInfo
            {
                Table = "hbase:meta".ToUtf8Bytes(),
                RegionName = "hbase:meta,,1".ToUtf8Bytes(),
                StopKey = new byte[0]
            };
            _adminRegionInfo = new RegionInfo();
            InfoRegionCache = new ConcurrentDictionary<RegionInfo, RegionClient>();
            KeyInfoCache = new BTreeDictionary<byte[], RegionInfo>(new RegionNameComparer());
            DefaultCancellationSource = new CancellationTokenSource();
        }

        public async Task<bool> CheckTable(string table, CancellationToken? token = null)
        {
            var get = new GetCall(table, "theKey");
            return null != await SendRPCToRegion<GetResponse>(get, token);
        }

        public async Task<List<Result>> Scan(ScanCall scan, CancellationToken? token = null)
        {
            var results = new List<Result>();
            ScanResponse scanres;
            ScanCall rpc = null;
            var table = scan.Table;
            var startRow = scan.StartRow;
            var stopRow = scan.StopRow;
            var families = scan.Families;
            var filters = scan.Filters;
            do
            {
                rpc = rpc == null
                    ? new ScanCall(table, families, startRow, stopRow) {Filters = filters}
                    : new ScanCall(table, families, rpc.Info.StopKey, stopRow) {Filters = filters};
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
            if (Type == ClientType.AdminClient)
            {
                if (_adminClient == null)
                {
                    await LocateMasterClient(token);
                    return (_adminClient, _adminRegionInfo);
                }
            }

            var reg = GetInfoFromCache(rpc.Table, rpc.Key);
            var client = GetRegionFromCache(reg);
            if (client != null)
                return (client, reg);

            if (_metaClient == null)
            {
                await LocateMetaClient(token);
            }

            await TaskEx.WaitOn(() => _isLocatingRegion);
            reg = GetInfoFromCache(rpc.Table, rpc.Key);
            client = GetRegionFromCache(reg);

            if (client != null)
                return (client, reg);


            (client, reg) = await LocateRegion(rpc.Table, rpc.Key, token);
            if (reg != null)
            {
                KeyInfoCache.TryAdd(reg.RegionName, reg);
                InfoRegionCache.TryAdd(reg, client);
            }

            return (client, reg);
        }

        public async Task<T> SendRPC<T>(ICall rpc, CancellationToken? token = null) where T : class, IMessage
        {
            return await SendRPCToRegion<T>(rpc, token);
        }

        private async Task<TResponse> SendRPCToRegion<TResponse>(ICall rpc, CancellationToken? token)
            where TResponse : class, IMessage
        {
            token ??= DefaultCancellationSource.Token;
            while (rpc.RetryCount < RetryCount && token.Value.IsCancellationRequested == false)
            {
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

                switch (result.Error)
                {
                    case CallQueueTooBigException _:
                        rpc.RetryCount++;
                        continue;
                    case RetryableException _:
                        rpc.RetryCount++;
                        continue;
                }
            }

            return null;
        }

        private async Task<(RegionClient client, RegionInfo info)> TryLocateRegion(byte[] searchKey,
            CancellationToken token)
        {
            var backoff = BackoffStart;
            for (var i = 0; i < RetryCount && token.IsCancellationRequested == false; i++)
            {
                var (reg, host, port) = await FindRegionInfoForRPC(searchKey, token);
                if (reg == null || port == 0)
                {
                    _logger.LogWarning(
                        $"Locate region failed in {i + 1}th，try the locate again after {backoff}.");
                    backoff = await SleepAndIncreaseBackoff(backoff, token);
                    continue;
                }

                var cacheClient = InfoRegionCache.Values.FirstOrDefault(t => t.Host == host && t.Port == port);
                if (cacheClient != null)
                {
                    return (cacheClient, reg);
                }

                var client = await new RegionClient(host, port, RegionType.ClientService).Build();
                return (client, reg);
            }

            return (null, null);
        }

        private async Task<(RegionClient client, RegionInfo info)> LocateRegion(byte[] table, byte[] key,
            CancellationToken token)
        {
            if (_metaClient == null) return (null, null);
            _isLocatingRegion = true;
            var searchKey = RegionInfo.CreateRegionSearchKey(table, key);
            var result = await TryLocateRegion(searchKey, token);
            _isLocatingRegion = false;
            return result;
        }

        private async Task<TimeSpan> SleepAndIncreaseBackoff(TimeSpan backoff, CancellationToken token)
        {
            await Task.Delay(backoff, token);
            return backoff < TimeSpan.FromSeconds(5) ? backoff * 2 : backoff + TimeSpan.FromSeconds(5);
        }

        private async Task<TResult> TryLocateResource<TResult>(string resource,
            Func<byte[], TResult> getResultFunc, CancellationToken token)
        {
            var zkc = _zkHelper.CreateClient(_zkquorum, TimeSpan.FromSeconds(30));
            var backoff = BackoffStart;
            var result = default(TResult);
            for (var i = 0; i < RetryCount && token.IsCancellationRequested == false; i++)
            {
                result = await _zkHelper.LocateResource(zkc, resource, getResultFunc);
                if (result == null)
                {
                    _logger.LogWarning(
                        $"Locate {resource} failed in {i + 1}th，try the locate again after {backoff}.");
                    backoff = await SleepAndIncreaseBackoff(backoff, token);
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

        private async Task LocateMetaClient(CancellationToken token)
        {
            await TaskEx.WaitOn(() => _isLocatingMetaClient);
            if (_metaClient != null) return;
            _isLocatingMetaClient = true;
            var meta = await TryLocateResource(ZkHelper.HBaseMeta, MetaRegionServer.Parser.ParseFrom,
                token);

            _metaClient = await new RegionClient(meta.Server.HostName, (ushort) meta.Server.Port,
                    RegionType.ClientService)
                .Build();
            if (_metaClient != null)
                _logger.LogInformation($"Locate meta server at : {_metaClient.Host}:{_metaClient.Port}");
            _isLocatingMetaClient = false;
        }

        private async Task LocateMasterClient(CancellationToken token)
        {
            await TaskEx.WaitOn(() => _isLocatingMasterClient);
            if (_adminClient != null) return;
            _isLocatingMasterClient = true;
            var master = await TryLocateResource(ZkHelper.HBaseMaster, Master.Parser.ParseFrom,
                token);

            _adminClient = await new RegionClient(master.Master_.HostName, (ushort) master.Master_.Port,
                    RegionType.MasterService)
                .Build();
            if (_adminClient != null)
                _logger.LogInformation($"Locate master server at : {_adminClient.Host}:{_adminClient.Port}");
            _isLocatingMasterClient = false;
        }

        private bool IsCacheKeyForTable(byte[] table, byte[] cacheKey)
        {
            for (var i = 0; i < table.Length; i++)
            {
                if (table[i] != cacheKey[i]) return false;
            }

            return cacheKey[table.Length] == ConstByte.Comma;
        }

        private RegionClient GetRegionFromCache(RegionInfo reg)
        {
            if (reg == null) return null;
            InfoRegionCache.TryGetValue(reg, out var c);
            return c;
        }

        private RegionInfo GetInfoFromCache(byte[] table, byte[] key)
        {
            if (BinaryEx.Compare(table, _metaTableName) == 0) return _metaRegionInfo;
            if (Type == ClientType.AdminClient) return _adminRegionInfo;

            var search = RegionInfo.CreateRegionSearchKey(table, key);
            var (_, info) = KeyInfoCache.EnumerateFrom(search).FirstOrDefault();
            if (info == null || false == IsCacheKeyForTable(table, info.RegionName)) return null;
            if (info.StopKey.Length > 0 && BinaryComparer.Compare(key, info.StopKey) > 0) return null;
            _logger.LogDebug(
                $"get region info from cache, search key:{search.ToUtf8String()},match region name:{info.RegionName.ToUtf8String()}");
            return info;
        }

        private async Task<(RegionInfo, string, ushort)> FindRegionInfoForRPC(byte[] searchKey, CancellationToken token)
        {
            if (_metaClient == null) return (null, null, 0);
            var getCall = GetCall.CreateGetBefore(_metaTableName, searchKey);
            getCall.Families = _infoFamily;
            getCall.Info = _metaRegionInfo;
            await _metaClient.QueueRPC(getCall);
            var resp = await _metaClient.GetRPCResult(getCall.CallId);

            if (resp?.Msg is GetResponse response)
            {
                return ParseMetaTableResponse(response);
            }

            return (null, null, 0);
        }

        private (RegionInfo, string, ushort) ParseMetaTableResponse(GetResponse metaRow)
        {
            if (metaRow?.HasResult != true) return (null, null, 0);

            var regCell = metaRow.Result.Cell
                .FirstOrDefault(t => t.Qualifier.ToStringUtf8().Equals(ConstString.RegionInfo));

            var reg = RegionInfo.ParseFromCell(regCell);
            if (reg == null) return (null, null, 0);

            var server = metaRow.Result.Cell
                .FirstOrDefault(t => t.Qualifier.ToStringUtf8().Equals(ConstString.Server) && t.HasValue);
            if (server == null) return (null, null, 0);
            var serverData = server.Value.ToArray();

            var idxColon = Array.IndexOf(serverData, ConstByte.Colon);
            if (idxColon < 1) return (null, null, 0);

            var host = serverData[..idxColon].ToUtf8String();
            if (false == ushort.TryParse(serverData[(idxColon + 1)..].ToUtf8String(), out var port))
                return (null, null, 0);
            _logger.LogInformation($"Find region info :{reg.RegionName.ToUtf8String()}, at {host}:{port}");
            return (reg, host, port);
        }
    }
}