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
        private static byte[] MetaTableName;
        private static Dictionary<string, string[]> InfoFamily;
        private static RegionInfo MetaRegionInfo;
        private RegionClient _metaClient;
        private readonly ZkHelper _zkHelper;
        private ConcurrentDictionary<RegionInfo, RegionClient> RegionClientCache { get; set; }
        private BTreeDictionary<byte[], RegionInfo> KeyRegionCache2 { get; set; }

        public HBaseClient(string zkquorum)
        {
            _zkquorum = zkquorum;
            _logger = HBaseConfig.Instance.LoggerFactory.CreateLogger<HBaseClient>();
            _zkHelper = new ZkHelper();
            InfoFamily = new Dictionary<string, string[]>
            {
                {"info", null}
            };
            MetaTableName = "hbase:meta".ToUtf8Bytes();
            MetaRegionInfo = new RegionInfo
            {
                Table = "hbase:meta".ToUtf8Bytes(),
                RegionName = "hbase:meta,,1".ToUtf8Bytes(),
                StopKey = new byte[0]
            };
            RegionClientCache = new ConcurrentDictionary<RegionInfo, RegionClient>();
            KeyRegionCache2 = new BTreeDictionary<byte[], RegionInfo>(new RegionNameComparer());
            // ReestablishRegion(MetaRegionInfo);
        }

        public async Task<bool> CheckTable(string table)
        {
            var get = new GetCall(table, "theKey");
            return null != await SendRPCToRegion<GetResponse>(get);
        }

        public async Task<GetResponse> Get(GetCall get)
        {
            return await SendRPCToRegion<GetResponse>(get);
        }

        private async Task<MutateResponse> Mutate(ICall mutate)
        {
            return await SendRPCToRegion<MutateResponse>(mutate);
        }

        public async Task<MutateResponse> Put(MutateCall mutate)
        {
            mutate.MutationType = MutationProto.Types.MutationType.Put;
            return await Mutate(mutate);
        }

        public async Task<MutateResponse> Delete(MutateCall mutate)
        {
            mutate.MutationType = MutationProto.Types.MutationType.Delete;
            return await Mutate(mutate);
        }

        public async Task<MutateResponse> Append(MutateCall mutate)
        {
            mutate.MutationType = MutationProto.Types.MutationType.Append;
            return await Mutate(mutate);
        }

        public async Task<MutateResponse> Increment(MutateCall mutate)
        {
            mutate.MutationType = MutationProto.Types.MutationType.Increment;
            return await Mutate(mutate);
        }

        public async Task<List<Result>> Scan(ScanCall scan)
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
                scanres = await SendRPCToRegion<ScanResponse>(rpc);
                if (scanres?.Results?.Any() != true) break;
                results.AddRange(scanres.Results);
                foreach (var item in scanres.Results)
                {
                    rpc = new ScanCall(table, scanres.ScannerId, rpc.Key, false);
                    scanres = await SendRPCToRegion<ScanResponse>(rpc);
                    if (scanres?.Results?.Any() != true) break;
                    results.AddRange(scanres.Results);
                }

                rpc = new ScanCall(table, scanres.ScannerId, rpc.Key, false);
                scanres = await SendRPCToRegion<ScanResponse>(rpc);
                if (rpc.Info.StopKey?.Length == 0
                    || stopRow.Length != 0 && BinaryComparer.Compare(stopRow, rpc.Info.StopKey) <= 0)
                    return results;
            } while (true);

            return results;
        }

        private readonly SemaphoreSlim _locateRegionSemaphore = new SemaphoreSlim(1, 1);

        private async Task<RegionClient> QueueRPC(ICall rpc)
        {
            var reg = GetRegionInfo(rpc.Table, rpc.Key);
            RegionClient client = null;
            if (reg != null)
            {
                client = ClientFor(reg);
            }

            if (client == null)
            {
                _locateRegionSemaphore.Wait();
                var lr = await LocateRegion(rpc.Table, rpc.Key);
                _locateRegionSemaphore.Release();

                if (lr != null)
                {
                    client = lr.Value.client;
                    reg = lr.Value.info;
                }
                else
                {
                    return null;
                }
            }

            rpc.Info = reg;
            client.QueueRPC(rpc);
            return client;
        }

        private async Task<TResponse> SendRPCToRegion<TResponse>(ICall rpc) where TResponse : class, IMessage
        {
            var client = await QueueRPC(rpc);
            if (client == null)
            {
                _logger.LogWarning("queue rpc return none client.");
                return null;
            }

            var result = await client.GetRPCResult();
            if (result.Msg is TResponse res)
            {
                return res;
            }

            return null;
        }

        private async Task<(RegionClient client, RegionInfo info)?> LocateRegion(byte[] table, byte[] key)
        {
            if (_metaClient == null)
            {
                await LocateMeta();
            }

            var metaKey = RegionInfo.CreateRegionSearchKey(table, key);
            var rpc = GetCall.CreateGetBefore(MetaTableName, metaKey);
            rpc.Families = InfoFamily;
            rpc.Info = MetaRegionInfo;
            if (!await _metaClient.SendRPC(rpc)) return null;
            var resp = await _metaClient.ReceiveRPC();
            if (!(resp.Msg is GetResponse response)) return null;
            var discover = await DiscoverRegion(response);
            if (discover?.client != null)
            {
                _logger.LogInformation(
                    $"Locate region server at : {discover.Value.client.Host}:{discover.Value.client.Port}, RegionName: {discover.Value.info.RegionName.ToUtf8String()}");
            }

            return discover;
        }

        private async Task<(RegionClient client, RegionInfo info)?> DiscoverRegion(GetResponse metaRow)
        {
            if (metaRow?.HasResult != true) return null;

            var regCell = metaRow.Result.Cell
                .FirstOrDefault(t => t.Qualifier.ToStringUtf8().Equals(ConstString.RegionInfo));
            var reg = RegionInfo.ParseFromCell(regCell);
            if (reg == null) return null;
            var server = metaRow.Result.Cell
                .FirstOrDefault(t => t.Qualifier.ToStringUtf8().Equals(ConstString.Server) && t.HasValue);

            if (server == null) return null;
            var serverData = server.Value.ToArray();
            var ss = serverData.ToUtf8String();
            var idxColon = Array.IndexOf(serverData, ConstByte.Colon);
            if (idxColon < 1) return null;
            var host = serverData[..idxColon].ToUtf8String();
            if (!ushort.TryParse(serverData[(idxColon + 1)..].ToUtf8String(), out var port)) return null;
            var client = await new RegionClient(host, port).Build();
            if (client == null) return null;
            AddRegionToCache(reg, client);
            return await Task.FromResult((client, reg));
        }

        private void AddRegionToCache(RegionInfo reg, RegionClient client)
        {
            KeyRegionCache2.TryAdd(reg.RegionName, reg);
            RegionClientCache.TryAdd(reg, client);
        }

        private async Task LocateMeta()
        {
            var zkc = _zkHelper.CreateClient(_zkquorum, TimeSpan.FromSeconds(30));
            var meta = await _zkHelper.LocateResource(zkc, ZkHelper.HBaseMeta, MetaRegionServer.Parser.ParseFrom);
            await zkc.closeAsync();
            
            _metaClient = await new RegionClient(meta.Server.HostName, (ushort) meta.Server.Port).Build();
            if (_metaClient != null)
                _logger.LogInformation($"Locate meta server at : {_metaClient.Host}:{_metaClient.Port}");
        }

        private RegionClient ClientFor(RegionInfo info)
        {
            if (info == MetaRegionInfo) return _metaClient;
            return RegionClientCache.TryGetValue(info, out var reg) ? reg : null;
        }

        private bool IsCacheKeyForTable(byte[] table, byte[] cacheKey)
        {
            for (var i = 0; i < table.Length; i++)
            {
                if (table[i] != cacheKey[i]) return false;
            }

            return cacheKey[table.Length] == ConstByte.Comma;
        }

        private async Task ReestablishRegion(RegionInfo reg)
        {
            if (reg != MetaRegionInfo)
            {
                RegionClientCache.TryRemove(reg, out _);
            }

            while (true)
            {
                if (reg == MetaRegionInfo)
                {
                    await LocateMeta();
                }
                else
                {
                    await LocateRegion(reg.Table, reg.StartKey);
                }
            }
        }

        private RegionInfo GetRegionInfo(byte[] table, byte[] key)
        {
            if (table == MetaTableName) return MetaRegionInfo;

            var search = RegionInfo.CreateRegionSearchKey(table, key);
            var (_, info) = KeyRegionCache2.EnumerateFrom(search).FirstOrDefault();
            if (
                info == null
                || info.StopKey.Length > 0 && BinaryComparer.Compare(key, info.StopKey) > 0
                || false == IsCacheKeyForTable(table, info.RegionName)
            )
                return null;
            _logger.LogDebug(
                $"get region info from cache, search key:{search.ToUtf8String()},match region name:{info.RegionName.ToUtf8String()}");
            return info;
        }
    }
}