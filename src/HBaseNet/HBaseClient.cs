using System;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Google.Protobuf;
using HBaseNet.Const;
using HBaseNet.HRpc;
using HBaseNet.Region;
using HBaseNet.Utility;
using HBaseNet.Zk;
using Pb;
using RegionInfo = HBaseNet.Region.RegionInfo;
using RegionClient = HBaseNet.Region.RegionClient;

namespace HBaseNet
{
    public class HBaseClient
    {
        private readonly string _zkquorum;
        public static byte[] MetaTableName;
        public static Dictionary<string, string[]> InfoFamily;
        public static RegionInfo MetaRegionInfo;
        private RegionClient _metaClient;
        private ZkHelper _zkHelper;
        public ConcurrentDictionary<RegionInfo, RegionClient> RegionClientCache { get; private set; }
        public ConcurrentDictionary<byte[], RegionInfo> KeyRegionCache { get; private set; }

        public HBaseClient(string zkquorum)
        {
            _zkquorum = zkquorum;
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
            };
            RegionClientCache = new ConcurrentDictionary<RegionInfo, RegionClient>();
            KeyRegionCache = new ConcurrentDictionary<byte[], RegionInfo>();
        }

        public async Task<bool> CheckTable(string table)
        {
            var get = new GetCall(table, "theKey", null);
            return null != await SendRpcToRegion<GetResponse>(get);
        }

        public async Task<GetResponse> Get(string table, string rowKey, IDictionary<string, string[]> families)
        {
            var get = new GetCall(table, rowKey, families);
            return await SendRpcToRegion<GetResponse>(get);
        }

        public async Task<MutateResponse> Mutate(string table, string rowKey,
            IDictionary<string, IDictionary<string, byte[]>> values, MutationProto.Types.MutationType mutationType)
        {
            var mutate = new MutateCall(table, rowKey, values, mutationType);
            return await SendRpcToRegion<MutateResponse>(mutate);
        }

        public async Task<MutateResponse> Put(string table, string rowKey,
            IDictionary<string, IDictionary<string, byte[]>> values)
        {
            return await Mutate(table, rowKey, values, MutationProto.Types.MutationType.Put);
        }

        public async Task<MutateResponse> Delete(string table, string rowKey,
            IDictionary<string, IDictionary<string, byte[]>> values)
        {
            return await Mutate(table, rowKey, values, MutationProto.Types.MutationType.Delete);
        }

        public async Task<MutateResponse> Append(string table, string rowKey,
            IDictionary<string, IDictionary<string, byte[]>> values)
        {
            return await Mutate(table, rowKey, values, MutationProto.Types.MutationType.Append);
        }

        public async Task<MutateResponse> Increment(string table, string rowKey,
            IDictionary<string, IDictionary<string, byte[]>> values)
        {
            return await Mutate(table, rowKey, values, MutationProto.Types.MutationType.Increment);
        }

        public async Task<ScanResponse> Scan(string table,
            IDictionary<string, string[]> families, byte[] startRow, byte[] stopRow)
        {
            var scan = new ScanCall(table, families, startRow, stopRow);
            return await SendRpcToRegion<ScanResponse>(scan);
        }

        private async Task<TResponse> SendRpcToRegion<TResponse>(ICall rpc) where TResponse : class, IMessage
        {
            var reg = GetRegionInfo(rpc.Table, rpc.Key);
            RegionClient client = null;
            if (reg != null)
            {
                client = ClientFor(reg);
            }

            if (client == null)
            {
                var lr = await LocateRegion(rpc.Table, rpc.Key);
                client = lr?.client;
                reg = lr?.info;
            }

            if (client == null)
            {
                return null;
            }

            rpc.Region = reg.RegionName;
            return await client.SendRPC<TResponse>(rpc);
        }

        private async Task<(RegionClient client, RegionInfo info)?> LocateRegion(byte[] table, byte[] key)
        {
            if (_metaClient == null)
            {
                await LocateMeta();
            }

            var metaKey = CreateRegionSearchKey(table, key);
            var rpc = GetCall.CreateGetBefore(MetaTableName, metaKey, InfoFamily);
            rpc.Region = MetaRegionInfo.RegionName;
            var resp = await _metaClient.SendRPC<GetResponse>(rpc);
            var discover = await DiscoverRegion(resp);
            return discover;
        }

        private async Task<(RegionClient client, RegionInfo info)?> DiscoverRegion(GetResponse metaRow)
        {
            if (metaRow?.HasResult == false) return null;
            var regCell = metaRow.Result.Cell
                .FirstOrDefault(t => t.Qualifier.ToString().Equals(ConstString.RegionInfo));
            var reg = RegionInfo.ParseFromCell(regCell);
            if (reg == null) return null;
            var server = metaRow.Result.Cell
                .FirstOrDefault(t => t.Qualifier.ToString().Equals(ConstString.Server) && t.HasValue);

            if (server == null) return null;
            var serverData = server.Value.ToArray();
            var idxColon = Array.IndexOf(serverData, ConstByte.Colon);
            if (idxColon < 1) return null;
            var host = Encoding.Default.GetString(serverData[..idxColon]);
            var port = (ushort) BinaryPrimitives.ReadUInt32LittleEndian(serverData[(idxColon + 1)..]);
            var client = new RegionClient(host, port);
            return (client, reg);
        }

        private async Task LocateMeta()
        {
            var zkc = _zkHelper.CreateClient(_zkquorum, TimeSpan.FromSeconds(30));
            var meta = await _zkHelper.LocateResource(zkc, ZkHelper.HBaseMeta, MetaRegionServer.Parser.ParseFrom);
            _metaClient = new RegionClient(meta.Server.HostName, (ushort) meta.Server.Port);
        }

        private RegionClient ClientFor(RegionInfo info)
        {
            if (info == MetaRegionInfo) return _metaClient;
            return RegionClientCache.TryGetValue(info, out var reg) ? reg : null;
        }

        private bool IsCacheKeyForTable(byte[] table, byte[] cacheKey)
        {
            return table.SequenceEqual(cacheKey)
                   && cacheKey[table.Length] == ConstByte.Comma;
        }

        private RegionInfo GetRegionInfo(byte[] table, byte[] key)
        {
            if (table == MetaTableName) return MetaRegionInfo;
            var regionName = CreateRegionSearchKey(table, key);
            if (KeyRegionCache.TryGetValue(regionName, out var info)
                && IsCacheKeyForTable(table, info.Table))
            {
                if (info.StopKey.Length != 0
                    && BinaryEx.Compare(key, 0, info.StopKey, 0, key.Length) >= 0)
                    return null;
                return info;
            }

            return null;
        }

        private byte[] CreateRegionSearchKey(byte[] table, byte[] key)
        {
            var metaKey = BinaryEx.ConcatInOrder(
                table,
                new[] {ConstByte.Comma},
                key,
                new[] {ConstByte.Comma},
                new[] {ConstByte.Colon}
            );
            return metaKey;
        }
    }
}