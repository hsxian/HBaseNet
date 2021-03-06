using System.Collections.Generic;
using System.Linq;
using CSharpTest.Net.Collections;
using HBaseNet.Logging;
using Microsoft.Extensions.Logging;

namespace HBaseNet.Region
{
    public class RegionCache
    {
        private BTreeDictionary<byte[], RegionInfo> KeyInfoCache { get; }
        private List<RegionClient> ClientCache { get; }
        private readonly ILogger<RegionCache> _logger;
        public RegionCache()
        {
            _logger = HBaseConfig.Instance.LoggerFactory?.CreateLogger<RegionCache>() ?? new DebugLogger<RegionCache>();
            KeyInfoCache = new BTreeDictionary<byte[], RegionInfo>(new RegionNameComparer());
            ClientCache = new List<RegionClient>();
        }

        public RegionInfo GetInfo(byte[] table, byte[] key)
        {
            var search = RegionInfo.CreateRegionSearchKey(table, key);
            return GetInfo(search);
        }

        public RegionInfo GetInfo(byte[] searchKey)
        {
            var (_, info) = KeyInfoCache.EnumerateFrom(searchKey).FirstOrDefault();
            return info;
        }

        private IEnumerable<RegionInfo> GetOverlaps(RegionInfo reg)
        {
            return KeyInfoCache.Values.Where(t => t.IsRegionOverlap(reg)).ToArray();
        }

        public RegionClient GetClient(string host, ushort port)
        {
            return ClientCache.FirstOrDefault(t => t.Host == host && t.Port == port);
        }

        private IEnumerable<RegionClient> GetClients(string host, ushort port)
        {
            return ClientCache.Where(t => t.Host == host && t.Port == port).ToArray();
        }

        public void Add(RegionInfo info)
        {
            //TODO:
            // var os = GetOverlaps(info).Where(t => t.ID < info.ID);
            // Remove(os);

            while (KeyInfoCache.ContainsKey(info.Name) == false)
            {
                KeyInfoCache.TryAdd(info.Name, info);
            }
        }

        public void Add(RegionClient client)
        {
            if (ClientCache.Any(t => t.Host == client.Host && t.Port == client.Port) == false)
            {
                ClientCache.Add(client);
            }
        }

        private void Remove(string host, ushort port)
        {
            var cs = GetClients(host, port);
            Remove(cs);
        }

        private void Remove(IEnumerable<RegionClient> cs)
        {
            foreach (var c in cs)
            {
                ClientCache.Remove(c);
                _logger.LogInformation($"Removed region client({c.Type}-{c.Host}:{c.Port})  from cache.");
            }
        }

        private void Remove(IEnumerable<RegionInfo> rs)
        {
            foreach (var c in rs)
            {
                KeyInfoCache.Remove(c.Name);
                _logger.LogInformation($"Removed region info({c})  from cache.");
            }
        }

        public void ClientDown(RegionInfo reg)
        {
            if (reg == null) return;
            Remove(reg.Host, reg.Port);
        }
    }
}