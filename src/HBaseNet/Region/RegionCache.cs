using System.Collections.Generic;
using System.Linq;
using CSharpTest.Net.Collections;

namespace HBaseNet.Region
{
    public class RegionCache
    {
        private BTreeDictionary<byte[], RegionInfo> KeyInfoCache { get; }
        private List<RegionClient> ClientCache { get; }

        public RegionCache()
        {
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
            var os = GetOverlaps(info).Where(t => t.ID < info.ID);
            Remove(os);

            while (KeyInfoCache.ContainsKey(info.Name) == false)
            {
                KeyInfoCache.TryAdd(info.Name, info);
            }
        }

        public void Add(RegionInfo info, RegionClient client)
        {
            if (ClientCache.Any(t => t.Host == info.Host && t.Port == info.Port) == false)
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
            }
        }

        private void Remove(IEnumerable<RegionInfo> rs)
        {
            foreach (var c in rs)
            {
                KeyInfoCache.Remove(c.Name);
            }
        }

        public void ClientDown(RegionInfo reg)
        {
            if (reg == null) return;
            Remove(reg.Host, reg.Port);
        }
    }
}