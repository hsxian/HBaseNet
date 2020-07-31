using System;
using System.Buffers.Binary;
using System.Linq;
using CSharpTest.Net.IO;
using HBaseNet.Const;
using HBaseNet.Utility;
using Pb;

namespace HBaseNet.Region
{
    public class RegionInfo
    {
        /// <summary>
        /// A timestamp when the region is created
        /// </summary>
        public ulong ID { get; set; }
        public byte[] Namespace { get; set; } = ConstByte.DefaultNamespace;

        public byte[] Table { get; set; }

        /// <summary>
        /// table_name,start_key,timestamp[.MD5.]
        /// </summary>
        public byte[] Name { get; set; }

        public byte[] StartKey { get; set; }
        public byte[] StopKey { get; set; }
        public string Host { get; set; }
        public ushort Port { get; set; }
        public RegionClient Client { get; set; }

        public RegionInfo(ulong id, byte[] @namespace, byte[] table, byte[] name, byte[] startKey)
        {
            ID = id;
            Namespace = @namespace;
            Table = table;
            Name = name;
            StartKey = startKey;
        }

        public static byte[] CreateRegionSearchKey(byte[] table, byte[] key)
        {
            var metaKey = BinaryEx.ConcatInOrder(
                table,
                new[] { ConstByte.Comma },
                key,
                new[] { ConstByte.Comma },
                new[] { ConstByte.Colon }
            );
            return metaKey;
        }

        public static int Compare(byte[] a, byte[] b)
        {
            var length = Math.Min(a.Length, b.Length);
            var i = 0;
            for (i = 0; i < length; i++)
            {
                var ai = a[i];
                var bi = b[i];
                if (ai != bi)
                {
                    if (ai == ConstByte.Comma)
                    {
                        return -1001; // `a' has a smaller table name.  a < b
                    }

                    if (bi == ConstByte.Comma)
                    {
                        return 1001;
                    }

                    return ai - bi;
                }

                if (ai == ConstByte.Comma)
                {
                    break;
                }
            }

            if (i == length)
            {
                throw new Exception($"No comma found in '{a.ToUtf8String()}' after offset {i}");
            }


            var aComma = Array.LastIndexOf(a, ConstByte.Comma);
            var bComma = Array.LastIndexOf(b, ConstByte.Comma);
            i++;
            var firstComma = Math.Min(aComma, bComma);

            for (; i < firstComma; i++)
            {
                var ai = a[i];
                var bi = b[i];
                if (ai != bi) return ai - bi;
            }

            if (aComma < bComma)
            {
                return -1002;
            }

            if (bComma < aComma)
            {
                return 1002;
            }

            for (; i < length; i++)
            {
                var ai = a[i];
                var bi = b[i];
                if (ai != bi) return ai - bi;
            }

            return a.Length - b.Length;
        }

        public bool IsRegionOverlap(RegionInfo other)
        {
            return BinaryComparer.Equals(Namespace, other.Namespace)
                    && BinaryComparer.Equals(Table, other.Table)
                    && (other.StopKey?.Any() != true || BinaryComparer.Compare(StartKey, other.StopKey) < 0)
                    && (StopKey?.Any() != true || BinaryComparer.Compare(StopKey, other.StartKey) > 0);
        }

        public override string ToString()
        {
            return $"RegionInfo->Name: {Name.ToUtf8String()}, ID: {ID}, Namespace: {Namespace.ToUtf8String()}, Table: {Table.ToUtf8String()}, StartKey:{StartKey.ToUtf8String()}, StopKey: {StopKey.ToUtf8String()}";
        }

        public static RegionInfo ParseFromCell(Cell cell)
        {
            var result = default(RegionInfo);
            if (cell?.HasValue != true) return result;
            var v = cell.Value.ToArray();
            if (v[0] != ConstByte.P) return result;
            var magic = BinaryPrimitives.ReadUInt32BigEndian(v);
            if (magic != ConstInt.PBufMagic) return result;
            var reg = v[4..^4].TryParseTo(Pb.RegionInfo.Parser.ParseFrom);
            if (reg == null) return result;
            var @namespace = reg.TableName.Namespace.ToArray();
            if (BinaryComparer.Compare(@namespace, ConstByte.DefaultNamespace) == 0)
            {
                @namespace = ConstByte.DefaultNamespace;
            }
            result = new RegionInfo(reg.RegionId, @namespace, reg.TableName.Qualifier.ToArray(), cell.Row.ToArray(),
                reg.StartKey.ToArray())
            {
                StopKey = reg.EndKey.ToArray()
            };
            return result;
        }

        public static RegionInfo ParseFromScanResponse(ScanResponse resp)
        {
            if (true != resp?.Results?.Any()) return null;

            var regCell = resp.Results.First().Cell.FirstOrDefault(t => t.Qualifier.ToStringUtf8().Equals(ConstString.RegionInfo));

            var reg = RegionInfo.ParseFromCell(regCell);
            if (reg == null) return null;

            var server = resp.Results.First().Cell.FirstOrDefault(t => t.Qualifier.ToStringUtf8().Equals(ConstString.Server) && t.HasValue);
            if (server == null) return null;
            var serverData = server.Value.ToArray();

            var idxColon = Array.IndexOf(serverData, ConstByte.Colon);
            if (idxColon < 1) return null;

            var host = serverData[..idxColon].ToUtf8String();
            if (false == ushort.TryParse(serverData[(idxColon + 1)..].ToUtf8String(), out var port))
                return null;
            reg.Host = host;
            reg.Port = port;
            return reg;
        }
    }
}