using System;
using System.Buffers.Binary;
using System.Linq;
using System.Threading;
using CSharpTest.Net.IO;
using HBaseNet.Const;
using HBaseNet.Utility;
using Pb;

namespace HBaseNet.Region
{
    public class RegionInfo
    {
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

        public static byte[] CreateRegionSearchKey(byte[] table, byte[] key)
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
            return BinaryComparer.Equals(Table, other.Table)
                   && BinaryComparer.Compare(StartKey, other.StopKey) < 0
                   && BinaryComparer.Compare(StopKey, other.StartKey) > 0;
        }

        public override string ToString()
        {
            return
                $"RegionInfo->Table: {Table.ToUtf8String()}, RegionName: {Name.ToUtf8String()}, StartKey:{StartKey.ToUtf8String()}, StopKey: {StopKey.ToUtf8String()}";
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
            result = new RegionInfo
            {
                Table = reg.TableName.Qualifier.ToArray(),
                Name = cell.Row.ToArray(),
                StartKey = reg.StartKey.ToArray(),
                StopKey = reg.EndKey.ToArray()
            };
            return result;
        }

        public static RegionInfo ParseFromGetResponse(GetResponse metaRow)
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