using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using HBaseNet.Const;
using HBaseNet.Utility;
using Pb;

namespace HBaseNet.Region
{
    public class RegionInfo
    {
        public byte[] Table { get; set; }
        public byte[] RegionName { get; set; }
        public byte[] StopKey { get; set; }

        public class RegionKeyEqualityComparer : IEqualityComparer<byte[]>
        {
            public bool Equals(byte[] x, byte[] y)
            {
                return Compare(x, y) == 0;
            }

            public int GetHashCode(byte[] obj)
            {
                return Compare(new byte[obj.Length].Initialize(ConstByte.Comma), obj);
            }
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

        public override string ToString()
        {
            return $"*region.Info:Table: {Table}, RegionName: {RegionName}, StopKey: {StopKey}";
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
                RegionName = cell.Row.ToArray(),
                StopKey = reg.EndKey.ToArray()
            };
            return result;
        }
    }
}