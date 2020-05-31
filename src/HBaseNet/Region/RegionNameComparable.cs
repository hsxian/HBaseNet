using System;
using System.Collections.Generic;

namespace HBaseNet.Region
{
    public class RegionNameComparer : IComparer<byte[]>
    {
        public int Compare(byte[] x, byte[] y)
        {
            return RegionInfo.Compare(y, x);
        }
    }
}