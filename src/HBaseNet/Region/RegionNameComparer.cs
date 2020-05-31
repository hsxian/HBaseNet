using System;
using System.Collections.Generic;

namespace HBaseNet.Region
{
    public class RegionNameComparer : IComparer<byte[]>
    {
        public int Compare(byte[] x, byte[] y)
        {
            //This will allow the b plus tree to be arranged in reverse order.
            return RegionInfo.Compare(y, x);
        }
    }
}