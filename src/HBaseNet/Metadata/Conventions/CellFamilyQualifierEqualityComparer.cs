using System;
using System.Collections.Generic;
using Pb;
using System.Linq;
using HBaseNet.Utility;

namespace HBaseNet.Metadata.Conventions
{
    public class CellFamilyQualifierEqualityComparer : IEqualityComparer<Cell>
    {
        public bool Equals(Cell x, Cell y)
        {
            return x.Qualifier.SequenceEqual(y.Qualifier) && x.Family.SequenceEqual(y.Family);
        }

        public int GetHashCode(Cell obj)
        {
            return obj.Family.Concat(obj.Qualifier).GetBinaryHashCode();
        }
    }
}