using System;
using System.Runtime.CompilerServices;
using NUnit.Framework;
using HBaseNet.Utility;
using System.Collections.Generic;
using HBaseNet.Filter;
using HBaseNet.HRpc;
using CSharpTest.Net.IO;
using System.Linq;

namespace HBaseNet.Test.HRpc
{
    public class GetCallTest
    {
        [Test]
        public void TestNewGet()
        {
            var table = "table";
            var tableB = table.ToUtf8Bytes();
            var key = "123";
            var keyB = key.ToUtf8Bytes();
            var family = new Dictionary<string, string[]> { { "info", new[] { "cl" } } };
            var filter = new FirstKeyOnlyFilter();
            var call = new GetCall(tableB, keyB);
            Assert.IsTrue(ConfirmGetProperties(call, tableB, keyB), "GetCall didn't set properties correctly.");

            call = new GetCall(table, key);
            Assert.IsTrue(ConfirmGetProperties(call, tableB, keyB), "GetCall didn't set properties correctly.");

            call = new GetCall(tableB, keyB)
            {
                Families = family
            };
            Assert.IsTrue(ConfirmGetProperties(call, tableB, keyB, family), "GetCall didn't set properties correctly.");
        }
        private bool ConfirmGetProperties(GetCall call, byte[] table, byte[] key)
        {
            if (false == BinaryComparer.Equals(call.Table, table)
            || false == BinaryComparer.Equals(call.Key, key)
            ) return false;
            return true;
        }


        private bool ConfirmGetProperties(GetCall call, byte[] table, byte[] key, IDictionary<string, string[]> family)
        {
            if (false == ConfirmGetProperties(call, table, key)
            || false == call.Families.OrderBy(r => r.Key).SequenceEqual(family.OrderBy(r => r.Key))
            ) return false;
            return true;
        }
        private bool ConfirmGetProperties(GetCall call, byte[] table, byte[] key, IDictionary<string, string[]> family, IFilter filter)
        {
            if (false == ConfirmGetProperties(call, table, key, family)
            || false == Type.Equals(call.Filters, filter)
            ) return false;
            return true;
        }
    }
}