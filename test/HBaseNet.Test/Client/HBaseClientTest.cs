using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using NUnit.Framework;

namespace HBaseNet.Test.Client
{
    public class HBaseClientTest
    {
        [Test]
        public async Task TestHBaseClient()
        {
            var ip = $"hbase-docker";
            var client = new HBaseClient(ip);
            var rs = await client.Get(
                "student",
                "1590241094",
                new Dictionary<string, string[]> {{"default", new[] {""}}}
            );
        }
    }
}