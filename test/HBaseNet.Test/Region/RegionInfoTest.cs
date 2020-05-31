using System.Collections.Concurrent;
using HBaseNet.Utility;
using NUnit.Framework;
using Pb;
using RegionInfo = HBaseNet.Region.RegionInfo;

namespace HBaseNet.Test.Region
{
    public class RegionInfoTest
    {
        [Test]
        public void TestCompare()
        {
            var testcases = new[]
            {
                new[]
                {
                    // Different table names.
                    "table,,1234567890".ToUtf8Bytes(), ".META.,,1234567890".ToUtf8Bytes()
                },
                new[]
                {
                    // Different table names but same prefix.
                    "tabl2,,1234567890".ToUtf8Bytes(), "tabl1,,1234567890".ToUtf8Bytes(),
                },
                new[]
                {
                    // Different table names (different lengths).
                    "table,,1234567890".ToUtf8Bytes(), "tabl,,1234567890".ToUtf8Bytes(),
                },
                new[]
                {
                    // Any key is greater than the start key.
                    "table,foo,1234567890".ToUtf8Bytes(), "table,,1234567890".ToUtf8Bytes(),
                },
                new[]
                {
                    // Different keys.
                    "table,foo,1234567890".ToUtf8Bytes(), "table,bar,1234567890".ToUtf8Bytes(),
                },
                new[]
                {
                    // Shorter key is smaller than longer key.
                    "table,fool,1234567890".ToUtf8Bytes(), "table,foo,1234567890".ToUtf8Bytes(),
                },
                new[]
                {
                    // Properly handle keys that contain commas.
                    "table,a,,c,1234567890".ToUtf8Bytes(), "table,a,,b,1234567890".ToUtf8Bytes(),
                },
                new[]
                {
                    // If keys are equal, then start code should break the tie.
                    "table,foo,1234567891".ToUtf8Bytes(), "table,foo,1234567890".ToUtf8Bytes(),
                },
                new[]
                {
                    // Make sure that a start code being a prefix of another is handled.
                    "table,foo,1234567890".ToUtf8Bytes(), "table,foo,123456789".ToUtf8Bytes(),
                },
                new[]
                {
                    // If both are start keys, then start code should break the tie.
                    "table,,1234567891".ToUtf8Bytes(), "table,,1234567890".ToUtf8Bytes(),
                },
                new[]
                {
                    // The value `:' is always greater than any start code.
                    "table,foo,:".ToUtf8Bytes(), "table,foo,9999999999".ToUtf8Bytes(),
                },
                new[]
                {
                    // Issue 27: searching for key "8,\001" and region key is "8".
                    "table,8,\u0001,:".ToUtf8Bytes(), "table,8,1339667458224".ToUtf8Bytes(),
                }
            };

            foreach (var testcase in testcases)
            {
                var a = RegionInfo.Compare(testcase[0], testcase[1]);
                var b = RegionInfo.Compare(testcase[1], testcase[0]);
                Assert.Greater(a, 0,
                    $"{testcase[0].ToUtf8String()} was found to be less than {testcase[1].ToUtf8String()} {a}");
                Assert.Less(b, 0,
                    $"{testcase[1].ToUtf8String()} was found to be less than {testcase[0].ToUtf8String()} {b}");
                var meta = "hbase:meta,,1".ToUtf8Bytes();
                var i = RegionInfo.Compare(meta, meta);
                Assert.Zero(i, $"{meta.ToUtf8String()} was found to not be equal to itself({i})");
            }
        }

        [Test]
        public void TestCompareBogusName()
        {
            var action = new TestDelegate(() => { RegionInfo.Compare("bogus".ToUtf8Bytes(), "bogus".ToUtf8Bytes()); });
            var exception = Assert.Catch(action);
            const string expectedMsg = "No comma found in 'bogus' after offset 5";
            var actualMsg = exception.Message;
            Assert.AreEqual(expectedMsg, actualMsg);
        }
    }
}