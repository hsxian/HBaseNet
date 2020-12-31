using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BitConverter;
using Bogus;
using Bogus.Extensions;
using HBaseNet.Console.Models;
using HBaseNet.Const;
using HBaseNet.HRpc;
using HBaseNet.Metadata.Conventions;
using HBaseNet.Utility;
using Pb;
using Serilog;

namespace HBaseNet.Console
{
    public class SingleThreadOperation
    {
        private readonly IStandardClient _client;

        public SingleThreadOperation(IStandardClient client)
        {
            _client = client;
            Randomizer.Seed = new Random(DateTime.Now.Millisecond);
        }

        public async Task ExecPut(int count)
        {
            var convertCache = new ConvertCache().BuildCache<Student>(EndianBitConverter.BigEndian);
            for (var i = 0; i < count; i++)
            {
                var rowKey = Program.GenerateRandomKey();
                var student = Program.StudentFaker.Generate();
                var values = HBaseConvert.Instance.ConvertToDictionary(student, convertCache);
                var rs = await _client.Put(new MutateCall(Program.Table, rowKey, values));
            }
        }

        public async Task ExecScan()
        {
            var sc = new ScanCall(Program.Table, "", "g")
            {
                // Families = Program.Family,
                // TimeRange = new TimeRange
                // {
                //     From = new DateTime(2018, 1, 1).ToUnixU13(),
                //     To = new DateTime(2019, 1, 2).ToUnixU13()
                // },
                NumberOfRows = 100000,
            };
            using var scanner = _client.Scan(sc);
            var scanResults = new List<Student>();
            var sth = Stopwatch.StartNew();
            var convertCache = new ConvertCache().BuildCache<Student>(EndianBitConverter.BigEndian);
            while (scanner.CanContinueNext)
            {
                sth.Restart();
                var per = await scanner.Next();
                var reqSpan = sth.Elapsed;
                if (true != per?.Any()) continue;
                sth.Restart();
                var stus = HBaseConvert.Instance.ConvertToCustom<Student>(per, convertCache);
                Debug.WriteLine($"scanner count:{per.Count}, elapesd: {reqSpan}, convert elapesad: {sth.Elapsed}.");
                scanResults.AddRange(stus);
            }

            Log.Information($"scan 'student' count:{scanResults.Count}");
        }

        public async Task ExecScanAndDelete()
        {
            using var scanner = _client.Scan(new ScanCall(Program.Table, "", "g") { NumberOfRows = 3 });
            var scanResults = await scanner.Next();
            if (null == scanResults) return;
            Log.Information($"scan result will delete, count:{scanResults.Count}");
            foreach (var result in scanResults)
            {
                var rowKey = result.Cell.Select(t => t.Row.ToStringUtf8()).FirstOrDefault();
                if (rowKey == null) continue;
                var delResult = await _client.Delete(new MutateCall(Program.Table, rowKey, null));
                Log.Logger.Information($"delete row at key: {rowKey}, processed:{delResult.Processed}");
            }
        }

        public async Task ExecAppend()
        {
            var rowKey = Program.GenerateRandomKey();
            var rs = await _client.Put(new MutateCall(Program.Table, rowKey, Program.Values));
            var v = new Dictionary<string, Dictionary<string, byte[]>>
            {
                {
                    ConstString.DefaultFamily, new Dictionary<string, byte[]>
                    {
                        {"key", " append".ToUtf8Bytes()}
                    }
                }
            };
            rs = await _client.Append(new MutateCall(Program.Table, rowKey, v));
            var upRs = await _client.Get(new GetCall(Program.Table, rowKey));
            rs = await _client.Append(new MutateCall(Program.Table, rowKey, ConstString.DefaultFamily, "key", " append"));
            upRs = await _client.Get(new GetCall(Program.Table, rowKey));
            var newV = upRs.Result.Cell[0].Value.ToStringUtf8();
            if ("value append append" == newV)
            {
                Log.Information($"append at key:{rowKey} ,success");
            }
            else
            {
                Log.Information($"append at key:{rowKey} ,failed");
            }
        }

        public async Task ExecIncrement()
        {
            var rowKey = Program.GenerateRandomKey();
            var v = new Dictionary<string, Dictionary<string, byte[]>>
            {
                {
                    ConstString.DefaultFamily, new Dictionary<string, byte[]>
                    {
                        {"key", null}
                    }
                }
            };
            v[ConstString.DefaultFamily]["key"] = EndianBitConverter.BigEndian.GetBytes(1L);
            var rs = await _client.Increment(new MutateCall(Program.Table, rowKey, v));
            rs = await _client.Increment(new MutateCall(Program.Table, rowKey, ConstString.DefaultFamily, "key", 5L));
            if (6 == rs)
            {
                Log.Information($"increment at key:{rowKey} ,success");
            }
            else
            {
                Log.Information($"increment at key:{rowKey} ,failed");
            }
        }

        public async Task ExecCheckAndPut()
        {
            var rowKey = Program.GenerateRandomKey();
            var put = new MutateCall(Program.Table, rowKey, Program.Values);
            var result = await _client.CheckAndPut(put, ConstString.DefaultFamily, "key", null, new CancellationToken());
            Log.Information($"check and put key:{rowKey},result:{result}");
            result = await _client.CheckAndPut(put, ConstString.DefaultFamily, "key", null, new CancellationToken());
            Log.Information($"check and put key:{rowKey} again,result:{result}");
        }
    }
}