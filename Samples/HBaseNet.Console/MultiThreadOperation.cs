using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using BitConverter;
using HBaseNet.Console.Models;
using HBaseNet.HRpc;
using HBaseNet.Metadata.Conventions;
using Pb;

namespace HBaseNet.Console
{
    public class MultiThreadOperation
    {
        private readonly IStandardClient _client;

        public MultiThreadOperation(IStandardClient client)
        {
            _client = client;
        }

        public async Task ExecPut(int count)
        {
            var tasks = new List<Task<MutateResponse>>();
            var convertCache = new ConvertCache().BuildCache<Student>(EndianBitConverter.BigEndian);
            for (var i = 0; i < count; i++)
            {
                var student = Program.StudentFaker.Generate();
                var values = HBaseConvert.Instance.ConvertToDictionary(student, convertCache);
                var rowKey = Program.GenerateRandomKey();
                var rs = _client.Put(new MutateCall(Program.Table, rowKey, values)
                {
                    Timestamp = new DateTime(2019, 1, 1, 1, 1, 1),
                });
                tasks.Add(rs);
            }

            await Task.WhenAll(tasks);
        }
    }
}