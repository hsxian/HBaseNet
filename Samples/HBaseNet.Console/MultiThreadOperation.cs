using System;
using System.Collections.Generic;
using System.Threading.Tasks;
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
            for (var i = 0; i < count; i++)
            {
                var rowKey = Program.GenerateRandomKey();
                var student = Program.StudentFaker.Generate();
                var values = HBaseConvert.Instance.ConvertToDictionary(student);
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