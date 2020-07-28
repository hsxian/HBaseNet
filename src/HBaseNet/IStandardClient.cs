using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using HBaseNet.HRpc;
using Pb;

namespace HBaseNet
{
    public interface IStandardClient : IDisposable
    {
        Task<bool> CheckTable(string table, CancellationToken? token = null);
        Task<List<Result>> Scan(ScanCall scan, CancellationToken? token = null);
        Task<GetResponse> Get(GetCall get, CancellationToken? token = null);
        Task<MutateResponse> Put(MutateCall put, CancellationToken? token = null);
        Task<MutateResponse> Delete(MutateCall del, CancellationToken? token = null);
        Task<MutateResponse> Append(MutateCall apd, CancellationToken? token = null);
        Task<long?> Increment(MutateCall inc, CancellationToken? token = null);
        Task<bool> CheckAndPut(MutateCall put, string family, string qualifier, byte[] expectedValue, CancellationToken? token = null);
        Task<TResponse> SendRPCToRegion<TResponse>(ICall rpc, CancellationToken? token) where TResponse : class, IMessage;
    }
}