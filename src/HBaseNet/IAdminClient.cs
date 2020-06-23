using System;
using System.Threading;
using System.Threading.Tasks;
using HBaseNet.HRpc;

namespace HBaseNet
{
    public interface IAdminClient:IDisposable
    {
        Task<bool> CreateTable(CreateTableCall t, CancellationToken? token = null);
        Task<bool> DeleteTable(DeleteTableCall t, CancellationToken? token = null);
        Task<bool> EnableTable(EnableTableCall t, CancellationToken? token = null);
        Task<bool> DisableTable(DisableTableCall t, CancellationToken? token = null);
        
    }
}