using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Pb;

namespace HBaseNet
{
    public interface IScanner : IDisposable
    {
        bool CanContinueNext { get; }
        Task<List<Result>> Next();
        Task Close();
    }
}