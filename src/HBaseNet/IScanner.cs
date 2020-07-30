using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Pb;

namespace HBaseNet
{
    public interface IScanner : IDisposable
    {
        Task<List<Result>> Next();
    }
}