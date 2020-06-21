using System;
using HBaseNet.HRpc;

namespace HBaseNet.Region
{
    internal class RPCSend
    {
        public DateTime QueueTime { get; set; }
        public ICall RPC { get; set; }
    }
}