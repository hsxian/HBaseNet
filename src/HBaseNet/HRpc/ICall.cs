using System;
using Google.Protobuf;
using Pb;

namespace HBaseNet.HRpc
{
    public interface ICall
    {
        byte[] Table { get; }
        byte[] Key { get; }
        string Name { get; }
        byte[] RegionName { get; }
        void SetRegion(byte[] region, byte[] regionStop);
        byte[] Serialize();
        IMessage ResponseParseFrom(byte[] bts);
    }
}