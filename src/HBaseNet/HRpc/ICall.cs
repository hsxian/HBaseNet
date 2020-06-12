using System;
using Google.Protobuf;
using Pb;
using RegionInfo = HBaseNet.Region.RegionInfo;

namespace HBaseNet.HRpc
{
    public interface ICall
    {
        byte[] Table { get; }
        byte[] Key { get; }
        string Name { get; }
        RegionInfo Info { get; set; }
        byte[] Serialize();
        IMessage ParseResponseFrom(byte[] bts);
    }
}