using Google.Protobuf;
using RegionInfo = HBaseNet.Region.RegionInfo;

namespace HBaseNet.HRpc
{
    public interface ICall
    {
        uint CallId { get; set; }
        uint RetryCount { get; set; }
        uint FindRegionRetryCount { get; set; }
        byte[] Namespace { get; }
        byte[] Table { get; }
        byte[] Key { get; }
        string Name { get; }
        RegionInfo Info { get; set; }
        byte[] Serialize();
        IMessage ParseResponseFrom(byte[] bts);
    }
}