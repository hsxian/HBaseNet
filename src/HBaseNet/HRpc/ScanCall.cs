using System.Collections.Generic;
using System.Linq;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using HBaseNet.Utility;
using Pb;

namespace HBaseNet.HRpc
{
    public class ScanCall : BaseCall
    {
        public IDictionary<string, string[]> Families { get; }
        public byte[] StartRow { get; }
        public byte[] StopRow { get; }
        public bool CloseScanner { get; }
        public ulong? ScannerID { get; }
        public byte[] RegionStop { get; private set; }

        public ScanCall(string table, IDictionary<string, string[]> families, byte[] startRow, byte[] stopRow)
        {
            Families = families;
            StartRow = startRow;
            StopRow = stopRow;
            Table = table.ToUtf8Bytes();
            Key = startRow;
        }

        public ScanCall(string table, ulong? scannerID, byte[] startRow, bool closeScanner)
        {
            ScannerID = scannerID;
            StartRow = startRow;
            CloseScanner = closeScanner;
            Table = table.ToUtf8Bytes();
            Key = startRow;
        }

        public override void SetRegion(byte[] region, byte[] regionStop)
        {
            RegionStop = regionStop;
            base.SetRegion(region, regionStop);
        }

        public override string Name => "Scan";

        public override byte[] Serialize()
        {
            var scan = new ScanRequest
            {
                Region = GetRegionSpecifier(),
                CloseScanner = CloseScanner,
                NumberOfRows = new UInt32Value {Value = 20}.Value //TODO:应该使用配置
            };
            if (ScannerID == null)
            {
                scan.Scan = new Scan
                {
                    StartRow = ByteString.CopyFrom(StartRow),
                    StopRow = ByteString.CopyFrom(StopRow),
                };
                var cols = ConvertToColumns(Families);
                scan.Scan.Column.AddRange(cols);
            }
            else
            {
                scan.ScannerId = ScannerID.Value;
            }

            return scan.ToByteArray();
        }

        public override IMessage ResponseParseFrom(byte[] bts)
        {
            return bts.TryParseTo(ScanResponse.Parser.ParseFrom);
        }
    }
}