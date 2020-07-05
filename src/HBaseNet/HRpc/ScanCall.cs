using System;
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
        public IDictionary<string, string[]> Families { get; set; }
        public byte[] StartRow { get; set; }
        public byte[] StopRow { get; set; }
        private bool CloseScanner { get; }
        private ulong? ScannerID { get; }
        public Filter.IFilter Filters { get; set; }
        public TimeRange TimeRange { get; set; }
        public uint MaxVersions { get; set; } = 1;
        public uint NumberOfRows { get; set; } = 128;

        public ScanCall(string table, IDictionary<string, string[]> families, byte[] startRow, byte[] stopRow)
        {
            StartRow = startRow;
            StopRow = stopRow;
            Families = families;
            Table = table.ToUtf8Bytes();
            Key = startRow;
        }

        public ScanCall(byte[] table, IDictionary<string, string[]> families, byte[] startRow, byte[] stopRow)
        {
            StartRow = startRow;
            StopRow = stopRow;
            Families = families;
            Table = table;
            Key = startRow;
        }

        public ScanCall(byte[] table, ulong? scannerID, byte[] startRow, bool closeScanner)
        {
            ScannerID = scannerID;
            CloseScanner = closeScanner;
            Table = table;
            Key = startRow;
        }

        public override string Name => "Scan";

        public override byte[] Serialize()
        {
            var scan = new ScanRequest
            {
                Region = GetRegionSpecifier(),
                CloseScanner = CloseScanner,
                NumberOfRows = NumberOfRows
            };
            if (ScannerID == null)
            {
                scan.Scan = new Scan
                {
                    StartRow = ByteString.CopyFrom(StartRow),
                    StopRow = ByteString.CopyFrom(StopRow),
                    Filter = Filters?.ConvertToPBFilter(),
                    MaxVersions = MaxVersions,
                    TimeRange = TimeRange
                };
                var cols = ConvertToColumns(Families);
                if (cols?.Any() == true)
                {
                    scan.Scan.Column.AddRange(cols);
                }
            }
            else
            {
                scan.ScannerId = ScannerID.Value;
            }

            return scan.ToByteArray();
        }

        public override IMessage ParseResponseFrom(byte[] bts)
        {
            return bts.TryParseTo(ScanResponse.Parser.ParseFrom);
        }
    }
}