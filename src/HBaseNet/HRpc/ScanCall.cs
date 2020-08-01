using System.Collections.Generic;
using System.Linq;
using Google.Protobuf;
using HBaseNet.Utility;
using Pb;

namespace HBaseNet.HRpc
{
    public class ScanCall : BaseCall
    {
        public IDictionary<string, string[]> Families { get; set; }
        public byte[] StartRow { get; set; }
        public byte[] StopRow { get; set; }
        public bool CloseScanner { get; set; }
        public ulong? ScannerID { get; set; }
        public Filter.IFilter Filters { get; set; }
        public TimeRange TimeRange { get; set; }
        public uint MaxVersions { get; set; } = 1;
        public uint NumberOfRows { get; set; } = 128;
        public bool AllowPartialResults { get; set; }
        public bool Reversed { get; set; }

        public ScanCall(string table, string startRow, string stopRow) : this(table.ToUtf8Bytes(), startRow?.ToUtf8Bytes(), stopRow?.ToUtf8Bytes())
        {
        }

        public ScanCall(byte[] table, byte[] startRow, byte[] stopRow)
        {
            Table = table;
            StartRow = startRow;
            StopRow = stopRow;
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
                    StartRow = ByteString.CopyFrom(StartRow ?? new byte[0]),
                    StopRow = ByteString.CopyFrom(StopRow ?? new byte[0]),
                    Filter = Filters?.ConvertToPBFilter(),
                    MaxVersions = MaxVersions,
                    TimeRange = TimeRange,
                    Reversed = Reversed
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