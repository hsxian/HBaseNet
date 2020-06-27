using Google.Protobuf;
using HBaseNet.Utility;
using Pb;

namespace HBaseNet.HRpc.Descriptors
{
    public class ColumnFamily
    {
        public byte[] Name { get; }
        public int MaxVersions { get; set; } = 3;
        public int TTL { get; set; } = int.MaxValue;
        public int MinVersion { get; set; } = 0;
        public KeepDeletedCells KeepDeletedCells { get; set; }
        public int BlockSize { get; set; } = 65536;
        public bool IsInMemory { get; set; }
        public Compression Compression { get; set; }
        public bool IsBlockCache { get; set; }
        public BloomFilter BloomFilter { get; set; }
        public int ReplicationScope { get; set; }
        public DataBlockEncoding DataBlockEncoding { get; set; } = DataBlockEncoding.FAST_DIFF;

        public ColumnFamily(byte[] name)
        {
            Name = name;
        }

        public ColumnFamily(string name) : this(name.ToUtf8Bytes())
        {
        }


        public ColumnFamilySchema ToPbSchema()
        {
            var schema = new ColumnFamilySchema
            {
                Name = ByteString.CopyFrom(Name)
            };
            schema.Attributes.Add(new BytesBytesPair
            {
                First = ByteString.CopyFromUtf8("VERSIONS"),
                Second = ByteString.CopyFromUtf8(MaxVersions.ToString())
            });
            schema.Attributes.Add(new BytesBytesPair
            {
                First = ByteString.CopyFromUtf8("TTl"),
                Second = ByteString.CopyFromUtf8(TTL.ToString())
            });
            schema.Attributes.Add(new BytesBytesPair
            {
                First = ByteString.CopyFromUtf8("MIN_VERSIONS"),
                Second = ByteString.CopyFromUtf8(MinVersion.ToString())
            });
            schema.Attributes.Add(new BytesBytesPair
            {
                First = ByteString.CopyFromUtf8("KEEP_DELETED_CELLS"),
                Second = ByteString.CopyFromUtf8(KeepDeletedCells.ToString())
            });
            schema.Attributes.Add(new BytesBytesPair
            {
                First = ByteString.CopyFromUtf8("BLOCKSIZE"),
                Second = ByteString.CopyFromUtf8(BlockSize.ToString())
            });

            schema.Attributes.Add(new BytesBytesPair
            {
                First = ByteString.CopyFromUtf8("COMPRESSION"),
                Second = ByteString.CopyFromUtf8(Compression.ToString())
            });
            schema.Attributes.Add(new BytesBytesPair
            {
                First = ByteString.CopyFromUtf8("BLOCKCACHE"),
                Second = ByteString.CopyFromUtf8(IsBlockCache.ToString())
            });
            schema.Attributes.Add(new BytesBytesPair
            {
                First = ByteString.CopyFromUtf8("BLOOMFILTER"),
                Second = ByteString.CopyFromUtf8(BloomFilter.ToString())
            });
            schema.Attributes.Add(new BytesBytesPair
            {
                First = ByteString.CopyFromUtf8("REPLICATION_SCOPE"),
                Second = ByteString.CopyFromUtf8(ReplicationScope.ToString())
            });
            schema.Attributes.Add(new BytesBytesPair
            {
                First = ByteString.CopyFromUtf8("DATA_BLOCK_ENCODING"),
                Second = ByteString.CopyFromUtf8(DataBlockEncoding.ToString())
            });

            return schema;
        }
    }
}