namespace HBaseNet.HRpc.Descriptors
{
    public enum DataBlockEncoding
    {
        /// <summary>
        /// Disable data block encoding.
        /// </summary>
        NONE,
        /// <summary>
        /// reserved for the BITSET algorithm to be added later
        /// </summary>
        PREFIX,
        DIFF,
        FAST_DIFF,
        ROW_INDEX_V1
    }
}