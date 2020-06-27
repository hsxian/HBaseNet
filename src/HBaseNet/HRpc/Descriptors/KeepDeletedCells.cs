namespace HBaseNet.HRpc.Descriptors
{
    public enum KeepDeletedCells
    {
        /// <summary>
        ///  Deleted Cells are not retained.
        /// </summary>
        FALSE,

        /// <summary>
        /// Deleted Cells are retained until they are removed by other means
        /// such TTL or VERSIONS.
        /// If no TTL is specified or no new versions of delete cells are
        /// written, they are retained forever.
        /// </summary>
        TRUE,

        /// <summary>
        /// Deleted Cells are retained until the delete marker expires due to TTL.
        /// This is useful when TTL is combined with MIN_VERSIONS and one
        /// wants to keep a minimum number of versions around but at the same
        /// time remove deleted cells after the TTL.
        /// </summary>
        TTL
    }
}