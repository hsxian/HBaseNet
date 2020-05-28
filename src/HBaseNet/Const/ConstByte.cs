namespace HBaseNet.Const
{
    public class ConstByte
    {
        public const byte Comma = (byte) ',';

        /// <summary>
        /// ':' is the first byte greater than '9'.  We always want to find the
        /// entry with the greatest timestamp, so by looking right before ':'
        /// we'll find it.
        /// </summary>
        public const byte Colon = (byte) ':';
        public const byte P = (byte) 'P';
    }
}