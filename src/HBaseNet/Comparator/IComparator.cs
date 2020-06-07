namespace HBaseNet.Comparator
{
    public interface IComparator
    {
        string Name { get; }
        Pb.ByteArrayComparable Comparable { get; }
        Pb.Comparator ConvertToPBComparator();
    }
}