namespace HBaseNet.Filter
{
    public interface IFilter
    {
        string Name { get; }
        Pb.Filter ConvertToPBFilter();
    }
}