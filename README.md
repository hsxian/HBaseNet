# HBaseNet

[![NuGet Badge](https://buildstats.info/nuget/hbasenet)](https://www.nuget.org/packages/HBaseNet)

This is a pure CSharp client for HBase.

## Supported Versions

HBase >= 1.0

## Example Usage

### Create a client

```csharp
var ZkQuorum = "zooKeep-host-ip";
var admin = await new AdminClient(ZkQuorum).Build();
if (admin == null) return;
var client = await new StandardClient(ZkQuorum).Build();
if (client == null) return;
```

### Admin operation

```csharp
var table = "student";
var cols = new[]
{
    new ColumnFamily("info")
    {
        Compression = Compression.GZ,
        KeepDeletedCells = KeepDeletedCells.TRUE
    },
    new ColumnFamily("special")
    {
        Compression = Compression.GZ,
        KeepDeletedCells = KeepDeletedCells.TTL,
        DataBlockEncoding = DataBlockEncoding.PREFIX
    }
};
var create = new CreateTableCall(table, cols)
{
    SplitKeys = new[] { "0", "5" }
};
var listTable = new ListTableNamesCall();
var disable = new DisableTableCall(table);
var delete = new DeleteTableCall(table);

var ct = await admin.CreateTable(create);

var tables = await admin.ListTableNames(listTable);

var dt = await admin.DisableTable(disable);

var del = await admin.DeleteTable(delete);
```

### Generally operation

```csharp
var table = "student";

// put
var rowKey = "123";
var values = new Dictionary<string, IDictionary<string,byte[]>>
{
    {
        "default", new Dictionary<string, byte[]>
        {
            {"key", "value".ToUtf8Bytes()}
        }
    }
};
var rs = await client.Put(new MutateCall(table, rowKey, values));

// scan
var sc = new ScanCall(table, "1", "")
{
    NumberOfRows = 100000
};
using var scanner = client.Scan(sc);
var scanResults = new List<Result>();
while (scanner.CanContinueNext)
{
    var per = await scanner.Next();
    if (true != per?.Any()) continue;
    scanResults.AddRange(per);
}

// get
var getResult = await client.Get(new GetCall(table, rowKey));

// delete
var delResult = await client.Delete(new MutateCall(table, rowKey, null));

```

### Use conversion tools

```csharp
public class Student
{
    public string Name { get; set; }
    public string Address { get; set; }
    public int Age { get; set; }
    public float Score { get; set; }
    public bool? IsMarried { get; set; }
    [HBaseConverter(typeof(DateTimeUnix13Converter))]
    public DateTime Create { get; set; }
    [HBaseProperty(family: "special")]
    [HBaseConverter(typeof(DateTimeUnix13Converter))]
    public DateTime? Modify { get; set; }
    [HBaseConverter(typeof(JsonStringConverter))]
    public List<string> Courses { get; set; }
}


var convertCache = new ConvertCache().BuildCache<Student>(EndianBitConverter.BigEndian);

var student = new Student
{
    Name = "Anna",
    Age = 20,
    Address = "Yuxi, China",
    Score = 99,
    IsMarried = true,
    Create = DateTime.Now,
    Courses = new List<string> { "Mathematics", "physics", "art" }
};

//object convert to values 
var values = HBaseConvert.Instance.ConvertToDictionary(student, convertCache);
  var rs = await client.Put(new MutateCall(Program.Table, rowKey, values));

//scan result convert to object of student
using var scanner = client.Scan(sc);
var scanResults = new List<Student>();
while (scanner.CanContinueNext)
{
    var per = await scanner.Next();
    if (true != per?.Any()) continue;
    var stus = HBaseConvert.Instance.ConvertToCustom<Student>(per, convertCache);
    scanResults.AddRange(stus);
}                   
```

You can also refer to the "[Samples/HBaseNet.Console](Samples/HBaseNet.Console/Program.cs)" project.
