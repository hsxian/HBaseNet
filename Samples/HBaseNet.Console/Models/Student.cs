using System;
using System.Collections.Generic;
using HBaseNet.Metadata.Annotations;
using HBaseNet.Metadata.Conventions;

namespace HBaseNet.Console.Models
{
    public class Student
    {
        public string Name { get; set; }
        public string Address { get; set; }
        public int Age { get; set; }
        public float Score { get; set; }
        [HBaseConverter(typeof(DateTimeUnix13Converter))]
        public DateTime Create { get; set; }
        [HBaseConverter(typeof(DateTimeUnix13Converter))]
        public DateTime? Modify { get; set; }
        [HBaseConverter(typeof(JsonStringConverter))]
        public List<string> Courses { get; set; }
    }
}