using System;

namespace HBaseNet.Metadata.Annotations
{
    public class HBaseConverterAttribute : Attribute
    {
        public HBaseConverterAttribute(Type binaryConvert, params object[] converterParameters)
        {
            BinaryConverter = binaryConvert;
            ConverterParameters = converterParameters;
        }
        public Type BinaryConverter { get; set; }
        public object[] ConverterParameters { get; set; }
    }
}