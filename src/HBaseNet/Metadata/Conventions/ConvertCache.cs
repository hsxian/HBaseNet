using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using BitConverter;
using Google.Protobuf;
using HBaseNet.Const;
using HBaseNet.Metadata.Annotations;
using HBaseNet.Utility;

namespace HBaseNet.Metadata.Conventions
{
    public class ConvertCache
    {
        public Dictionary<ByteString, Dictionary<ByteString, ConvertItem>> ReaderCache { get; set; }
        public ConvertItem[] WriterCache { get; set; }
        public ConvertItem[] AllItems { get; set; }

        public ConvertCache BuildCache<T>(EndianBitConverter bitConverter = null)
        {
            bitConverter ??= EndianBitConverter.BigEndian;
            AllItems = typeof(T).GetProperties()
                      .Select(prop =>
                      {
                          var ret = new ConvertItem
                          {
                              Property = prop,
                              Family = ConstByte.DefaultFamily,
                              Qualifier = prop.Name.ToUtf8Bytes()
                          };
                          var hbpa = prop.GetCustomAttribute<HBasePropertyAttribute>();
                          if (hbpa != null)
                          {
                              if (hbpa.IsIgnore) return null;
                              ret.Family = hbpa.Family ?? ret.Family;
                              ret.Qualifier = hbpa.Qualifier ?? ret.Qualifier;
                          }
                          ret.Family2 = ret.Family.ToUtf8String();
                          ret.Qualifier2 = ret.Qualifier.ToUtf8String();
                          ret.ReadConvert = GetReadMethod(prop, bitConverter);
                          ret.WriteConvert = GetWriteMethod(prop, bitConverter);
                          if (ret.ReadConvert == null && ret.WriteConvert == null) return null;
                          return ret;
                      })
                      .Where(t => t != null)
                      .ToArray()
                      ;

            ReaderCache = AllItems
            .Where(t => t.ReadConvert != null)
            .GroupBy(t => t.Family, ByteArrayComparer.Default)
            .ToDictionary(
                t => ByteString.CopyFrom(t.Key),
                t => t.ToDictionary(tt => ByteString.CopyFrom(tt.Qualifier), tt => tt, ByteStringComparer.Default),
                ByteStringComparer.Default
                );

            WriterCache = AllItems
            .Where(t => t.WriteConvert != null)
            .ToArray();

            return this;
        }
        private Converter<ByteString, object> GetReadMethod(PropertyInfo prop, EndianBitConverter bitConverter)
        {
            if (prop.CanRead == false) return null;

            var hca = prop.GetCustomAttribute<HBaseConverterAttribute>();
            if (hca?.BinaryConverter != null)
            {
                var read = hca.BinaryConverter.GetMethod(nameof(IByteArrayConverter.Read));
                if (read == null) return null;
                var reader = Activator.CreateInstance(hca.BinaryConverter);
                return b => read.Invoke(reader, new object[] { b.ToArray(), prop.PropertyType, hca.ConverterParameters });
            }

            if (prop.PropertyType == typeof(string))
            {
                return b => b.ToStringUtf8();
            }

            var propertyType = prop.PropertyType.Name == "Nullable`1" ? prop.PropertyType.GenericTypeArguments.FirstOrDefault() : prop.PropertyType;
            if (propertyType == null) return null;

            var method = bitConverter.GetType().GetMethods().FirstOrDefault(t =>
                 t.Name.StartsWith("To") &&
                 t.ReturnParameter.ParameterType == propertyType);
            if (method == null) return null;

            return b => method.Invoke(bitConverter, new object[] { b.ToByteArray(), 0 });
        }

        private Converter<object, byte[]> GetWriteMethod(PropertyInfo prop, EndianBitConverter bitConverter)
        {
            if (prop.CanWrite == false) return null;

            var hbca = prop.GetCustomAttribute<HBaseConverterAttribute>();
            if (hbca != null)
            {
                var writer = Activator.CreateInstance(hbca.BinaryConverter);
                var write = hbca.BinaryConverter.GetMethod(nameof(IByteArrayConverter.Write));
                if (write == null) return null;
                return v => write.Invoke(writer, new[] { v, hbca.ConverterParameters }) as byte[];
            }

            if (prop.PropertyType == typeof(string))
            {
                return v => v.ToString().ToUtf8Bytes();
            }

            var propertyType = prop.PropertyType.Name == "Nullable`1" ? prop.PropertyType.GenericTypeArguments.FirstOrDefault() : prop.PropertyType;
            if (propertyType == null) return null;

            var method = bitConverter.GetType().GetMethods().FirstOrDefault(t =>
                 t.Name == nameof(EndianBitConverter.GetBytes) &&
                 t.GetParameters().FirstOrDefault()?.ParameterType == propertyType);
            if (method == null) return null;

            return v => method.Invoke(bitConverter, new[] { v }) as byte[];
        }
    }
}