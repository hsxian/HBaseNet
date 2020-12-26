using System;
using System.Collections;
using System.Runtime;
using System.Collections.Generic;
using BitConverter;
using Google.Protobuf;
using Pb;
using System.Linq;
using System.Reflection;
using HBaseNet.Const;
using HBaseNet.Utility;
using HBaseNet.Metadata.Annotations;

namespace HBaseNet.Metadata.Conventions
{
    public class HBaseConvert
    {
        private static readonly Lazy<HBaseConvert> _default = new Lazy<HBaseConvert>(() => new HBaseConvert());
        public static HBaseConvert Instance => _default.Value;
        private HBaseConvert()
        {

        }
        public IEnumerable<T> ConvertToCustom<T>(IEnumerable<Result> result, EndianBitConverter bitConverter = null) where T : new()
        {
            bitConverter ??= EndianBitConverter.BigEndian;
            var convertCache = GetFamilyQualifierConvertDict<T>(bitConverter);
            var ret = result.Select(t => ConvertToCustom<T>(t, convertCache)).ToArray();
            return ret;
        }
        public T ConvertToCustom<T>(Result result, EndianBitConverter bitConverter = null) where T : new()
        {
            bitConverter ??= EndianBitConverter.BigEndian;
            var convertCache = GetFamilyQualifierConvertDict<T>(bitConverter);
            var ret = ConvertToCustom<T>(result, convertCache);
            return ret;
        }
        internal T ConvertToCustom<T>(Result result, Dictionary<string, ConvertItem<T>> convertCache) where T : new()
        {
            var ret = new T();
            foreach (var cell in result.Cell)
            {
                if (convertCache.TryGetValue(cell.Family.ToStringUtf8() + cell.Qualifier.ToStringUtf8(), out var convert))
                {
                    convert.Convert(ret, cell.Value);
                }
            }
            return ret;
        }
        public Dictionary<string, ConvertItem<T>> GetFamilyQualifierConvertDict<T>(EndianBitConverter bitConverter)
        {
            var tType = typeof(T);
            var arr = tType.GetProperties()
            .Select(prop =>
            {
                var hbpa = prop.GetCustomAttribute<HBasePropertyAttribute>();
                string key;
                if (hbpa != null)
                {
                    if (hbpa.IsIgnore) return (null, null);
                    var family = string.IsNullOrWhiteSpace(hbpa.Family) ? ConstString.DefaultFamily : hbpa.Family;
                    var qualifier = string.IsNullOrWhiteSpace(hbpa.Qualifier) ? prop.Name : hbpa.Qualifier;
                    key = family + qualifier;
                }
                else
                {
                    key = ConstString.DefaultFamily + prop.Name;
                }

                var convert = GetConvertMethod<T>(prop, bitConverter);
                if (convert == null) return (null, null);
                return (key, new ConvertItem<T> { Property = prop, Convert = convert });
            })
            .Where(t => t.key != null)
            ;
            return arr.ToDictionary(t => t.key, t => t.Item2);
        }
        private Action<T, ByteString> GetConvertMethod<T>(PropertyInfo prop, EndianBitConverter bitConverter)
        {
            Action<T, ByteString> convert = null;
            var hca = prop.GetCustomAttribute<HBaseConverterAttribute>();
            if (hca?.BinaryConverter != null)
            {
                var read = hca.BinaryConverter.GetMethod(nameof(IByteArrayConverter.Read));
                if (read == null) return convert;
                var reader = Activator.CreateInstance(hca.BinaryConverter);
                convert = new Action<T, ByteString>((t, b) =>
                {
                    var ik = read.Invoke(reader, new object[] { b.ToArray(), prop.PropertyType, hca.ConverterParameters });
                    prop.SetValue(t, ik);
                });
                return convert;
            }
            if (prop.PropertyType == typeof(string))
            {
                convert = new Action<T, ByteString>((t, b) =>
                {
                    var ik = b.ToStringUtf8();
                    prop.SetValue(t, ik);
                });
                return convert;
            }

            var method = bitConverter.GetType().GetMethod($"To{prop.PropertyType.Name}");
            if (method == null) return convert;
            convert = new Action<T, ByteString>((t, b) =>
            {
                var ik = method.Invoke(bitConverter, new object[] { b.ToByteArray(), 0 });
                prop.SetValue(t, ik);
            });
            return convert;
        }

        public Dictionary<string, Dictionary<string, byte[]>> ConvertToDictionary<T>(T obj, EndianBitConverter bitConverter = null) where T : class
        {
            bitConverter ??= EndianBitConverter.BigEndian;
            var ret = new Dictionary<string, Dictionary<string, byte[]>>();
            var tType = typeof(T);
            var tProps = tType.GetProperties();
            foreach (var prop in tProps)
            {
                var (family, qualifier) = GetFamilyQualifier(prop);
                if (qualifier == null) continue;
                var value = prop.GetValue(obj);
                if (value == null) continue;
                var binary = GetByteArrayByConvertMethod(prop, value, bitConverter);
                if (binary == null) continue;
                AddOrUpdateBinaryDictionary(ret, family, qualifier, binary);
            }
            return ret;
        }
        private byte[] GetByteArrayByConvertMethod(PropertyInfo prop, object value, EndianBitConverter bitConverter)
        {
            var hbca = prop.GetCustomAttribute<HBaseConverterAttribute>();
            if (hbca != null)
            {
                var writer = Activator.CreateInstance(hbca.BinaryConverter);
                var write = hbca.BinaryConverter.GetMethod(nameof(IByteArrayConverter.Write));
                if (write == null) return null;
                return write.Invoke(writer, new[] { value, hbca.ConverterParameters }) as byte[];
            }

            if (prop.PropertyType == typeof(string))
            {
                return value.ToString().ToUtf8Bytes();
            }

            var convert = bitConverter.GetType().GetMethods().FirstOrDefault(t =>
                 t.Name == nameof(EndianBitConverter.GetBytes) &&
                 t.GetParameters().FirstOrDefault()?.ParameterType == prop.PropertyType);
            return convert?.Invoke(bitConverter, new[] { value }) as byte[];
        }
        private (string family, string qualifier) GetFamilyQualifier(PropertyInfo prop)
        {
            var family = ConstString.DefaultFamily;
            string qualifier = null;
            var hbpa = prop.GetCustomAttribute<HBasePropertyAttribute>();
            if (hbpa != null)
            {
                if (hbpa.IsIgnore) return (null, null);
                family = string.IsNullOrWhiteSpace(hbpa.Family) ? family : hbpa.Family;
                qualifier = string.IsNullOrWhiteSpace(hbpa.Qualifier) ? prop.Name : hbpa.Qualifier;
                return (family, qualifier);
            }
            qualifier = prop.Name;
            return (family, qualifier);
        }
        private void AddOrUpdateBinaryDictionary(Dictionary<string, Dictionary<string, byte[]>> dict, string family, string qualifier, byte[] value)
        {
            if (dict.TryGetValue(family, out var quDict))
            {
                if (quDict.ContainsKey(qualifier))
                {
                    quDict[qualifier] = value;
                }
                else
                {
                    quDict.Add(qualifier, value);
                }
            }
            else
            {
                dict.Add(family, new Dictionary<string, byte[]>() { { qualifier, value } });
            }
        }
    }
}