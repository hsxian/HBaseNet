using System;
using System.Collections.Generic;
using Pb;
using System.Linq;

namespace HBaseNet.Metadata.Conventions
{
    public class HBaseConvert
    {
        private static readonly Lazy<HBaseConvert> _default = new Lazy<HBaseConvert>(() => new HBaseConvert());
        public static HBaseConvert Instance => _default.Value;
        private HBaseConvert()
        {

        }
        public IEnumerable<T> ConvertToCustom<T>(IEnumerable<Result> result, ConvertCache convertCache) where T : new()
        {
            var ret = result.Select(t => ConvertToCustom<T>(t, convertCache)).ToArray();
            return ret;
        }
        internal T ConvertToCustom<T>(Result result, ConvertCache convertCache) where T : new()
        {
            var ret = new T();
            foreach (var cell in result.Cell)
            {
                if (convertCache.ReaderCache.TryGetValue(cell.Family, out var dict) && dict.TryGetValue(cell.Qualifier, out var convert))
                {
                    var v = convert.ReadConvert(cell.Value);
                    if (v == null) continue;
                    convert.Property.SetValue(ret, v);
                }
            }
            return ret;
        }



        public Dictionary<string, Dictionary<string, byte[]>> ConvertToDictionary<T>(T obj, ConvertCache convertCache) where T : class
        {
            var ret = new Dictionary<string, Dictionary<string, byte[]>>();
            foreach (var prop in convertCache.WriterCache)
            {
                var v = prop.Property.GetValue(obj);
                if (v == null) continue;
                var binary = prop.WriteConvert(v);
                if (binary == null) continue;
                AddOrUpdateBinaryDictionary(ret, prop.Family2, prop.Qualifier2, binary);
            }
            return ret;
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