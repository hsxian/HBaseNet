using System;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Net.WebSockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using HBaseNet.HRpc;
using HBaseNet.Utility;
using Pb;

namespace HBaseNet.Region
{
    public class RegionClient
    {
        public uint Id { get; private set; }
        public string Host { get; private set; }
        public ushort Port { get; private set; }
        public NetworkStream Conn { get; private set; }
        public int TimeOut { get; set; }
        private ConcurrentQueue<ICall> _rpcQueue;
        private Mutex _writeMutex;
        private IOException _ioException;

        public RegionClient(string host, ushort port)
        {
            Host = host;
            Port = port;
            TimeOut = (int) TimeSpan.FromSeconds(30).TotalMilliseconds;
            var tcp = new TcpClient();

            var ipAddress = IPAddress.TryParse(host, out var ip)
                ? ip
                : Dns.GetHostEntry(host).AddressList.FirstOrDefault();
            tcp.Connect(ipAddress, port);
            Conn = tcp.GetStream();
            Conn.ReadTimeout = TimeOut;
            Conn.WriteTimeout = TimeOut;
            _writeMutex = new Mutex();
            _ = SendHello();
        }

        private async Task ProcessRPCQueue()
        {
            while (true)
            {
                await Task.Delay(100);
                _writeMutex.WaitOne();
                if (_ioException != null)
                {
                    foreach (var rpc in _rpcQueue)
                    {
                        
                    }
                }
            }
        }

        private Task<bool> SendHello()
        {
            var connHeader = new ConnectionHeader
            {
                UserInfo = new UserInformation
                {
                    EffectiveUser = "gopher"
                },
                ServiceName = "ClientService"
            };
            var data = connHeader.ToByteArray();
            var header = "HBas\x00\x50".ToUtf8Bytes(); // \x50 = Simple Auth.
            var buf = new byte[header.Length + 4 + data.Length];
            header.CopyTo(buf, 0);
            var dataLenBig = BitConverter.GetBytes(BinaryPrimitives.ReverseEndianness((uint) data.Length));
            dataLenBig.CopyTo(buf, 6);
            data.CopyTo(buf, header.Length + 4);
            return Write(buf);
        }

        private async Task<bool> Write(byte[] buf)
        {
            try
            {
                await Conn.WriteAsync(buf);
            }
            catch (Exception e)when (e is IOException io)
            {
                _ioException = io;
                Debug.WriteLine($"error when write to rpc conn:{e}");
            }
            finally
            {
            }

            return true;
        }

        private async Task<bool> ReadFully(byte[] buf)
        {
            var rs = 0;
            try
            {
                rs = await Conn.ReadAsync(buf);
            }
            catch (Exception e)
            {
                Debug.WriteLine($"error when read fully from rpc conn:{e}");
            }

            return rs > 0;
        }

        public async Task<T> SendRPC<T>(ICall rpc) where T : class, IMessage
        {
            Id++;
            var reqHeader = new RequestHeader
            {
                CallId = Id,
                MethodName = rpc.Name,
                RequestParam = true
            };
            var payload = rpc.Serialize();
            var payloadLen = ProtoBufEx.EncodeVarint((ulong) payload.Length);
            var headerData = reqHeader.ToByteArray();
            var buf = new byte[4 + 1 + headerData.Length + payloadLen.Length + payload.Length];
            BinaryPrimitives.WriteUInt32BigEndian(buf, (uint) (buf.Length - 4));
            buf[4] = (byte) headerData.Length;
            headerData.CopyTo(buf, 5);
            payloadLen.CopyTo(buf, 5 + headerData.Length);
            payload.CopyTo(buf, 5 + headerData.Length + payloadLen.Length);

            await Write(buf);
            var sz = new byte[4];
            await ReadFully(sz);

            buf = new byte[BinaryPrimitives.ReadInt32BigEndian(sz)];
            await ReadFully(buf);
            var (respLen, nb) = ProtoBufEx.DecodeVarint(buf);
            var resp = new ResponseHeader();
            buf = buf[(int) nb..];
            resp.MergeFrom(buf[..(int) respLen]);
            buf = buf[(int) respLen..];
            if (resp.Exception != null)
            {
                Debug.WriteLine($"Failed to deserialize the response header:{resp.Exception}");
            }

            if (resp.CallId == 0)
            {
                Debug.WriteLine("Response doesn't have a call ID!");
                return null;
            }

            if (resp.CallId != Id)
            {
                Debug.WriteLine($"Not the callId we expected: {reqHeader.CallId}");
                Debug.WriteLine(
                    $"remote exception :\r\n{resp.Exception.ExceptionClassName}:\n{resp.Exception.StackTrace}");
                return null;
            }

            if (resp.Exception != null)
            {
                Debug.WriteLine(
                    $"remote exception :\r\n{resp.Exception.ExceptionClassName}:\n{resp.Exception.StackTrace}");
                return null;
            }

            (respLen, nb) = ProtoBufEx.DecodeVarint(buf);
            buf = buf[(int) nb..];
            var rpcResp = rpc.ResponseParseFrom(buf);
            buf = buf[(int) respLen..];
            var result = rpcResp as T;
            return result;
        }
    }
}