using System;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using HBaseNet.HRpc;
using HBaseNet.Utility;
using Microsoft.Extensions.Logging;
using Pb;

namespace HBaseNet.Region
{
    public class RegionClient
    {
        private uint Id { get; set; }
        public string Host { get; private set; }
        public ushort Port { get; private set; }
        private NetworkStream Conn { get; set; }
        private int TimeOut { get; set; }
        private readonly ConcurrentQueue<ICall> _rpcQueue;
        private readonly ConcurrentQueue<RPCResult> _rpcResultQueue;
        private readonly ILogger<RegionClient> _logger;

        public RegionClient(string host, ushort port)
        {
            _logger = HBaseConfig.Instance.LoggerFactory.CreateLogger<RegionClient>();
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
            _rpcQueue = new ConcurrentQueue<ICall>();
            _rpcResultQueue = new ConcurrentQueue<RPCResult>();
            _ = SendHello();
            ProcessRPCQueue();
        }

        private void ProcessRPCQueue()
        {
            Task.Factory.StartNew(async () =>
            {
                while (true)
                {
                    if (_rpcQueue.Count > 0)
                    {
                        while (_rpcQueue.TryDequeue(out var rpc))
                        {
                            var result = new RPCResult();
                            try
                            {
                                result.Msg = await SendRPC<IMessage>(rpc);
                            }
                            catch (Exception e)
                            {
                                result.Error = e;
                                _logger.LogError("SendRPC in RPCQueue", e);
                            }
                            finally
                            {
                                _rpcResultQueue.Enqueue(result);
                            }
                        }
                    }
                    else
                    {
                        await Task.Delay(1);
                    }
                }
            }, TaskCreationOptions.LongRunning);
        }

        public void QueueRPC(ICall rpc)
        {
            _rpcQueue.Enqueue(rpc);
        }

        public async Task<RPCResult> GetRPCResult()
        {
            while (_rpcResultQueue.Count < 1) await Task.Delay(10);
            RPCResult result = null;
            do
            {
                _rpcResultQueue.TryDequeue(out var rpcResult);
                result = rpcResult;
            } while (result == null);

            return result;
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
            catch (Exception e) when (e is IOException io)
            {
                _logger.LogError($"error when write to rpc conn: ", e);
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
                _logger.LogError($"error when read fully from rpc conn: ", e);
            }

            return rs > 0;
        }

        public async Task<T> SendRPC<T>(ICall rpc) where T : class, IMessage
        {
            // await _sendRPCSemaphore.WaitAsync();
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
                _logger.LogError($"Failed to deserialize the response header:", resp.Exception);
            }

            if (resp.CallId == 0)
            {
                _logger.LogError("Response doesn't have a call ID!");
                return null;
            }

            if (resp.CallId != Id)
            {
                _logger.LogError($"Not the callId we expected: {reqHeader.CallId}");
                _logger.LogError(
                    $"remote exception :{resp.Exception}");
                return null;
            }

            if (resp.Exception != null)
            {
                _logger.LogError(
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