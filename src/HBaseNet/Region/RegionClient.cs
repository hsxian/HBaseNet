using System;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
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
        private uint _callId;
        public string Host { get; }
        public ushort Port { get; }
        private NetworkStream Conn { get; set; }
        private int TimeOut { get; }
        private readonly ILogger<RegionClient> _logger;
        private ConcurrentDictionary<uint, ICall> _idRpcDict;
        private ConcurrentQueue<ICall> _rpcQueue;
        private ConcurrentQueue<RPCResult> _resultQueue;
        private Guid _guid;

        public RegionClient(string host, ushort port)
        {
            _logger = HBaseConfig.Instance.LoggerFactory.CreateLogger<RegionClient>();
            Host = host;
            Port = port;
            TimeOut = (int) TimeSpan.FromSeconds(30).TotalMilliseconds;
        }

        public async Task<RegionClient> Build()
        {
            var tcp = new TcpClient();
            var ipAddress = IPAddress.TryParse(Host, out var ip)
                ? ip
                : (await Dns.GetHostEntryAsync(Host)).AddressList.FirstOrDefault();
            await tcp.ConnectAsync(ipAddress, Port);
            Conn = tcp.GetStream();
            Conn.ReadTimeout = TimeOut;
            Conn.WriteTimeout = TimeOut;
            _rpcQueue = new ConcurrentQueue<ICall>();
            _resultQueue = new ConcurrentQueue<RPCResult>();
            _idRpcDict = new ConcurrentDictionary<uint, ICall>();
            if (!await SendHello()) return null;
            _guid = Guid.NewGuid();
            ProcessRPCs();
            return this;
        }

        private void ProcessRPCs()
        {
            Task.Factory.StartNew(async () =>
            {
                while (true)
                {
                    if (_rpcQueue.Count < 1)
                    {
                        await Task.Delay(10);
                    }
                    else
                    {
                        while (_rpcQueue.TryDequeue(out var rpc))
                        {
                            await SendRPC(rpc);
                            var res = await ReceiveRPC();
                            _resultQueue.Enqueue(res);
                        }
                    }
                }
            }, TaskCreationOptions.LongRunning);
        }

        public async Task<RPCResult> GetRPCResult()
        {
            RPCResult result = null;
            do
            {
                if (_resultQueue.TryDequeue(out var res))
                {
                    result = res;
                    break;
                }

                await Task.Delay(1);
            } while (true);

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
                _logger.LogError(e, $"error when write to rpc conn: ");
            }

            return true;
        }

        private async Task<Exception> ReadFully(byte[] buf)
        {
            var rs = 0;
            try
            {
                rs = await Conn.ReadAsync(buf);
            }
            catch (Exception e)
            {
                _logger.LogError(e, $"error when read fully from rpc conn: ");
                return e;
            }

            return null;
        }

        public void QueueRPC(ICall rpc)
        {
            _rpcQueue.Enqueue(rpc);
        }

        public async Task<bool> SendRPC(ICall rpc)
        {
            _callId++;
            var reqHeader = new RequestHeader
            {
                CallId = _callId,
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

            var w = await Write(buf);
            return w && _idRpcDict.TryAdd(_callId, rpc);
        }

        public async Task<RPCResult> ReceiveRPC()
        {
            var result = new RPCResult();
            var sz = new byte[4];
            result.Error = await ReadFully(sz);
            if (result.Error != null) return result;

            var buf = new byte[BinaryPrimitives.ReadInt32BigEndian(sz)];
            result.Error = await ReadFully(buf);
            if (result.Error != null) return result;

            var resp = new ResponseHeader();
            var (respLen, nb) = ProtoBufEx.DecodeVarint(buf);
            buf = buf[nb..];
            try
            {
                resp.MergeFrom(buf[..(int) respLen]);
                buf = buf[(int) respLen..];
            }
            catch (Exception e)
            {
                _logger.LogError(e, $"Failed to deserialize the response header from length :{respLen}.");
                result.Error = e;

                return result;
            }

            if (resp.CallId == 0)
            {
                const string msg = "Response doesn't have a call ID!";
                _logger.LogError(msg);
                result.Error = new Exception(msg);
                return result;
            }

            if (_idRpcDict.TryRemove(resp.CallId, out var rpc) == false)
            {
                var msg = $"Not the callId we expected: {resp.CallId}";
                _logger.LogError(msg);
                result.Error = new Exception(msg);
                return result;
            }


            if (resp.Exception != null)
            {
                var errStr =
                    $"HBase java exception: {resp.Exception.ExceptionClassName}:\n{resp.Exception.StackTrace}";
                _logger.LogError(errStr);
                result.Error = new Exception(errStr);
            }
            else
            {
                (respLen, nb) = ProtoBufEx.DecodeVarint(buf);
                buf = buf[(int) nb..];
                result.Msg = rpc.ParseResponseFrom(buf);
                buf = buf[(int) respLen..];
            }
            return result;
        }
    }
}