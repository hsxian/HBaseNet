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
using HBaseNet.Region.Exceptions;
using HBaseNet.Utility;
using Microsoft.Extensions.Logging;
using Pb;

namespace HBaseNet.Region
{
    public class RegionClient : IDisposable
    {
        private int _callId;
        public string Host { get; }
        public ushort Port { get; }
        private RegionType Type { get; }
        private NetworkStream _conn;
        private Socket _socket;
        private int TimeOut { get; }
        private readonly ILogger<RegionClient> _logger;
        private ConcurrentDictionary<uint, RPCResult> _idResultDict;
        private ConcurrentDictionary<uint, ICall> _idRPCDict;
        private BlockingCollection<ICall> _rpcQueue;
        private bool _isWorking;
        public int CallQueueSize { get; set; } = 150;

        public RegionClient(string host, ushort port, RegionType type)
        {
            _logger = HBaseConfig.Instance.LoggerFactory.CreateLogger<RegionClient>();
            Host = host;
            Port = port;
            Type = type;
            TimeOut = (int) TimeSpan.FromSeconds(30).TotalMilliseconds;
        }

        private int GetNextCallId()
        {
            return Interlocked.Increment(ref _callId);
        }

        public async Task<RegionClient> Build()
        {
            try
            {
                var ipAddress = IPAddress.TryParse(Host, out var ip)
                    ? ip
                    : (await Dns.GetHostEntryAsync(Host)).AddressList.FirstOrDefault();
                _socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
                await _socket.ConnectAsync(ipAddress, Port);
                _conn = new NetworkStream(_socket, FileAccess.ReadWrite)
                    {ReadTimeout = TimeOut, WriteTimeout = TimeOut};
                _rpcQueue = new BlockingCollection<ICall>(CallQueueSize);
                _idResultDict = new ConcurrentDictionary<uint, RPCResult>();
                _idRPCDict = new ConcurrentDictionary<uint, ICall>();
                _isWorking = true;
                if (false == await SendHello(Type)) return null;
                ProcessRPCs();
                ReceiveRPCs();
                _logger.LogInformation($"connect to the RegionServer at {Host}:{Port}");
            }
            catch (Exception e)
            {
                _logger.LogError(e, $"failed to connect to the RegionServer at {Host}:{Port}");
                return null;
            }

            return this;
        }

        private void ProcessRPCs()
        {
            Task.Factory.StartNew(async () =>
            {
                while (_isWorking)
                {
                    await Task.Delay(1);

                    while (_rpcQueue.TryTake(out var rpc))
                    {
                        //TODO:CancellationToken
                        var token = new CancellationToken();
                        await SendRPC(rpc, token);
                    }
                }
            }, TaskCreationOptions.LongRunning);
        }

        private void ReceiveRPCs()
        {
            Task.Factory.StartNew(async () =>
            {
                while (_isWorking)
                {
                    var token = new CancellationToken();
                    var res = await ReceiveRPC(token);
                    if (res.CallId == 0)
                    {
                        await Task.Delay(1, token);
                    }

                    while (_idResultDict.ContainsKey(res.CallId) == false)
                    {
                        _idResultDict.TryAdd(res.CallId, res);
                    }
                }
            }, TaskCreationOptions.LongRunning);
        }

        public async Task<RPCResult> GetRPCResult(uint callId)
        {
            RPCResult result = null;
            do
            {
                if (_idResultDict.TryRemove(callId, out var res))
                {
                    result = res;
                    break;
                }

                await Task.Delay(1);
            } while (_isWorking);

            return result;
        }

        private Task<bool> SendHello(RegionType type)
        {
            var connHeader = new ConnectionHeader
            {
                UserInfo = new UserInformation
                {
                    EffectiveUser = "gopher"
                },
                ServiceName = type.ToString()
            };
            var data = connHeader.ToByteArray();
            var header = "HBas\x00\x50".ToUtf8Bytes(); // \x50 = Simple Auth.
            var buf = new byte[header.Length + 4 + data.Length];
            header.CopyTo(buf, 0);
            var dataLenBig = BitConverter.GetBytes(BinaryPrimitives.ReverseEndianness((uint) data.Length));
            dataLenBig.CopyTo(buf, 6);
            data.CopyTo(buf, header.Length + 4);
            return Write(buf, new CancellationToken());
        }

        private async Task<bool> Write(byte[] buf, CancellationToken token)
        {
            try
            {
                await _conn.WriteAsync(buf, token);
            }
            catch (Exception e) when (e is IOException)
            {
                _logger.LogError(e, $"error when write to rpc conn");
                return false;
            }

            return true;
        }

        private async Task<Exception> ReadFully(byte[] buf, CancellationToken token)
        {
            try
            {
                var rs = await _conn.ReadAsync(buf, token);
                if (rs == 0)
                {
                    return new IOException("io not anything");
                }
            }
            catch (Exception e)
            {
                _logger.LogError(e, $"error when read fully from rpc conn");
                return e;
            }

            return null;
        }

        public async Task QueueRPC(ICall rpc)
        {
            rpc.CallId = (uint) GetNextCallId();
            while (_rpcQueue.TryAdd(rpc) == false)
            {
                await Task.Delay(1);
            }
        }

        private async Task<bool> SendRPC(ICall rpc, CancellationToken token)
        {
            var reqHeader = new RequestHeader
            {
                CallId = rpc.CallId,
                MethodName = rpc.Name,
                RequestParam = true
            };

            while (_idRPCDict.ContainsKey(rpc.CallId) == false)
            {
                _idRPCDict.TryAdd(rpc.CallId, rpc);
            }

            var payload = rpc.Serialize();

            var payloadLen = ProtoBufEx.EncodeVarint((ulong) payload.Length);

            var headerData = reqHeader.ToByteArray();

            var buf = new byte[4 + 1 + headerData.Length + payloadLen.Length + payload.Length];
            BinaryPrimitives.WriteUInt32BigEndian(buf, (uint) (buf.Length - 4));
            buf[4] = (byte) headerData.Length;
            headerData.CopyTo(buf, 5);
            payloadLen.CopyTo(buf, 5 + headerData.Length);
            payload.CopyTo(buf, 5 + headerData.Length + payloadLen.Length);
            
            return await Write(buf, token);
        }

        private async Task<RPCResult> ReceiveRPC(CancellationToken token)
        {
            var result = new RPCResult();
            var sc = _socket;
            var sz = new byte[4];
            result.Error = await ReadFully(sz, token);
            if (result.Error != null) return result;

            var buf = new byte[BinaryPrimitives.ReadInt32BigEndian(sz)];
            result.Error = await ReadFully(buf, token);
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


            if (_idRPCDict.ContainsKey(resp.CallId) == false)
            {
                var msg = $"Not the callId we expected: {resp.CallId}.{string.Join(",", _idRPCDict.Keys)}";
                _logger.LogError(msg);
                result.Error = new Exception(msg);
                return result;
            }

            ICall rpc;
            do
            {
                _idRPCDict.TryRemove(resp.CallId, out var p);
                rpc = p;
            } while (rpc == null);


            if (resp.Exception != null)
            {
                var errStr =
                    $"HBase java exception: {resp.Exception.ExceptionClassName}:{resp.Exception.StackTrace}";

                if (ExceptionMap.IsMatch<CallQueueTooBigException>(resp.Exception.ExceptionClassName))
                {
                    result.Error = new CallQueueTooBigException();
                    _logger.LogWarning(
                        $"{errStr}.\n\tThe rpc will be retried {rpc.RetryCount + 1}th, and you can reduce the {nameof(CallQueueSize)} configuration to reduce the exception.");
                }
                else if (ExceptionMap.IsMatch<RetryableException>(resp.Exception.ExceptionClassName))
                {
                    result.Error = new RetryableException(errStr);
                }
                else
                {
                    _logger.LogError(errStr);
                }
            }
            else
            {
                (respLen, nb) = ProtoBufEx.DecodeVarint(buf);
                buf = buf[(int) nb..];
                result.Msg = rpc.ParseResponseFrom(buf);
                buf = buf[(int) respLen..];
            }

            result.CallId = resp.CallId;
            return result;
        }

        public void Dispose()
        {
            _isWorking = false;
        }
    }
}