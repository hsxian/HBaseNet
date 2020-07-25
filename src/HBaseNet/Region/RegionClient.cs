using System;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using BitConverter;
using Google.Protobuf;
using HBaseNet.Const;
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
        private Socket _socket;
        private readonly ILogger<RegionClient> _logger;
        private ConcurrentDictionary<uint, RPCResult> _idResultDict;
        private ConcurrentDictionary<uint, RPCSend> _idRPCDict;
        private BlockingCollection<RPCSend> _rpcQueue;
        public TimeSpan TimeOut { get; set; } = TimeSpan.FromSeconds(30);
        public int CallQueueSize { get; set; } = 150;
        public string EffectiveUser { get; set; } = ConstString.DefaultEffectiveUser;
        private CancellationTokenSource _defaultCancellationSource;

        public RegionClient(string host, ushort port, RegionType type)
        {
            _logger = HBaseConfig.Instance.LoggerFactory.CreateLogger<RegionClient>();
            Host = host;
            Port = port;
            Type = type;
            _rpcQueue = new BlockingCollection<RPCSend>(CallQueueSize);
            _idResultDict = new ConcurrentDictionary<uint, RPCResult>();
            _idRPCDict = new ConcurrentDictionary<uint, RPCSend>();
            _defaultCancellationSource = new CancellationTokenSource();
        }

        private int GetNextCallId()
        {
            return Interlocked.Increment(ref _callId);
        }

        private async Task TryConn(int retryCount, CancellationToken token)
        {
            var ipAddress = IPAddress.TryParse(Host, out var ip)
                ? ip
                : (await Dns.GetHostEntryAsync(Host)).AddressList.FirstOrDefault();
            var backoff = TimeSpan.FromMilliseconds(100);
            for (var i = 0; i < retryCount && token.IsCancellationRequested == false; i++)
            {
                try
                {
                    await _socket.ConnectAsync(ipAddress, Port);

                    _logger.LogInformation($"Connect to the RegionServer({Type}) at {Host}:{Port}");
                    return;
                }
                catch (Exception e)
                {
                    _logger.LogWarning(
                        $"Connect RegionServer({Host}:{Port}) failed in {i + 1}th，try the locate again after {backoff}.\n{e}");
                    backoff = await TaskEx.SleepAndIncreaseBackoff(backoff, TimeSpan.FromSeconds(5), token);
                }
            }

            _logger.LogError($"Connect RegionServer({Host}:{Port}) failed.");
        }

        public async Task<RegionClient> Build(int retryCount, CancellationToken token)
        {
            _socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            await TryConn(retryCount, token);
            if (false == await SendHello(Type)) return null;
            ProcessRPCs();
            ReceiveRPCs();
            ProcessOtherSituation();
            return this;
        }

        private void ProcessOtherSituation()
        {
            Task.Factory.StartNew(async () =>
            {
                while (_defaultCancellationSource.IsCancellationRequested == false)
                {
                    await Task.Delay(1000);
                }
            }, TaskCreationOptions.LongRunning);
        }
        public async Task QueueRPC(ICall rpc)
        {
            rpc.CallId = (uint)GetNextCallId();
            var send = new RPCSend
            {
                RPC = rpc
            };
            while (_rpcQueue.TryAdd(send) == false)
            {
                await Task.Delay(1);
            }

            send.QueueTime = DateTime.Now;
        }
        private void ProcessRPCs()
        {
            Task.Factory.StartNew(async () =>
            {
                while (_defaultCancellationSource.IsCancellationRequested == false)
                {
                    await Task.Delay(10);
                    while (_rpcQueue.TryTake(out var rpc))
                    {
                        //TODO:CancellationToken
                        await SendRPC(rpc, _defaultCancellationSource.Token);
                    }
                }
            }, TaskCreationOptions.LongRunning);
        }

        private void ReceiveRPCs()
        {
            Task.Factory.StartNew(async () =>
            {
                while (_defaultCancellationSource.IsCancellationRequested == false)
                {
                    var res = await ReceiveRPC(_defaultCancellationSource.Token);
                    if (res.CallId == 0)
                    {
                        await Task.Delay(1, _defaultCancellationSource.Token);
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

                if (_idRPCDict.TryGetValue(callId, out var rpc))
                {
                    if ((DateTime.Now - rpc.QueueTime) > TimeOut)
                    {
                        while (_idRPCDict.ContainsKey(callId))
                        {
                            _idRPCDict.TryRemove(callId, out _);
                        }

                        result = new RPCResult
                        {
                            CallId = callId,
                            Error = new TimeoutException(
                                $"RPC ({rpc.RPC.Name}) waiting time out({TimeOut} milliseconds),the client will no longer wait.")
                        };
                        _logger.LogError(result.Error.Message);
                        break;
                    }
                }

                await Task.Delay(1);
            } while (_defaultCancellationSource.IsCancellationRequested == false);

            return result;
        }

        private Task<bool> SendHello(RegionType type)
        {
            var connHeader = new ConnectionHeader
            {
                UserInfo = new UserInformation
                {
                    EffectiveUser = EffectiveUser
                },
                ServiceName = type.ToString()
            };
            var data = connHeader.ToByteArray();
            var header = "HBas\x00\x50".ToUtf8Bytes(); // \x50 = Simple Auth.
            var buf = new byte[header.Length + 4 + data.Length];
            header.CopyTo(buf, 0);
            var dataLenBig = EndianBitConverter.BigEndian.GetBytes((uint)data.Length);
            dataLenBig.CopyTo(buf, 6);
            data.CopyTo(buf, header.Length + 4);
            return Write(buf, new CancellationToken());
        }

        private async Task<bool> Write(byte[] buf, CancellationToken token)
        {
            try
            {
                var count = await _socket.SendAsync(buf, SocketFlags.None);
                if (count != buf.Length)
                {
                    _logger.LogError("The data was not sent completely.");
                }
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
                var size = buf.Length;
                var total = 0;
                var dataLeft = size;

                while (total < size)
                {
                    await Task.CompletedTask;
                    var recv = _socket.Receive(buf, total, dataLeft, SocketFlags.None);
                    if (recv == 0)
                    {
                        break;
                    }

                    total += recv;
                    dataLeft -= recv;
                }
            }
            catch (Exception e)
            {
                _logger.LogError(e, $"error when read fully from rpc conn");
                return e;
            }

            return null;
        }

        private async Task<bool> SendRPC(RPCSend send, CancellationToken token)
        {
            var reqHeader = new RequestHeader
            {
                CallId = send.RPC.CallId,
                MethodName = send.RPC.Name,
                RequestParam = true
            };

            while (_idRPCDict.ContainsKey(send.RPC.CallId) == false)
            {
                _idRPCDict.TryAdd(send.RPC.CallId, send);
            }

            var payload = send.RPC.Serialize();

            var payloadLen = ProtoBufEx.EncodeVarint((ulong)payload.Length);

            var headerData = reqHeader.ToByteArray();

            var buf = new byte[4 + 1 + headerData.Length + payloadLen.Length + payload.Length];
            BinaryPrimitives.WriteUInt32BigEndian(buf, (uint)(buf.Length - 4));
            buf[4] = (byte)headerData.Length;
            headerData.CopyTo(buf, 5);
            payloadLen.CopyTo(buf, 5 + headerData.Length);
            payload.CopyTo(buf, 5 + headerData.Length + payloadLen.Length);
            return await Write(buf, token);
        }

        private async Task<RPCResult> ReceiveRPC(CancellationToken token)
        {
            var result = new RPCResult();
            var sz = new byte[4];
            result.Error = await ReadFully(sz, token);
            if (result.Error != null) return result;
            var buf = new byte[BinaryPrimitives.ReadUInt32BigEndian(sz)];
            result.Error = await ReadFully(buf, token);
            if (result.Error != null) return result;

            var resp = new ResponseHeader();
            var (respLen, nb) = ProtoBufEx.DecodeVarint(buf);
            buf = buf[nb..];
            try
            {
                resp.MergeFrom(buf[..(int)respLen]);
                buf = buf[(int)respLen..];
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
                rpc = p.RPC;
            } while (rpc == null);


            if (resp.Exception != null)
            {
                ProcessRPCResultException(rpc, result, resp.Exception);
            }
            else
            {
                (respLen, nb) = ProtoBufEx.DecodeVarint(buf);
                buf = buf[nb..];
                result.Msg = rpc.ParseResponseFrom(buf);
                buf = buf[(int)respLen..];
            }

            result.CallId = resp.CallId;
            return result;
        }

        private void ProcessRPCResultException(ICall rpc, RPCResult result, ExceptionResponse exception)
        {
            var errStr =
                $"HBase java exception: {exception.ExceptionClassName}:{exception.StackTrace}";

            if (ExceptionMap.IsMatch<CallQueueTooBigException>(exception.ExceptionClassName))
            {
                result.Error = new CallQueueTooBigException();
                _logger.LogWarning(
                    $"{errStr}.\n\tThe rpc will be retried {rpc.RetryCount + 1}th, and you can reduce the {nameof(CallQueueSize)} configuration to reduce the exception.");
            }
            else if (ExceptionMap.IsMatch<RetryableException>(exception.ExceptionClassName))
            {
                result.Error = new RetryableException(errStr);
            }
            else if (ExceptionMap.IsMatch<DoNotRetryIOException>(exception.ExceptionClassName))
            {
                result.Error = new DoNotRetryIOException(errStr);
                _logger.LogError($"{errStr}");
            }
            else
            {
                result.Error = new Exception(errStr);
                _logger.LogError(errStr);
            }
        }

        public void Dispose()
        {
            _defaultCancellationSource.Cancel();
        }
    }
}