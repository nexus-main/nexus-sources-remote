using System.Buffers.Binary;
using System.Net.Sockets;
using System.Text;
using Microsoft.Extensions.Logging;
using StreamJsonRpc;

namespace Nexus.Sources;

internal class RemoteCommunicator : IDisposable
{
    private readonly string _host;

    private readonly int _port;

    private readonly TcpClient _comm = new();

    private readonly TcpClient _data = new();

    private NetworkStream? _commStream;

    private NetworkStream? _dataStream;

    private IJsonRpcServer _rpcServer = default!;

    private readonly ILogger _logger;

    private readonly Func<string, DateTime, DateTime, Task> _readData;

    public RemoteCommunicator(
        string host,
        int port,
        Func<string, DateTime, DateTime, Task> readData,
        ILogger logger
    )
    {
        _host = host;
        _port = port;
        _readData = readData;
        _logger = logger;
    }

    public async Task<IJsonRpcServer> ConnectAsync(CancellationToken cancellationToken)
    {
        var id = Guid.NewGuid().ToString();

        // comm connection
        await _comm.ConnectAsync(_host, _port, cancellationToken);
        _commStream = _comm.GetStream();

        await _commStream.WriteAsync(Encoding.UTF8.GetBytes(id), cancellationToken);
        await _commStream.WriteAsync(Encoding.UTF8.GetBytes("comm"), cancellationToken);
        await _commStream.FlushAsync(cancellationToken);

        // data connection
        await _data.ConnectAsync(_host, _port, cancellationToken);
        _dataStream = _data.GetStream();
        
        await _dataStream.WriteAsync(Encoding.UTF8.GetBytes(id), cancellationToken);
        await _dataStream.WriteAsync(Encoding.UTF8.GetBytes("data"), cancellationToken);
        await _dataStream.FlushAsync(cancellationToken);

        var formatter = new SystemTextJsonFormatter()
        {
            JsonSerializerOptions = Utilities.JsonSerializerOptions
        };

        var messageHandler = new LengthHeaderMessageHandler(_commStream, _commStream, formatter);
        var jsonRpc = new JsonRpc(messageHandler);

        jsonRpc.AddLocalRpcMethod("log", new Action<LogLevel, string>((logLevel, message) =>
        {
            _logger.Log(logLevel, "{Message}", message);
        }));

        jsonRpc.AddLocalRpcMethod("readData", _readData);
        jsonRpc.StartListening();

        _rpcServer = jsonRpc.Attach<IJsonRpcServer>(new JsonRpcProxyOptions()
        {
            MethodNameTransform = pascalCaseAsyncName =>
            {
                return char.ToLower(pascalCaseAsyncName[0]) + pascalCaseAsyncName[1..].Replace("Async", string.Empty);
            }
        });

        return _rpcServer;
    }

    public ValueTask ReadRawAsync(Memory<byte> buffer, CancellationToken cancellationToken)
    {
        if (_dataStream is null)
            throw new Exception("You need to connect before read any data");

        return _dataStream.ReadExactlyAsync(buffer, cancellationToken);
    }

    public async Task<byte> ReadByteAsync(CancellationToken cancellationToken)
    {
        if (_dataStream is null)
            throw new Exception("You need to connect before read any data");

        byte[] buffer = new byte[1];
        await _dataStream.ReadExactlyAsync(buffer, cancellationToken);
        return buffer[0];
    }

    public async Task<int> ReadInt32BigEndianAsync(CancellationToken cancellationToken)
    {
        if (_dataStream is null)
            throw new Exception("You need to connect before read any data");

        byte[] buffer = new byte[4];
        await _dataStream.ReadExactlyAsync(buffer, cancellationToken);
        return BinaryPrimitives.ReadInt32BigEndian(buffer);
    }

    public Task WriteRawAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken)
    {
        if (_dataStream is null)
            throw new Exception("You need to connect before write any data");

        return InternalWriteRawAsync(buffer, _dataStream, cancellationToken);
    }

    private static async Task InternalWriteRawAsync(
        ReadOnlyMemory<byte> buffer, 
        Stream target, 
        CancellationToken cancellationToken
    )
    {
        var length = BitConverter.GetBytes(buffer.Length);
        Array.Reverse(length);

        await target.WriteAsync(length, cancellationToken);
        await target.WriteAsync(buffer, cancellationToken);
        await target.FlushAsync(cancellationToken);
    }

#region IDisposable

    private bool _disposedValue;

    protected virtual void Dispose(bool disposing)
    {
        if (!_disposedValue)
        {
            if (disposing)
            {
                var disposable = _rpcServer as IDisposable;
                disposable?.Dispose();

                _commStream?.Dispose();
                _dataStream?.Dispose();
            }

            _disposedValue = true;
        }
    }

    public void Dispose()
    {
        Dispose(disposing: true);
        GC.SuppressFinalize(this);
    }
    
#endregion
}
