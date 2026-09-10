using System.Buffers.Binary;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using System.Text;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
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

    private readonly Func<int, string, DateTime, DateTime, Task> _readData;

    private readonly byte[] _readDataResponseDataFrameHeader = new byte[9];

    private readonly byte[] _readDataResponseEndFrameHeader = new byte[5];

    private readonly byte[] _readDataResponseErrorFrameHeader = new byte[9];

    private readonly SemaphoreSlim _dataWriteLock = new(1, 1);

    public RemoteCommunicator(
        string host,
        int port,
        Func<int, string, DateTime, DateTime, Task> readData,
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

    public Task WriteReadDataResponseAsync(
        int requestId,
        ReadOnlyMemory<byte> data,
        CancellationToken cancellationToken)
    {
        if (_dataStream is null)
            throw new Exception("You need to connect before write any data");

        return InternalWriteReadDataResponseAsync(requestId, data, cancellationToken);
    }

    public Task WriteReadDataResponseErrorAsync(
        int requestId,
        string message,
        CancellationToken cancellationToken)
    {
        if (_dataStream is null)
            throw new Exception("You need to connect before write any data");

        return InternalWriteReadDataResponseErrorAsync(requestId, message, cancellationToken);
    }

    public void ResetReadDataResponseProtocol()
    {
        // Success payloads are self-contained Arrow IPC streams; request-id framing remains for correlation.
    }

    public ArrowStreamReader CreateArrowStreamReader()
    {
        if (_dataStream is null)
            throw new Exception("You need to connect before read any data");

        return new ArrowStreamReader(_dataStream);
    }

    private async Task InternalWriteReadDataResponseAsync(
        int requestId,
        ReadOnlyMemory<byte> data,
        CancellationToken cancellationToken)
    {
        await _dataWriteLock.WaitAsync(cancellationToken);

        try
        {
            var remainingData = CreateReadDataArrowStream(data).AsMemory();

            while (!remainingData.IsEmpty)
            {
                var payloadLength = Math.Min(remainingData.Length, Remote.MAX_BATCH_STREAM_PAYLOAD_LENGTH);

                _readDataResponseDataFrameHeader[0] = 0x01;
                BinaryPrimitives.WriteInt32LittleEndian(_readDataResponseDataFrameHeader.AsSpan(1), requestId);
                BinaryPrimitives.WriteInt32LittleEndian(_readDataResponseDataFrameHeader.AsSpan(5), payloadLength);

                await _dataStream!.WriteAsync(_readDataResponseDataFrameHeader, cancellationToken);
                await _dataStream.WriteAsync(remainingData[..payloadLength], cancellationToken);
                remainingData = remainingData[payloadLength..];
            }

            _readDataResponseEndFrameHeader[0] = 0x03;
            BinaryPrimitives.WriteInt32LittleEndian(_readDataResponseEndFrameHeader.AsSpan(1), requestId);
            await _dataStream!.WriteAsync(_readDataResponseEndFrameHeader, cancellationToken);
            await _dataStream.FlushAsync(cancellationToken);
        }
        finally
        {
            _dataWriteLock.Release();
        }
    }

    private async Task InternalWriteReadDataResponseErrorAsync(
        int requestId,
        string message,
        CancellationToken cancellationToken)
    {
        await _dataWriteLock.WaitAsync(cancellationToken);

        try
        {
            var msgBytes = Encoding.UTF8.GetBytes(message);

            if (msgBytes.Length > Remote.MAX_ERROR_MESSAGE_LENGTH)
            {
                while (Encoding.UTF8.GetByteCount(message) > Remote.MAX_ERROR_MESSAGE_LENGTH)
                    message = message[..(message.Length - 1)];

                msgBytes = Encoding.UTF8.GetBytes(message);
            }

            _readDataResponseErrorFrameHeader[0] = 0x02;
            BinaryPrimitives.WriteInt32LittleEndian(_readDataResponseErrorFrameHeader.AsSpan(1), requestId);
            BinaryPrimitives.WriteInt32LittleEndian(_readDataResponseErrorFrameHeader.AsSpan(5), msgBytes.Length);
            await _dataStream!.WriteAsync(_readDataResponseErrorFrameHeader, cancellationToken);
            await _dataStream.WriteAsync(msgBytes, cancellationToken);
            await _dataStream.FlushAsync(cancellationToken);
        }
        finally
        {
            _dataWriteLock.Release();
        }
    }

    private static byte[] CreateReadDataArrowStream(ReadOnlyMemory<byte> data)
    {
        if (data.Length % sizeof(double) != 0)
            throw new Exception("The readData response buffer length is not a multiple of the double element size.");

        var valueCount = data.Length / sizeof(double);
        var schema = CreateReadDataSchema();
        using var stream = new MemoryStream();
        using var writer = new ArrowStreamWriter(stream, schema);
        using var recordBatch = CreateReadDataRecordBatch(schema, data, valueCount);

        writer.WriteRecordBatch(recordBatch);
        writer.WriteEnd();

        return stream.ToArray();
    }

    private static Schema CreateReadDataSchema()
    {
        var fields = new[]
        {
            new Field("offset", new Int64Type(), nullable: false, metadata: []),
            new Field("values", new ListType(new DoubleType()), nullable: false, metadata: [])
        };

        return new Schema(fields, metadata: []);
    }

    private static RecordBatch CreateReadDataRecordBatch(Schema schema, ReadOnlyMemory<byte> data, int valueCount)
    {
        var offsetArray = new Int64Array.Builder().Append(0).Build(default);
        var offsetsBytes = MemoryMarshal.AsBytes(new[] { 0, valueCount }.AsSpan()).ToArray();
        var offsetsBuffer = new ArrowBuffer(offsetsBytes);
        var valueArray = new DoubleArray(new ArrowBuffer(data), ArrowBuffer.Empty, valueCount, nullCount: 0, offset: 0);
        var listArray = new ListArray(new ListType(new DoubleType()), 1, offsetsBuffer, valueArray, ArrowBuffer.Empty, 0, 0);

        return new RecordBatch(schema, [offsetArray, listArray], 1);
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
                _dataWriteLock.Dispose();
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
