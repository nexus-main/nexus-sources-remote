using Microsoft.Extensions.Logging;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using Nexus.DataModel;
using Nexus.Extensibility;
using System.Buffers;
using System.Buffers.Binary;
using System.Diagnostics;
using System.Globalization;
using System.Net.Sockets;
using System.Reflection;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization;

namespace Nexus.Remoting;

internal class Logger(
    NetworkStream commStream, 
    Stopwatch watchdogTimer, 
    CancellationToken cancellationToken
) : ILogger
{
    private readonly Stopwatch _watchdogTimer = watchdogTimer;

    private readonly NetworkStream _commStream = commStream;

    private readonly CancellationToken _cancellationToken = cancellationToken;

    public IDisposable? BeginScope<TState>(TState state) where TState : notnull
    {
        throw new NotImplementedException("Scopes are not supported on this logger.");
    }

    public bool IsEnabled(LogLevel logLevel)
    {
        return true;
    }

    public void Log<TState>(
        LogLevel logLevel, 
        EventId eventId, 
        TState state, 
        Exception? exception, 
        Func<TState, Exception?, string> formatter
    )
    {
        var notification = new JsonObject()
        {
            ["jsonrpc"] = "2.0",
            ["method"] = "log",
            ["params"] = new JsonArray(logLevel.ToString(), formatter(state, exception))
        };

        _ = Utilities.SendToServerAsync(notification, _commStream, _cancellationToken);
        _watchdogTimer.Restart();
    }
}

/// <summary>
/// A remote communicator.
/// </summary>
public class RemoteCommunicator
{
    private const int MaximumBatchStreamPayloadLength = 4 * 1024 * 1024;
    private const int MaximumBatchStreamErrorMessageLength = 64 * 1024;

    private readonly NetworkStream _commStream;

    private readonly NetworkStream _dataStream;

    private readonly Func<string, Type> _getDataSourceType;

    private readonly Stopwatch _watchdogTimer = new();

    private ILogger _logger = default!;

    private string? _sourceTypeName = default;

    private IDataSource? _dataSource = default;

    private readonly SemaphoreSlim _frameWriteLock = new(1, 1);

    private readonly SemaphoreSlim _readDataResponseReadLock = new(1, 1);

    private int _readDataRequestId;

    private readonly Dictionary<int, ReadDataResponseBuilder> _readDataResponses = [];

    /// <summary>
    /// Initializes a new instance of the <see cref="RemoteCommunicator" />.
    /// </summary>
    /// <param name="commStream">The network stream for communications.</param>
    /// <param name="dataStream">The network stream for data.</param>
    /// <param name="getDataSourceType">A func to get a new data source instance by its type name.</param>
    public RemoteCommunicator(
        NetworkStream commStream,
        NetworkStream dataStream,
        Func<string, Type> getDataSourceType
    )
    {
        _commStream = commStream;
        _dataStream = dataStream;

        _getDataSourceType = getDataSourceType;
    }

    /// <summary>
    /// Gets the time passed since the last communication.
    /// </summary>
    public TimeSpan LastCommunication => _watchdogTimer.Elapsed;

    /// <summary>
    /// Starts the remoting operation.
    /// </summary>
    /// <returns></returns>
    public Task RunAsync(CancellationToken cancellationToken)
    {
        static JsonElement Read(Span<byte> jsonRequest)
        {
            var reader = new Utf8JsonReader(jsonRequest);
            return JsonSerializer.Deserialize<JsonElement>(ref reader, Utilities.JsonSerializerOptions);
        }

        /* Make this method async as early as possible to not block the calling method.
         * Otherwise new clients cannot connect because the call to ReadSize may block
         * forever, preventing the Lock to be released.
         */
        return Task.Run(async () =>
        {
            // loop
            while (true)
            {
                // https://www.jsonrpc.org/specification

                // get request message
                var size = ReadSize(_commStream);

                using var memoryOwner = MemoryPool<byte>.Shared.Rent(size);
                var messageMemory = memoryOwner.Memory[..size];

                _commStream.InternalReadExactly(messageMemory.Span);
                var request = Read(messageMemory.Span);

                // process message
                JsonObject? response;

                if (request.TryGetProperty("jsonrpc", out var element) &&
                    element.ValueKind == JsonValueKind.String &&
                    element.GetString() == "2.0")
                {
                    if (request.TryGetProperty("id", out var _))
                    {
                        try
                        {
                            var result = await ProcessInvocationAsync(request, cancellationToken);
                            _watchdogTimer.Restart();

                            response = new JsonObject()
                            {
                                ["result"] = result
                            };
                        }
                        catch (Exception ex)
                        {
                            response = new JsonObject()
                            {
                                ["error"] = new JsonObject()
                                {
                                    ["code"] = -1,
                                    ["message"] = ex.ToString()
                                }
                            };
                        }
                    }
                    else
                    {
                        throw new Exception($"JSON-RPC 2.0 notifications are not supported.");
                    }
                }
                else
                {
                    throw new Exception($"JSON-RPC 2.0 message expected, but got something else.");
                }

                response.Add("jsonrpc", "2.0");

                var id = request.TryGetProperty("id", out var element2)
                    ? element2.GetInt32()
                    : throw new Exception("Unable to read the request message id.");

                response.Add("id", id);

                // send response
                await Utilities.SendToServerAsync(response, _commStream, cancellationToken);

            }
        });
    }

    private async Task<JsonNode?> ProcessInvocationAsync(
        JsonElement request, 
        CancellationToken cancellationToken
    )
    {
#warning Use strongly typed deserialization instead?

        JsonNode? result = default;

        var methodName = request.GetProperty("method").GetString();
        var @params = request.GetProperty("params");

        if (methodName == "initialize")
        {
            _sourceTypeName = @params[0].ToString();
        }

        else if (methodName == "upgradeSourceConfiguration")
        {
            if (_sourceTypeName is null)
                throw new Exception("The connection must be initialized with a type before invoking other methods.");

            var dataSourceType = _getDataSourceType(_sourceTypeName);
            var upgradedConfiguration = @params[0];

            if (dataSourceType.IsAssignableTo(typeof(IUpgradableDataSource)))
            {
                var upgradableDataSource = (IUpgradableDataSource)Activator.CreateInstance(dataSourceType)!;
                var timeoutTokenSource = new CancellationTokenSource(TimeSpan.FromMinutes(1));

                upgradedConfiguration = await upgradableDataSource.UpgradeSourceConfigurationAsync(
                    @params[0],
                    timeoutTokenSource.Token
                );
            }

            result = JsonSerializer.SerializeToNode(upgradedConfiguration, Utilities.JsonSerializerOptions);
        }

        else if (methodName == "setContext")
        {
            if (_sourceTypeName is null)
                throw new Exception("The connection must be initialized with a type before invoking other methods.");

            var rawContext = @params[0];

            var context = JsonSerializer
                .Deserialize<DataSourceContext<JsonElement>>(rawContext, Utilities.JsonSerializerOptions)!;

            var logger = new Logger(_commStream, _watchdogTimer, cancellationToken);
            var dataSourceType = _getDataSourceType(_sourceTypeName);
            var dataSource = (IDataSource)Activator.CreateInstance(dataSourceType)!;

            /* Find generic parameter */
            var dataSourceInterfaceTypes = dataSourceType.GetInterfaces();

            var genericInterface = dataSourceInterfaceTypes
                .FirstOrDefault(x =>
                    x.IsGenericType &&
                    x.GetGenericTypeDefinition() == typeof(IDataSource<>)
                );

            if (genericInterface is null)
                throw new Exception("Data sources must implement IDataSource<T>.");

            var configurationType = genericInterface.GenericTypeArguments[0];

            /* Invoke SetContextAsync */
            var methodInfo = typeof(RemoteCommunicator)
                .GetMethod(nameof(SetContextAsync), BindingFlags.NonPublic | BindingFlags.Instance)!;

            var genericMethod = methodInfo
                .MakeGenericMethod(configurationType);

            await (Task)genericMethod.Invoke(
                this,
                [
                    dataSource,
                    context.ResourceLocator,
                    context.SourceConfiguration,
                    context.RequestConfiguration,
                    logger,
                    cancellationToken
                ]
            )!;

            _logger = logger;
            _dataSource = dataSource;
        }

        else if (methodName == "getCatalogRegistrations")
        {
            if (_dataSource is null)
                throw new Exception("The data source context must be set before invoking other methods.");

            var path = @params[0].GetString()!;
            var registrations = await _dataSource.GetCatalogRegistrationsAsync(path, cancellationToken);

            result = JsonSerializer.SerializeToNode(registrations, Utilities.JsonSerializerOptions);
        }

        else if (methodName == "enrichCatalog")
        {
            if (_dataSource is null)
                throw new Exception("The data source context must be set before invoking other methods.");

            var originalCatalog = JsonSerializer.Deserialize<ResourceCatalog>(@params[0], Utilities.JsonSerializerOptions)!;
            var catalog = await _dataSource.EnrichCatalogAsync(originalCatalog, cancellationToken);

            result = JsonSerializer.SerializeToNode(catalog, Utilities.JsonSerializerOptions);
        }

        else if (methodName == "getTimeRange")
        {
            if (_dataSource is null)
                throw new Exception("The data source context must be set before invoking other methods.");

            var catalogId = @params[0].GetString()!;
            var (begin, end) = await _dataSource.GetTimeRangeAsync(catalogId, cancellationToken);

            result = new JsonObject()
            {
                ["begin"] = begin.ToString("o", CultureInfo.InvariantCulture),
                ["end"] = end.ToString("o", CultureInfo.InvariantCulture)
            };
        }

        else if (methodName == "getAvailability")
        {
            if (_dataSource is null)
                throw new Exception("The data source context must be set before invoking other methods.");

            var catalogId = @params[0].GetString()!;

            var beginString = @params[1].GetString()!;
            var begin = DateTime.ParseExact(beginString, "o", CultureInfo.InvariantCulture);

            var endString = @params[2].GetString()!;
            var end = DateTime.ParseExact(endString, "o", CultureInfo.InvariantCulture);

            var availability = await _dataSource.GetAvailabilityAsync(catalogId, begin, end, cancellationToken);

            result = availability;
        }

        else if (methodName == "read")
        {
            if (_dataSource is null)
                throw new Exception("The data source context must be set before invoking other methods.");

            var beginString = @params[0].GetString()!;
            var begin = DateTime.ParseExact(beginString, "o", CultureInfo.InvariantCulture).ToUniversalTime();

            var endString = @params[1].GetString()!;
            var end = DateTime.ParseExact(endString, "o", CultureInfo.InvariantCulture).ToUniversalTime();

            var remoteReadRequests = JsonSerializer.Deserialize<RemoteReadRequest[]>(@params[2], Utilities.JsonSerializerOptions)!;

            if (remoteReadRequests.Length > byte.MaxValue + 1)
                throw new Exception("A remote batch read must not contain more than 256 resources.");

            var readRequests = new ReadRequest[remoteReadRequests.Length];
            var streamedIndices = new HashSet<int>();
            _readDataResponses.Clear();
            var schema = CreateReadSchema();
            using var arrowWriter = new ArrowStreamWriter(_dataStream, schema);

            for (int i = 0; i < remoteReadRequests.Length; i++)
            {
                var (data, status) = ExtensibilityUtilities.CreateBuffers(remoteReadRequests[i].CatalogItem.Representation, begin, end);
                var index = i;

                Func<CancellationToken, Task> onCompleted = async ct =>
                {
                    await WriteReadRecordBatchAsync(arrowWriter, schema, index, data, status, remoteReadRequests[index].CatalogItem.Representation.ElementSize, ct);
                    streamedIndices.Add(index);
                };

                readRequests[i] = new ReadRequest(
                    remoteReadRequests[i].OriginalResourceName,
                    remoteReadRequests[i].CatalogItem,
                    data,
                    status,
                    onCompleted,
                    cancellationToken);
            }

            await _dataSource.ReadAsync(
                begin,
                end,
                readRequests,
                HandleReadDataAsync,
                new Progress<double>(),
                cancellationToken);

            for (int i = 0; i < readRequests.Length; i++)
            {
                if (!streamedIndices.Contains(i))
                {
                    await WriteReadRecordBatchAsync(arrowWriter, schema, i, readRequests[i].Data, readRequests[i].Status, remoteReadRequests[i].CatalogItem.Representation.ElementSize, cancellationToken);
                    streamedIndices.Add(i);
                }
            }

            await arrowWriter.WriteEndAsync(cancellationToken);
            await _dataStream.FlushAsync(cancellationToken);
        }

        // Add cancellation support?
        // https://github.com/microsoft/vs-streamjsonrpc/blob/main/doc/sendrequest.md#cancellation
        // https://github.com/Microsoft/language-server-protocol/blob/main/versions/protocol-2-x.md#cancelRequest
        else if (methodName == "$/cancelRequest")
        {
            //
        }

        // Add progress support?
        // https://github.com/microsoft/vs-streamjsonrpc/blob/main/doc/progresssupport.md
        else if (methodName == "$/progress")
        {
            //
        }

        // Add OOB stream support?
        // https://github.com/microsoft/vs-streamjsonrpc/blob/main/doc/oob_streams.md

        else
            throw new Exception($"Unknown method '{methodName}'.");

        return result;
    }

    private record RemoteReadRequest(
        string OriginalResourceName,
        CatalogItem CatalogItem);

    private sealed class ReadDataResponseBuilder
    {
        private readonly MemoryStream _data = new();

        public bool IsCompleted { get; private set; }

        public string? ErrorMessage { get; private set; }

        public void Add(ReadOnlySpan<byte> data)
        {
            if (IsCompleted)
                throw new Exception("The readData response received data after completion.");

            _data.Write(data);
        }

        public void Complete()
        {
            if (IsCompleted)
                throw new Exception("The readData response completed more than once.");

            IsCompleted = true;
        }

        public void Fail(string message)
        {
            if (IsCompleted)
                throw new Exception("The readData response failed after completion.");

            ErrorMessage = message;
            IsCompleted = true;
        }

        public byte[] ToArray() => _data.ToArray();
    }

    private Task SetContextAsync<T>(
        IDataSource<T?> dataSource,
        Uri? resourceLocator,
        JsonElement sourceConfiguration,
        IReadOnlyDictionary<string, JsonElement>? requestConfiguration,
        ILogger logger,
        CancellationToken cancellationToken
    )
    {
        var context = new DataSourceContext<T?>(
            ResourceLocator: resourceLocator,
            SourceConfiguration: JsonSerializer.Deserialize<T>(sourceConfiguration, Utilities.JsonSerializerOptions),
            RequestConfiguration: requestConfiguration
        );

        return dataSource.SetContextAsync(context, logger, cancellationToken);
    }

    private static Schema CreateReadSchema()
    {
        var fields = new[]
        {
            new Field("resourceIndex", new Int32Type(), nullable: false, metadata: []),
            new Field("offset", new Int64Type(), nullable: false, metadata: []),
            new Field("data", new BinaryType(), nullable: false, metadata: []),
            new Field("status", new BinaryType(), nullable: false, metadata: [])
        };

        return new Schema(fields, metadata: []);
    }

    private async Task WriteReadRecordBatchAsync(
        ArrowStreamWriter writer,
        Schema schema,
        int index,
        Memory<byte> data,
        Memory<byte> status,
        int elementSize,
        CancellationToken cancellationToken)
    {
        await _frameWriteLock.WaitAsync(cancellationToken);

        try
        {
            using var recordBatch = CreateReadRecordBatch(schema, index, 0, data, status, elementSize);
            await writer.WriteRecordBatchAsync(recordBatch, cancellationToken);
            await _dataStream.FlushAsync(cancellationToken);
        }
        finally
        {
            _frameWriteLock.Release();
        }
    }

    private static RecordBatch CreateReadRecordBatch(
        Schema schema,
        int index,
        long offset,
        ReadOnlyMemory<byte> data,
        ReadOnlyMemory<byte> status,
        int elementSize)
    {
        if (data.Length % elementSize != 0)
            throw new Exception("The remote read data buffer length is not a multiple of the representation element size.");

        var elementCount = data.Length / elementSize;

        if (status.Length != elementCount)
            throw new Exception("The remote read data and status buffers have different element counts.");

        var resourceIndexArray = new Int32Array.Builder().Append(index).Build(default);
        var offsetArray = new Int64Array.Builder().Append(offset).Build(default);
        var dataArray = new BinaryArray.Builder().Append(data.Span).Build(default);
        var statusArray = new BinaryArray.Builder().Append(status.Span).Build(default);

        return new RecordBatch(schema, [resourceIndexArray, offsetArray, dataArray, statusArray], 1);
    }

    private async Task HandleReadDataAsync(
        string resourcePath,
        DateTime begin,
        DateTime end,
        Memory<double> buffer,
        CancellationToken cancellationToken)
    {
        var requestId = Interlocked.Increment(ref _readDataRequestId);
        var readDataRequest = new JsonObject()
        {
            ["jsonrpc"] = "2.0",
            ["method"] = "readData",
            ["params"] = new JsonArray
                (
                    requestId,
                    resourcePath, 
                    begin.ToString("o", CultureInfo.InvariantCulture), 
                    end.ToString("o", CultureInfo.InvariantCulture
                )
            )
        };

        _logger.LogDebug("Read resource path {ResourcePath} from Nexus", resourcePath);

        await Utilities.SendToServerAsync(readDataRequest, _commStream, cancellationToken);
        _watchdogTimer.Restart();

        await ReadReadDataResponseAsync(requestId, buffer, cancellationToken);
        _watchdogTimer.Restart();
    }

    private async Task ReadReadDataResponseAsync(
        int requestId,
        Memory<double> target,
        CancellationToken cancellationToken)
    {
        await _readDataResponseReadLock.WaitAsync(cancellationToken);

        try
        {
            while (true)
            {
                if (_readDataResponses.TryGetValue(requestId, out var completedResponse) && completedResponse.IsCompleted)
                {
                    _readDataResponses.Remove(requestId);

                    if (completedResponse.ErrorMessage is not null)
                        throw new Exception(completedResponse.ErrorMessage);

                    CopyReadDataArrowStream(completedResponse.ToArray(), target);
                    return;
                }

                var frameType = await ReadByteAsync(cancellationToken);
                var frameRequestId = await ReadInt32LittleEndianAsync(cancellationToken);
                var builder = GetReadDataResponseBuilder(frameRequestId);

                if (frameType == 0x01) // Data
                {
                    var payloadLength = await ReadInt32LittleEndianAsync(cancellationToken);

                    if (payloadLength <= 0 || payloadLength > MaximumBatchStreamPayloadLength)
                        throw new Exception("The readData response returned an invalid payload length.");

                    var payload = new byte[payloadLength];
                    await _dataStream.ReadExactlyAsync(payload, cancellationToken);
                    builder.Add(payload);
                }

                else if (frameType == 0x02) // Error
                {
                    var messageLength = await ReadInt32LittleEndianAsync(cancellationToken);

                    if (messageLength < 0 || messageLength > MaximumBatchStreamErrorMessageLength)
                        throw new Exception("The readData response returned an invalid error message length.");

                    var messageBytes = new byte[messageLength];
                    await _dataStream.ReadExactlyAsync(messageBytes, cancellationToken);
                    builder.Fail(Encoding.UTF8.GetString(messageBytes));
                }

                else if (frameType == 0x03) // End
                {
                    builder.Complete();
                }

                else
                {
                    throw new Exception($"Unknown readData response frame type '{frameType}'.");
                }
            }
        }
        finally
        {
            _readDataResponseReadLock.Release();
        }
    }

    private ReadDataResponseBuilder GetReadDataResponseBuilder(int requestId)
    {
        if (!_readDataResponses.TryGetValue(requestId, out var builder))
        {
            builder = new ReadDataResponseBuilder();
            _readDataResponses[requestId] = builder;
        }

        return builder;
    }

    private static void CopyReadDataArrowStream(byte[] data, Memory<double> target)
    {
        using var stream = new MemoryStream(data);
        using var reader = new ArrowStreamReader(stream);
        var copiedValues = 0;

        while (true)
        {
            using var recordBatch = reader.ReadNextRecordBatch();

            if (recordBatch is null)
                break;

            var (offsetArray, valuesArray) = GetReadDataArrowArrays(recordBatch);

            for (var rowIndex = 0; rowIndex < recordBatch.Length; rowIndex++)
            {
                var offset = offsetArray.GetValue(rowIndex) ?? throw new Exception("The readData response returned a null offset.");
                var length = valuesArray.GetValueLength(rowIndex);

                if (offset < 0 || offset > int.MaxValue)
                    throw new Exception("The readData response returned an invalid offset.");

                var targetOffset = checked((int)offset);

                if (targetOffset < copiedValues || targetOffset + length > target.Length)
                    throw new Exception("The readData response returned values outside the requested range.");

                CopyReadDataArrowValues(valuesArray, rowIndex, length, target.Slice(targetOffset, length));
                copiedValues = targetOffset + length;
            }
        }

        if (copiedValues != target.Length)
            throw new Exception("Data returned by Nexus have an unexpected length");
    }

    private static (Int64Array OffsetArray, ListArray ValuesArray) GetReadDataArrowArrays(RecordBatch recordBatch)
    {
        var fields = recordBatch.Schema.FieldsList;

        if (fields.Count != 2 ||
            fields[0].Name != "offset" || fields[0].DataType is not Int64Type ||
            fields[1].Name != "values" || fields[1].DataType is not ListType { ValueDataType: DoubleType })
            throw new Exception("The readData response returned an invalid Arrow schema.");

        Int64Array? offsetArray = null;
        ListArray? valuesArray = null;
        var columnIndex = 0;

        foreach (var array in recordBatch.Arrays)
        {
            switch (columnIndex)
            {
                case 0 when array is Int64Array current:
                    offsetArray = current;
                    break;
                case 1 when array is ListArray current:
                    valuesArray = current;
                    break;
                default:
                    throw new Exception("The readData response returned an invalid Arrow schema.");
            }

            columnIndex++;
        }

        if (columnIndex != 2 || offsetArray is null || valuesArray is null)
            throw new Exception("The readData response returned an invalid Arrow schema.");

        return (offsetArray, valuesArray);
    }

    private static void CopyReadDataArrowValues(ListArray valuesArray, int rowIndex, int length, Memory<double> target)
    {
        if (valuesArray.Values is not DoubleArray doubleArray)
            throw new Exception("The readData response returned an invalid Arrow values array.");

        var valueOffset = valuesArray.ValueOffsets[rowIndex];
        doubleArray.Values.Slice(valueOffset, length).CopyTo(target.Span);
    }

    private async Task<byte> ReadByteAsync(CancellationToken cancellationToken)
    {
        var buffer = new byte[1];
        await _dataStream.ReadExactlyAsync(buffer, cancellationToken);
        return buffer[0];
    }

    private async Task<int> ReadInt32LittleEndianAsync(CancellationToken cancellationToken)
    {
        var buffer = new byte[4];
        await _dataStream.ReadExactlyAsync(buffer, cancellationToken);
        return BinaryPrimitives.ReadInt32LittleEndian(buffer);
    }

    private int ReadSize(NetworkStream currentStream)
    {
        Span<byte> sizeBuffer = stackalloc byte[4];
        currentStream.InternalReadExactly(sizeBuffer);
        MemoryExtensions.Reverse(sizeBuffer);

        var size = BitConverter.ToInt32(sizeBuffer);
        return size;
    }
}

internal static class Utilities
{
    private static readonly SemaphoreSlim _semaphore = new(1, 1);

    static Utilities()
    {
        JsonSerializerOptions = new JsonSerializerOptions()
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase
        };
        
        JsonSerializerOptions.Converters.Add(new JsonStringEnumConverter());
        JsonSerializerOptions.Converters.Add(new RoundtripDateTimeConverter());
    }

    public static JsonSerializerOptions JsonSerializerOptions { get; }

    public static async Task SendToServerAsync(JsonNode response, NetworkStream currentStream, CancellationToken cancellationToken)
    {
        var encodedResponse = JsonSerializer.SerializeToUtf8Bytes(response, JsonSerializerOptions);
        var messageLength = BitConverter.GetBytes(encodedResponse.Length);
        System.Array.Reverse(messageLength);

        await _semaphore.WaitAsync(TimeSpan.FromMinutes(1), cancellationToken);

        try
        {
            await currentStream.WriteAsync(messageLength, cancellationToken);
            await currentStream.WriteAsync(encodedResponse, cancellationToken);
            await currentStream.FlushAsync();
        }
        finally
        {
            _semaphore.Release();
        }
    }
}

internal static class StreamExtensions
{
    public static void InternalReadExactly(this Stream stream, Span<byte> buffer)
    {
        while (buffer.Length > 0)
        {
            var read = stream.Read(buffer);

            if (read == 0)
                throw new Exception("The stream has been closed");

            buffer = buffer[read..];
        }
    }
}

internal class RoundtripDateTimeConverter : JsonConverter<DateTime>
{
    public override DateTime Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        if (!DateTime.TryParseExact
            (
                reader.GetString(), 
                "o",
                CultureInfo.InvariantCulture,
                DateTimeStyles.AdjustToUniversal,
                out var dateTime
            )
        )
        {
            throw new JsonException();
        }

        return dateTime;
    }

    public override void Write(
        Utf8JsonWriter writer, 
        DateTime value,
        JsonSerializerOptions options
    )
    {
        writer.WriteStringValue(value.ToString("o", CultureInfo.InvariantCulture));
    }
}
