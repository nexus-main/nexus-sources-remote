using Microsoft.Extensions.Logging;
using Nexus.DataModel;
using Nexus.Extensibility;
using System.Buffers;
using System.Diagnostics;
using System.Globalization;
using System.Net.Sockets;
using System.Reflection;
using System.Runtime.InteropServices;
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
    private readonly NetworkStream _commStream;

    private readonly NetworkStream _dataStream;

    private readonly Func<string, Type> _getDataSourceType;

    private readonly Stopwatch _watchdogTimer = new();

    private ILogger _logger = default!;

    private string? _sourceTypeName = default;

    private IDataSource? _dataSource = default;

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
                Memory<byte> data = default;
                Memory<byte> status = default;
                JsonObject? response;

                if (request.TryGetProperty("jsonrpc", out var element) &&
                    element.ValueKind == JsonValueKind.String &&
                    element.GetString() == "2.0")
                {
                    if (request.TryGetProperty("id", out var _))
                    {
                        try
                        {
                            (var result, data, status) = await ProcessInvocationAsync(request, cancellationToken);
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

                // send data
                if (!data.Equals(default) && !status.Equals(default))
                {
                    await _dataStream.WriteAsync(data);
                    await _dataStream.WriteAsync(status);
                    await _dataStream.FlushAsync();
                }
            }
        });
    }

    private async Task<(JsonNode?, Memory<byte>, Memory<byte>)> ProcessInvocationAsync(
        JsonElement request, 
        CancellationToken cancellationToken
    )
    {
#warning Use strongly typed deserialization instead?

        JsonNode? result = default;
        Memory<byte> data = default;
        Memory<byte> status = default;

        var methodName = request.GetProperty("method").GetString();
        var @params = request.GetProperty("params");

        if (methodName == "initialize")
        {
            _sourceTypeName = @params[0].ToString();
            result = 1; // API version
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

        else if (methodName == "readSingle")
        {
            if (_dataSource is null)
                throw new Exception("The data source context must be set before invoking other methods.");

            var beginString = @params[0].GetString()!;
            var begin = DateTime.ParseExact(beginString, "o", CultureInfo.InvariantCulture).ToUniversalTime();

            var endString = @params[1].GetString()!;
            var end = DateTime.ParseExact(endString, "o", CultureInfo.InvariantCulture).ToUniversalTime();

            var originalResourceName = @params[2].GetString()!;

            var catalogItem = JsonSerializer.Deserialize<CatalogItem>(@params[3], Utilities.JsonSerializerOptions)!;
            (data, status) = ExtensibilityUtilities.CreateBuffers(catalogItem.Representation, begin, end);
            var readRequest = new ReadRequest(originalResourceName, catalogItem, data, status, _ => Task.CompletedTask, CancellationToken.None);

            await _dataSource.ReadAsync(
                begin,
                end,
                [readRequest],
                HandleReadDataAsync,
                new Progress<double>(),
                CancellationToken.None
            );
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

        return (result, data, status);
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

    private async Task HandleReadDataAsync(
        string resourcePath,
        DateTime begin,
        DateTime end,
        Memory<double> buffer,
        CancellationToken cancellationToken)
    {
        var readDataRequest = new JsonObject()
        {
            ["jsonrpc"] = "2.0",
            ["method"] = "readData",
            ["params"] = new JsonArray
                (
                    resourcePath, 
                    begin.ToString("o", CultureInfo.InvariantCulture), 
                    end.ToString("o", CultureInfo.InvariantCulture
                )
            )
        };

        _logger.LogDebug("Read resource path {ResourcePath} from Nexus", resourcePath);

        await Utilities.SendToServerAsync(readDataRequest, _commStream, cancellationToken);
        _watchdogTimer.Restart();

        var size = ReadSize(_dataStream);

        if (size != buffer.Length * sizeof(double))
            throw new Exception("Data returned by Nexus have an unexpected length");

        _logger.LogTrace("Try to read {ByteCount} bytes from Nexus", size);

        _dataStream.InternalReadExactly(MemoryMarshal.AsBytes(buffer.Span));
        _watchdogTimer.Restart();
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
        Array.Reverse(messageLength);

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

internal class CastMemoryManager<TFrom, TTo>(Memory<TFrom> from) : MemoryManager<TTo>
        where TFrom : struct
        where TTo : struct
{
    private readonly Memory<TFrom> _from = from;

    public override Span<TTo> GetSpan() => MemoryMarshal.Cast<TFrom, TTo>(_from.Span);

    protected override void Dispose(bool disposing)
    {
        //
    }

    public override MemoryHandle Pin(int elementIndex = 0) => throw new NotSupportedException("CastMemoryManager does not support pinning.");

    public override void Unpin() => throw new NotSupportedException("CastMemoryManager does not support unpinning.");
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
