using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using Nexus.DataModel;
using Nexus.Extensibility;
using System.Buffers;
using System.Reflection;
using System.Text.Json;
using System.Text.RegularExpressions;

namespace Nexus.Sources;

public record RemoteSettings(
    Uri RemoteUrl,
    string RemoteType,
    JsonElement RemoteConfiguration
);

[ExtensionDescription(
    "Provides access to remote databases",
    "https://github.com/nexus-main/nexus-sources-remote",
    "https://github.com/nexus-main/nexus-sources-remote")] 
public partial class Remote : IDataSource<RemoteSettings>, IUpgradableDataSource, IDisposable
{
    private const int DEFAULT_AGENT_PORT = 56145;
    internal const int MAX_BATCH_STREAM_PAYLOAD_LENGTH = 4 * 1024 * 1024;
    internal const int MAX_ERROR_MESSAGE_LENGTH = 64 * 1024;

    private ReadDataHandler? _readData;

    private Dictionary<string, int>? _inFlightRequests;

    private RemoteCommunicator _communicator = default!;
    
    private IJsonRpcServer _rpcServer = default!;

    /* Possible features to be implemented for this data source:
     * 
     * Transports: 
     *      - anonymous pipes (done)
     *      - named pipes client
     *      - tcp client
     *      - shared memory
     *      - ...
     *      
     * Protocols:
     *      - JsonRpc + binary data stream (done)
     *      - 0mq
     *      - messagepack
     *      - gRPC
     *      - ...
     */

    private DataSourceContext<RemoteSettings> Context { get; set; } = default!;

    public async Task<JsonElement> UpgradeSourceConfigurationAsync(
        JsonElement configuration,
        CancellationToken cancellationToken
    )
    {
        var thisConfiguration = JsonSerializer
            .Deserialize<RemoteSettings>(configuration, Utilities.JsonSerializerOptions)!;

        var (communicator, rpcServer) = await CreateRemoteCommunicatorAsync(
            thisConfiguration.RemoteUrl,
            thisConfiguration.RemoteType,
            (_, _, _, _) => throw new Exception("This should never happen."),
            NullLogger.Instance,
            cancellationToken
        );

        using var comm = communicator;

        var upgradedRemoteConfiguration = await rpcServer.UpgradeSourceConfigurationAsync(
            thisConfiguration.RemoteConfiguration,
            cancellationToken
        );

        var upgradedThisConfiguration = thisConfiguration with 
        { 
            RemoteConfiguration = upgradedRemoteConfiguration 
        };

        return JsonSerializer.SerializeToElement(upgradedThisConfiguration, Utilities.JsonSerializerOptions);
    }

    public async Task SetContextAsync(
        DataSourceContext<RemoteSettings> context,
        ILogger logger,
        CancellationToken cancellationToken)
    {
        Context = context;

        (_communicator, _rpcServer) = await CreateRemoteCommunicatorAsync(
            context.SourceConfiguration.RemoteUrl, 
            context.SourceConfiguration.RemoteType,
            HandleReadDataAsync,
            logger,
            cancellationToken
        );

        logger.LogTrace("Set context to remote client");

        var resourceLocator = Context.ResourceLocator;
        var sourceConfiguration = context.SourceConfiguration.RemoteConfiguration;

        var subContext = new DataSourceContext<JsonElement>(
            resourceLocator,
            sourceConfiguration,
            context.RequestConfiguration
        );

        var timeoutTokenSource = new CancellationTokenSource(TimeSpan.FromMinutes(1));

        await _rpcServer.SetContextAsync(
            subContext, 
            timeoutTokenSource.Token
        );
    }

    public async Task<CatalogRegistration[]> GetCatalogRegistrationsAsync(
        string path,
        CancellationToken cancellationToken)
    {
        var timeoutTokenSource = new CancellationTokenSource(TimeSpan.FromMinutes(1));
        cancellationToken.Register(timeoutTokenSource.Cancel);

        var registrations = await _rpcServer
            .GetCatalogRegistrationsAsync(path, timeoutTokenSource.Token);

        return registrations;
    }

    public async Task<ResourceCatalog> EnrichCatalogAsync(
        ResourceCatalog catalog,
        CancellationToken cancellationToken)
    {
        var timeoutTokenSource = new CancellationTokenSource(TimeSpan.FromMinutes(1));
        cancellationToken.Register(timeoutTokenSource.Cancel);

        var newCatalog = await _rpcServer
            .EnrichCatalogAsync(catalog, timeoutTokenSource.Token);

        return newCatalog;
    }

    public async Task<CatalogTimeRange> GetTimeRangeAsync(
        string catalogId,
        CancellationToken cancellationToken)
    {
        var timeoutTokenSource = new CancellationTokenSource(TimeSpan.FromMinutes(1));
        cancellationToken.Register(timeoutTokenSource.Cancel);

        var response = await _rpcServer
            .GetTimeRangeAsync(catalogId, timeoutTokenSource.Token);

        var begin = response.Begin.ToUniversalTime();
        var end = response.End.ToUniversalTime();

        return new CatalogTimeRange(begin, end);
    }

    public async Task<double> GetAvailabilityAsync(
        string catalogId,
        DateTime begin,
        DateTime end,
        CancellationToken cancellationToken)
    {
        var timeoutTokenSource = new CancellationTokenSource(TimeSpan.FromMinutes(1));
        cancellationToken.Register(timeoutTokenSource.Cancel);

        var availability = await _rpcServer
            .GetAvailabilityAsync(catalogId, begin, end, timeoutTokenSource.Token);

        return availability;
    }

    public async Task ReadAsync(
        DateTime begin,
        DateTime end,
        ReadRequest[] requests,
        ReadDataHandler readData,
        IProgress<double> progress,
        CancellationToken cancellationToken)
    {
        _readData = readData;

        try
        {
            var remoteRequests = requests
                .Select(request => new RemoteReadRequest(request.OriginalResourceName, request.CatalogItem))
                .ToArray();

            _inFlightRequests = new Dictionary<string, int>(requests.Length);

            for (int i = 0; i < requests.Length; i++)
            {
                var resourcePath = $"{requests[i].CatalogItem.Catalog.Id}/{requests[i].OriginalResourceName}/{requests[i].CatalogItem.Representation.SamplePeriod.ToUnitString()}";
                _inFlightRequests[resourcePath] = i;
            }

            try
            {
                var rpcTask = _rpcServer.ReadAsync(begin, end, remoteRequests, cancellationToken);
                var counter = 0.0;
                var completedRequests = new bool[requests.Length];
                var receivedDataLengths = new int[requests.Length];
                var receivedStatusLengths = new int[requests.Length];

                for (int i = 0; i < requests.Length; i++)
                {
                    if (GetExpectedPayloadLength(requests[i]) == 0)
                    {
                        completedRequests[i] = true;
                        await requests[i].CompleteAsync();
                        progress.Report(++counter / requests.Length);
                    }
                }

                using var reader = _communicator.CreateArrowStreamReader();

                while (true)
                {
                    using var recordBatch = await ReadNextRecordBatchAsync(reader, rpcTask, cancellationToken);

                    if (recordBatch is null)
                    {
                        await rpcTask;

                        if (completedRequests.Any(completedRequest => !completedRequest))
                            throw new RemoteException("The remote read operation completed before all resources were received.");

                        break;
                    }

                    var (resourceIndexArray, offsetArray, dataArray, statusArray) = GetArrowArrays(recordBatch);

                    for (var rowIndex = 0; rowIndex < recordBatch.Length; rowIndex++)
                    {
                        var index = resourceIndexArray.GetValue(rowIndex) ?? throw new RemoteException("The remote read operation returned a null resource index.");
                        ValidateFrameIndex(index, requests.Length);

                        var offset = offsetArray.GetValue(rowIndex) ?? throw new RemoteException("The remote read operation returned a null offset.");
                        CopyArrowPayload(
                            requests[index],
                            offset,
                            dataArray.GetBytes(rowIndex),
                            statusArray.GetBytes(rowIndex),
                            ref receivedDataLengths[index],
                            ref receivedStatusLengths[index]);

                        if (!completedRequests[index] &&
                            receivedDataLengths[index] == requests[index].Data.Length &&
                            receivedStatusLengths[index] == requests[index].Status.Length)
                        {
                            completedRequests[index] = true;
                            await requests[index].CompleteAsync();
                            progress.Report(++counter / requests.Length);
                        }
                    }
                }
            }
            finally
            {
                _inFlightRequests = null;
            }
        }
        finally
        {
            _readData = null;
        }
    }

    private static void ValidateFrameIndex(int index, int requestCount)
    {
        if (index < 0 || index >= requestCount)
            throw new RemoteException("The remote read operation returned an invalid resource index.");
    }

    private static (Int32Array ResourceIndexArray, Int64Array OffsetArray, BinaryArray DataArray, BinaryArray StatusArray) GetArrowArrays(RecordBatch recordBatch)
    {
        var fields = recordBatch.Schema.FieldsList;

        if (fields.Count != 4 ||
            fields[0].Name != "resourceIndex" || fields[0].DataType is not Int32Type ||
            fields[1].Name != "offset" || fields[1].DataType is not Int64Type ||
            fields[2].Name != "data" || fields[2].DataType is not BinaryType ||
            fields[3].Name != "status" || fields[3].DataType is not BinaryType)
            throw new RemoteException("The remote read operation returned an invalid Arrow schema.");

        Int32Array? resourceIndexArray = null;
        Int64Array? offsetArray = null;
        BinaryArray? dataArray = null;
        BinaryArray? statusArray = null;
        var columnIndex = 0;

        foreach (var array in recordBatch.Arrays)
        {
            switch (columnIndex)
            {
                case 0 when array is Int32Array current:
                    resourceIndexArray = current;
                    break;
                case 1 when array is Int64Array current:
                    offsetArray = current;
                    break;
                case 2 when array is BinaryArray current:
                    dataArray = current;
                    break;
                case 3 when array is BinaryArray current:
                    statusArray = current;
                    break;
                default:
                    throw new RemoteException("The remote read operation returned an invalid Arrow schema.");
            }

            columnIndex++;
        }

        if (columnIndex != 4 || resourceIndexArray is null || offsetArray is null || dataArray is null || statusArray is null)
            throw new RemoteException("The remote read operation returned an invalid Arrow schema.");

        return (resourceIndexArray, offsetArray, dataArray, statusArray);
    }

    private static void CopyArrowPayload(
        ReadRequest request,
        long offset,
        ReadOnlySpan<byte> data,
        ReadOnlySpan<byte> status,
        ref int receivedDataLength,
        ref int receivedStatusLength)
    {
        var elementSize = request.CatalogItem.Representation.ElementSize;

        if (offset < 0 || offset > int.MaxValue)
            throw new RemoteException("The remote read operation returned an invalid offset.");

        if (data.Length % elementSize != 0)
            throw new RemoteException("The remote read operation returned a data payload with an unexpected length.");

        var elementCount = data.Length / elementSize;

        if (status.Length != elementCount)
            throw new RemoteException("The remote read operation returned data and status payloads with different element counts.");

        var targetElementOffset = checked((int)offset);
        var targetDataOffset = checked(targetElementOffset * elementSize);

        if (targetDataOffset < receivedDataLength || targetElementOffset < receivedStatusLength)
            throw new RemoteException("The remote read operation returned payloads out of order.");

        if (targetDataOffset + data.Length > request.Data.Length || targetElementOffset + status.Length > request.Status.Length)
            throw new RemoteException("The remote read operation returned payloads outside the requested range.");

        data.CopyTo(request.Data.Span.Slice(targetDataOffset, data.Length));
        status.CopyTo(request.Status.Span.Slice(targetElementOffset, status.Length));
        receivedDataLength = targetDataOffset + data.Length;
        receivedStatusLength = targetElementOffset + status.Length;
    }

    private static async Task<RecordBatch?> ReadNextRecordBatchAsync(ArrowStreamReader reader, Task rpcTask, CancellationToken cancellationToken)
    {
        var readTask = reader.ReadNextRecordBatchAsync(cancellationToken).AsTask();
#pragma warning disable VSTHRD003 // Intentionally race the RPC task to surface remote failures before waiting for data.
        var completedTask = await Task.WhenAny(readTask, rpcTask).ConfigureAwait(false);

        if (completedTask == rpcTask)
            await rpcTask.ConfigureAwait(false);

        return await readTask.ConfigureAwait(false);
#pragma warning restore VSTHRD003
    }

    private static int GetExpectedPayloadLength(ReadRequest request)
    {
        return request.Data.Length + request.Status.Length;
    }

    private static async Task<(RemoteCommunicator, IJsonRpcServer)> CreateRemoteCommunicatorAsync(
        Uri remoteUrl,
        string remoteType,
        Func<int, string, DateTime, DateTime, Task> readData,
        ILogger logger,
        CancellationToken cancellationToken
    )
    {
        if (remoteUrl is null || remoteUrl.Scheme != "tcp")
            throw new ArgumentException("The resource locator parameter URI must be set with the 'tcp' scheme.");

        var host = remoteUrl.Host;
        var port = remoteUrl.Port;

        if (port == -1)
            port = DEFAULT_AGENT_PORT;

        var communicator = new RemoteCommunicator(
            host,
            port,
            readData,
            logger
        );

        var timeoutTokenSource = new CancellationTokenSource(TimeSpan.FromMinutes(1));
        cancellationToken.Register(timeoutTokenSource.Cancel);

        var rpcServer = await communicator.ConnectAsync(timeoutTokenSource.Token);
        await rpcServer.InitializeAsync(remoteType, timeoutTokenSource.Token);

        return (communicator, rpcServer);
    }

    // copy from Nexus -> DataModelUtilities

    [GeneratedRegex(@"^(?'catalog'.*)\/(?'resource'.*)\/(?'sample_period'[0-9]+_[a-zA-Z]+)(?:_(?'kind'[^\(#\s]+))?(?:\((?'parameters'.*)\))?(?:#(?'fragment'.*))?$", RegexOptions.Compiled)]
    private partial Regex ResourcePathEvaluator { get; }

    private static readonly MethodInfo _toSamplePeriodMethodInfo = typeof(DataModelExtensions)
        .GetMethod("ToSamplePeriod", BindingFlags.Static | BindingFlags.NonPublic) ?? throw new Exception("Unable to locate ToSamplePeriod method.");

    private async Task HandleReadDataAsync(int requestId, string resourcePath, DateTime begin, DateTime end)
    {
        var timeoutTokenSource = new CancellationTokenSource(TimeSpan.FromMinutes(1));

        try
        {
            // cycle detection
            if (_inFlightRequests is not null && _inFlightRequests.ContainsKey(resourcePath))
                throw new RemoteException("Cyclic read detected.");

            // copy of _readData handler
            var localReadData = _readData ?? throw new InvalidOperationException("Unable to read data without previous invocation of the ReadAsync method.");

            // find sample period
            var match = ResourcePathEvaluator.Match(resourcePath);

            if (!match.Success)
                throw new Exception("Invalid resource path");

            var samplePeriod = (TimeSpan)_toSamplePeriodMethodInfo.Invoke(null, [
                match.Groups["sample_period"].Value
            ])!;

            // find buffer length and rent buffer
            var length = (int)((end - begin).Ticks / samplePeriod.Ticks);

            using var memoryOwner = MemoryPool<double>.Shared.Rent(length);
            var buffer = memoryOwner.Memory[..length];

            // read data
            await localReadData(resourcePath, begin, end, buffer, timeoutTokenSource.Token);
            var byteBuffer = new CastMemoryManager<double, byte>(buffer).Memory;

            // write to communicator
            await _communicator.WriteReadDataResponseAsync(requestId, byteBuffer, timeoutTokenSource.Token);
        }
        catch (Exception ex)
        {
            await _communicator.WriteReadDataResponseErrorAsync(requestId, ex.Message, timeoutTokenSource.Token);
        }
    }

    #region IDisposable

    private bool _disposedValue;

    protected virtual void Dispose(bool disposing)
    {
        if (!_disposedValue)
        {
            if (disposing)
            {
                _communicator?.Dispose();
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
