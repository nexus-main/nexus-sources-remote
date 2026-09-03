using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Nexus.DataModel;
using Nexus.Extensibility;
using System.Buffers;
using System.Reflection;
using System.Text;
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
    private const int MAX_ERROR_MESSAGE_LENGTH = 64 * 1024;

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
            (_, _, _) => throw new Exception("This should never happen."),
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

                while (true)
                {
                    var frameType = await _communicator.ReadByteAsync(cancellationToken);

                    if (frameType == 0x01) // Data
                    {
                        var index = await _communicator.ReadInt32BigEndianAsync(cancellationToken);
                        ValidateFrameIndex(index, requests.Length);
                        await _communicator.ReadRawAsync(requests[index].Data, cancellationToken);
                        await _communicator.ReadRawAsync(requests[index].Status, cancellationToken);
                        await requests[index].CompleteAsync(cancellationToken);
                        progress.Report(++counter / requests.Length);
                    }

                    else if (frameType == 0x02) // Error
                    {
                        var index = await _communicator.ReadInt32BigEndianAsync(cancellationToken);
                        ValidateFrameIndex(index, requests.Length);
                        var msgLen = await ReadErrorMessageLengthAsync(cancellationToken);
                        var msgBytes = new byte[msgLen];
                        await _communicator.ReadRawAsync(msgBytes, cancellationToken);
                        requests[index].Data.Span.Clear();
                        requests[index].Status.Span.Clear();
                        await requests[index].CompleteAsync(cancellationToken);
                        progress.Report(++counter / requests.Length);
                    }

                    else if (frameType == 0x03) // End
                    {
                        var count = await _communicator.ReadInt32BigEndianAsync(cancellationToken);

                        if (count != requests.Length)
                            throw new RemoteException("The remote read operation completed with an invalid result count.");

                        var hadError = await _communicator.ReadByteAsync(cancellationToken);
                        var msgLen = await ReadErrorMessageLengthAsync(cancellationToken);
                        var msgBytes = new byte[msgLen];
                        await _communicator.ReadRawAsync(msgBytes, cancellationToken);
                        var errorMsg = Encoding.UTF8.GetString(msgBytes);

                        await rpcTask;

                        if (hadError != 0)
                            throw new RemoteException(string.IsNullOrEmpty(errorMsg)
                                ? "The remote read operation failed."
                                : errorMsg);

                        break;
                    }

                    else
                    {
                        throw new Exception($"Unknown frame type '{frameType}'.");
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

    private async Task<int> ReadErrorMessageLengthAsync(CancellationToken cancellationToken)
    {
        var messageLength = await _communicator.ReadInt32BigEndianAsync(cancellationToken);

        if (messageLength < 0 || messageLength > MAX_ERROR_MESSAGE_LENGTH)
            throw new RemoteException("The remote read operation returned an invalid error message length.");

        return messageLength;
    }

    private static async Task<(RemoteCommunicator, IJsonRpcServer)> CreateRemoteCommunicatorAsync(
        Uri remoteUrl,
        string remoteType,
        Func<string, DateTime, DateTime, Task> readData,
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

    private async Task HandleReadDataAsync(string resourcePath, DateTime begin, DateTime end)
    {
        // cycle detection
        if (_inFlightRequests is not null && _inFlightRequests.ContainsKey(resourcePath))
            throw new RemoteException("Cyclic read detected.");

        // copy of _readData handler
        var localReadData = _readData ?? throw new InvalidOperationException("Unable to read data without previous invocation of the ReadAsync method.");

        // timeout token source
        var timeoutTokenSource = new CancellationTokenSource(TimeSpan.FromMinutes(1));

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
        await _communicator.WriteRawAsync(byteBuffer, timeoutTokenSource.Token);
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
