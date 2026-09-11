using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
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

    private ReadDataHandler? _readData;

    private static readonly int API_LEVEL = 1;

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
            var counter = 0.0;

            foreach (var request in requests)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var timeoutTokenSource = new CancellationTokenSource(TimeSpan.FromMinutes(1));
                cancellationToken.Register(timeoutTokenSource.Cancel);

                await _rpcServer
                    .ReadSingleAsync(begin, end, request.OriginalResourceName, request.CatalogItem, timeoutTokenSource.Token);

                await _communicator.ReadRawAsync(request.Data, timeoutTokenSource.Token);
                await _communicator.ReadRawAsync(request.Status, timeoutTokenSource.Token);
                await request.CompleteAsync();

                progress.Report(++counter / requests.Length);
            }
        }
        finally
        {
            _readData = null;
        }
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
        var apiVersion = await rpcServer.InitializeAsync(remoteType, timeoutTokenSource.Token);

        if (apiVersion < 1 || apiVersion > API_LEVEL)
            throw new Exception($"The API level '{apiVersion}' is not supported.");

        return (communicator, rpcServer);
    }

    // copy from Nexus -> DataModelUtilities

    [GeneratedRegex(@"^(?'catalog'.*)\/(?'resource'.*)\/(?'sample_period'[0-9]+_[a-zA-Z]+)(?:_(?'kind'[^\(#\s]+))?(?:\((?'parameters'.*)\))?(?:#(?'fragment'.*))?$", RegexOptions.Compiled)]
    private partial Regex ResourcePathEvaluator { get; }

    private static readonly MethodInfo _toSamplePeriodMethodInfo = typeof(DataModelExtensions)
        .GetMethod("ToSamplePeriod", BindingFlags.Static | BindingFlags.NonPublic) ?? throw new Exception("Unable to locate ToSamplePeriod method.");

    private async Task HandleReadDataAsync(string resourcePath, DateTime begin, DateTime end)
    {
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
