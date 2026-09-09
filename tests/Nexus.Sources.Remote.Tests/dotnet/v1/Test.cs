using System.Buffers;
using System.Globalization;
using System.Runtime.InteropServices;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using Nexus.DataModel;
using Nexus.Extensibility;

namespace Nexus.Sources;

public record TestSettings(
    string LogMessage
);

public class TestBase : IUpgradableDataSource
{
    public virtual Task<JsonElement> UpgradeSourceConfigurationAsync(
        JsonElement configuration, 
        CancellationToken cancellationToken
    )
    {
        var jsonNode = JsonSerializer.SerializeToNode(configuration)!;
        jsonNode["foo"] = jsonNode["logMessage"]!.DeepClone();
        var upgradedConfiguration = JsonSerializer.SerializeToElement(jsonNode, JsonSerializerOptions.Web);

        return Task.FromResult(upgradedConfiguration);
    }
}

/* Note: Inherit from StructuredFileDataSource would be possible 
 * but collides with ReadAndModifyNexusData method
 */
public class Test : TestBase, IDataSource<TestSettings>
{
    private DataSourceContext<TestSettings> _context = default!;

    public override async Task<JsonElement> UpgradeSourceConfigurationAsync(
        JsonElement configuration,
        CancellationToken cancellationToken
    )
    {
        configuration = await base.UpgradeSourceConfigurationAsync(configuration, cancellationToken);
        var jsonNode = JsonSerializer.SerializeToNode(configuration)!;
        jsonNode["bar"] = jsonNode["logMessage"]!.DeepClone();
        var upgradedConfiguration = JsonSerializer.SerializeToElement(jsonNode, JsonSerializerOptions.Web);

        return upgradedConfiguration;
    }

    public Task SetContextAsync(
        DataSourceContext<TestSettings> context, 
        ILogger logger, 
        CancellationToken cancellationToken
    )
    {
        _context = context;

        if (context.ResourceLocator is null)
            throw new Exception($"Resource locator is required.");

        if (context.ResourceLocator.Scheme != "file")
            throw new Exception($"Expected 'file' URI scheme, but got '{context.ResourceLocator.Scheme}'.");

        logger.LogInformation(context.SourceConfiguration.LogMessage);
        
        return Task.CompletedTask;
    }

    public Task<CatalogRegistration[]> GetCatalogRegistrationsAsync(string path, CancellationToken cancellationToken)
    {
        if (path == "/")
            return Task.FromResult(new CatalogRegistration[]
                {
                    new CatalogRegistration("/A/B/C", "Test catalog /A/B/C."),
                    new CatalogRegistration("/D/E/F", "Test catalog /D/E/F."),
                });

        else
            return Task.FromResult(new CatalogRegistration[0]);
    }

    public Task<ResourceCatalog> EnrichCatalogAsync(ResourceCatalog catalog, CancellationToken cancellationToken)
    {
        if (catalog.Id == "/A/B/C")
        {
            var representation1 = new Representation(NexusDataType.INT64, TimeSpan.FromSeconds(1));

            var resource1 = new ResourceBuilder("resource1")
                .WithUnit("°C")
                .WithGroups("group1")
                .AddRepresentation(representation1)
                .Build();

            var representation2 = new Representation(NexusDataType.FLOAT64, TimeSpan.FromSeconds(1));

            var resource2 = new ResourceBuilder("resource2")
                .WithUnit("bar")
                .WithGroups("group2")
                .AddRepresentation(representation2)
                .Build();

            catalog = new ResourceCatalogBuilder("/A/B/C")
                .WithProperty("a", "b")
                .WithProperty("c", 1)
                .AddResources(resource1, resource2)
                .Build();
        }
        else if (catalog.Id == "/D/E/F")
        {
            var representation = new Representation(NexusDataType.FLOAT64, TimeSpan.FromSeconds(1));

            var resource = new ResourceBuilder("resource1")
                .WithUnit("m/s")
                .WithGroups("group1")
                .AddRepresentation(representation)
                .Build();

            var resource2 = new ResourceBuilder("resource2")
                .WithUnit("m/s")
                .WithGroups("group1")
                .AddRepresentation(representation)
                .Build();

            catalog = new ResourceCatalogBuilder("/D/E/F")
                .AddResources(resource, resource2)
                .Build();
        }

        else
            throw new Exception("Unknown catalog identifier.");
        
        return Task.FromResult(catalog);
    }

    public Task<CatalogTimeRange> GetTimeRangeAsync(string catalogId, CancellationToken cancellationToken)
    {
        if (catalogId != "/A/B/C")
            throw new Exception("Unknown catalog identifier.");

        var filePaths = Directory.GetFiles(_context.ResourceLocator!.ToPath(), "*.dat", SearchOption.AllDirectories);
        var fileNames = filePaths.Select(filePath => Path.GetFileName(filePath));

        var dateTimes = fileNames
            .Select(fileName => DateTime.SpecifyKind(
                DateTime.ParseExact(
                    fileName,
                    "yyyy-MM-dd_HH-mm-ss'.dat'",
                    CultureInfo.InvariantCulture),
                DateTimeKind.Utc))
            .OrderBy(dateTime => dateTime)
            .ToList();

        return Task.FromResult(new CatalogTimeRange(dateTimes[0], dateTimes[^1]));
    }

    public Task<double> GetAvailabilityAsync(string catalogId, DateTime begin, DateTime end, CancellationToken cancellationToken)
    {
        if (catalogId != "/A/B/C")
            throw new Exception("Unknown catalog identifier.");

        var periodPerFile = TimeSpan.FromMinutes(10);
        var maxFileCount = (end - begin).Ticks / periodPerFile.Ticks;
        var filePaths = Directory.GetFiles(_context.ResourceLocator!.ToPath(), "*.dat", SearchOption.AllDirectories);
        var fileNames = filePaths.Select(filePath => Path.GetFileName(filePath));

        var actualFileCount = fileNames
            .Select(fileName => DateTime.SpecifyKind(
                DateTime.ParseExact(
                    fileName,
                    "yyyy-MM-dd_HH-mm-ss'.dat'",
                    CultureInfo.InvariantCulture),
                DateTimeKind.Utc))
            .Where(current => current >= begin && current < end)
            .Count();

        return Task.FromResult(actualFileCount / (double)maxFileCount);
    }

    public Task ReadAsync(
        DateTime begin,
        DateTime end,
        ReadRequest[] requests,
        ReadDataHandler readData,
        IProgress<double> progress,
        CancellationToken cancellationToken)
    {
        if (requests[0].CatalogItem.Catalog.Id == "/A/B/C")
            return ReadLocalFiles(
                begin, end, requests, progress, cancellationToken);

        else
            return ReadAndModifyNexusData(
                begin, end, requests, readData, progress, cancellationToken);
    }

    private async Task ReadLocalFiles(
        DateTime begin,
        DateTime end,
        ReadRequest[] requests,
        IProgress<double> progress,
        CancellationToken cancellationToken
    )
    {
        // ############################################################################
        // Warning! This is a simplified implementation and not generally applicable! #
        // ############################################################################

        // The test files are located in a folder hierarchy where each has a length of 
        // 10-minutes (600 s):
        // ...
        // TESTDATA/2020-01/2020-01-02/2020-01-02_00-00-00.dat
        // TESTDATA/2020-01/2020-01-02/2020-01-02_00-10-00.dat
        // ...
        // The data itself is made up of progressing timestamps (unix time represented 
        // stored as 8 byte little-endian integers) with a sample rate of 1 Hz.

        foreach (var request in requests)
        {
            var samplesPerSecond = (int)(1 / request.CatalogItem.Representation.SamplePeriod.TotalSeconds);
            var secondsPerFile = 600;
            var typeSize = 8;

            // go
            var currentBegin = begin;

            while (currentBegin < end)
            {
                // find files
                var searchPath = Path.Combine(_context.ResourceLocator!.ToPath(), currentBegin.ToString("yyyy-MM"), currentBegin.ToString("yyyy-MM-dd"));
                var filePaths = Directory.GetFiles(searchPath, "*.dat", SearchOption.AllDirectories);

                foreach (var filePath in filePaths)
                {
                    var fileName = Path.GetFileName(filePath);
                    var fileBegin = DateTime.SpecifyKind(DateTime.ParseExact(fileName, "yyyy-MM-dd_HH-mm-ss'.dat'", CultureInfo.InvariantCulture), DateTimeKind.Utc);

                    // if file date/time is within the limits
                    if (fileBegin >= currentBegin && fileBegin < end)
                    {
                        // compute target offset and file length
                        var targetOffset = (int)((fileBegin - begin).TotalSeconds) * samplesPerSecond;
                        var fileLength = samplesPerSecond * secondsPerFile;

                        // load and copy binary data
                        var fileData = await File.ReadAllBytesAsync(filePath, cancellationToken);

                        fileData
                            .CopyTo(request.Data.Slice(targetOffset * typeSize));

                        // set status to 1 for all written data
                        request.Status.Slice(targetOffset, fileLength).Span.Fill(1);
                    }
                }

                currentBegin += TimeSpan.FromDays(1);
            }

            await request.CompleteAsync();
        }
    }

    private async Task ReadAndModifyNexusData(
        DateTime begin,
        DateTime end,
        ReadRequest[] requests,
        ReadDataHandler readData,
        IProgress<double> progress,
        CancellationToken cancellationToken
    )
    {
        void GenerateData(ReadRequest request, ReadOnlySpan<double> dataFromNexus)
        {
            var doubleData = MemoryMarshal.Cast<byte, double>(request.Data.Span);

            for (int i = 0; i < doubleData.Length; i++)
            {
                doubleData[i] = dataFromNexus[i] * 2;
            }

            request.Status.Span.Fill(1);
        }

        async Task HandleRequestAsync(ReadRequest request)
        {
            var length = (int)((end - begin).Ticks / request.CatalogItem.Representation.SamplePeriod.Ticks);

            using var memoryOwner = MemoryPool<double>.Shared.Rent(length);
            var buffer = memoryOwner.Memory.Slice(0, length);

            await readData("/need/more/data/1_s", begin, end, buffer, cancellationToken);
            GenerateData(request, buffer.Span);
            request.Status.Span[0] = (byte)requests.Length;

            await request.CompleteAsync();
        }

        await Task.WhenAll(requests.Select(HandleRequestAsync));
    }
}
