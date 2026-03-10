using OmeZarr.Core.OmeZarr.Helpers;
using ZarrNET;
using ZarrNET.Core;
using ZarrNET.Core.OmeZarr;
using ZarrNET.Core.Zarr.Store;

// =============================================================================
// Example 1 — Read from public HTTP URL
// =============================================================================
namespace ZarrNET
{
    public class Reader
    {
    async Task ReadFromPublicUrl()
        {
            // OME-Zarr datasets can be hosted on any HTTP server
            var url = "https://uk1s3.embassy.ebi.ac.uk/idr/zarr/v0.4/idr0062A/6001240.zarr";

            await using var reader = await OmeZarrReader.OpenAsync(url);

            var image = reader.AsMultiscaleImage();
            var multiscale = image.Multiscales[0];

            Console.WriteLine($"Remote image: {multiscale.Name}");
            Console.WriteLine($"Axes: {string.Join(", ", multiscale.Axes.Select(a => a.Name))}");
            Console.WriteLine($"Resolution levels: {multiscale.Datasets.Length}");

            // Open a low-resolution level for fast preview
            var level = await image.OpenResolutionLevelAsync(datasetIndex: 2);
            Console.WriteLine($"Level 2 shape: [{string.Join(", ", level.Shape)}]");

            // Read a small region
            var plane = await level.ReadPlaneAsync(t: 0, c: 0, z: 0);
            Console.WriteLine($"Downloaded plane: {plane}");
        }

    // =============================================================================
    // Example 2 — Read from S3 public bucket
    // =============================================================================

    async Task ReadFromS3()
    {
        // Public S3 buckets can be accessed directly via HTTPS
        var s3Url = "https://s3.amazonaws.com/my-bucket/datasets/image.zarr";

        await using var reader = await OmeZarrReader.OpenAsync(s3Url);
        var image = reader.AsMultiscaleImage();
        var level = await image.OpenResolutionLevelAsync(0);

        var plane = await level.ReadPlaneAsync(t: 0, c: 0, z: 0);
        Console.WriteLine($"S3 plane: {plane}");
    }

    // =============================================================================
    // Example 3 — Read from Azure Blob Storage
    // =============================================================================

    async Task ReadFromAzure()
    {
        // Azure Blob Storage public containers or SAS URLs
        var azureUrl = "https://myaccount.blob.core.windows.net/container/image.zarr";

        await using var reader = await OmeZarrReader.OpenAsync(azureUrl);
        var image = reader.AsMultiscaleImage();

        // Rest is the same as local files...
    }

    // =============================================================================
    // Example 4 — Read from Google Cloud Storage
    // =============================================================================

    async Task ReadFromGcs()
    {
        // Google Cloud Storage public buckets
        var gcsUrl = "https://storage.googleapis.com/my-bucket/image.zarr";

        await using var reader = await OmeZarrReader.OpenAsync(gcsUrl);
        // ...
    }

    // =============================================================================
    // Example 5 — Custom HttpClient configuration (authentication, timeouts)
    // =============================================================================

    async Task ReadWithCustomHttpClient()
    {
        // Create custom HttpClient with authentication
        var httpClient = new HttpClient();
        httpClient.DefaultRequestHeaders.Add("Authorization", "Bearer YOUR_TOKEN");
        httpClient.Timeout = TimeSpan.FromMinutes(10);

        var store = new HttpZarrStore("https://example.com/data.zarr", httpClient, ownsHttpClient: true);

        await using var reader = await OmeZarrReader.OpenAsync(store);
        var image = reader.AsMultiscaleImage();

        // Read data...
    }

    // =============================================================================
    // Example 6 — IDR (Image Data Resource) public datasets
    // =============================================================================

    async Task ReadFromIdr()
    {
        // IDR hosts many public OME-Zarr datasets
        // Browse at: https://idr.openmicroscopy.org/

        var idrUrl = "https://uk1s3.embassy.ebi.ac.uk/idr/zarr/v0.4/idr0062A/6001240.zarr";

        await using var reader = await OmeZarrReader.OpenAsync(idrUrl);
        var image = reader.AsMultiscaleImage();

        Console.WriteLine("Reading from IDR...");

        // Get metadata
        var axes = image.Multiscales[0].Axes;
        var omero = image.Multiscales[0].Omero;

        if (omero?.Channels != null)
        {
            Console.WriteLine("Channels:");
            foreach (var ch in omero.Channels)
            {
                Console.WriteLine($"  {ch.Label}: color={ch.Color}, active={ch.Active}");
            }
        }

        // Read a thumbnail from lowest resolution
        var numLevels = image.Multiscales[0].Datasets.Length;
        var thumbnail = await image.OpenResolutionLevelAsync(numLevels - 1);
        var plane = await thumbnail.ReadPlaneAsync(t: 0, c: 0, z: 0);

        Console.WriteLine($"Thumbnail: {plane.Width}x{plane.Height}");
    }

    // =============================================================================
    // Example 7 — Progressive loading (low res → high res)
    // =============================================================================

    async Task ProgressiveLoading(string url)
    {
        await using var reader = await OmeZarrReader.OpenAsync(url);
        var image = reader.AsMultiscaleImage();

        var levels = await image.OpenAllResolutionLevelsAsync();

        // Load from lowest to highest resolution
        for (int i = levels.Count - 1; i >= 0; i--)
        {
            var level = levels[i];
            var plane = await level.ReadPlaneAsync(t: 0, c: 0, z: 0);

            Console.WriteLine($"Level {i}: {plane.Width}x{plane.Height}");

            // Display progressively more detailed image
            // UpdateDisplay(plane);

            if (i > 0)
            {
                // Show low-res while loading high-res
                Console.WriteLine($"  Loaded {plane.Data.Length:N0} bytes, loading next level...");
            }
        }

        Console.WriteLine("Full resolution loaded!");
    }

    // =============================================================================
    // Example 8 — Download and cache locally
    // =============================================================================

    async Task DownloadAndCache(string url, string localPath)
    {
        Console.WriteLine($"Downloading from {url}...");

        await using var httpReader = await OmeZarrReader.OpenAsync(url);
        var image = httpReader.AsMultiscaleImage();
        var level = await image.OpenResolutionLevelAsync(0);

        // Read all data
        var fullExtent = new ZarrNET.Core.OmeZarr.Coordinates.PixelRegion(
            start: new long[level.Rank],
            end: level.Shape
        );

        var data = await level.ReadPixelRegionAsync(fullExtent);

        Console.WriteLine($"Downloaded {data.Data.Length:N0} bytes");

        // Save to local file for caching
        // (This is a simple example - for production, copy the entire Zarr structure)
        System.IO.File.WriteAllBytes(localPath, data.Data);

        Console.WriteLine($"Cached to {localPath}");
    }

    // =============================================================================
    // Example 9 — Compare local vs remote performance
    // =============================================================================

    async Task CompareLocalVsRemote(string localPath, string remoteUrl)
    {
        var sw = System.Diagnostics.Stopwatch.StartNew();

        // Local read
        await using (var localReader = await OmeZarrReader.OpenAsync(localPath))
        {
            var level = await localReader.AsMultiscaleImage().OpenResolutionLevelAsync(0);
            var plane = await level.ReadPlaneAsync(t: 0, c: 0, z: 0);
            sw.Stop();
            Console.WriteLine($"Local read: {sw.ElapsedMilliseconds}ms ({plane.Data.Length:N0} bytes)");
        }

        sw.Restart();

        // Remote read
        await using (var remoteReader = await OmeZarrReader.OpenAsync(remoteUrl))
        {
            var level = await remoteReader.AsMultiscaleImage().OpenResolutionLevelAsync(0);
            var plane = await level.ReadPlaneAsync(t: 0, c: 0, z: 0);
            sw.Stop();
            Console.WriteLine($"Remote read: {sw.ElapsedMilliseconds}ms ({plane.Data.Length:N0} bytes)");
        }
    }

    // =============================================================================
    // Example 10 — Handle network errors gracefully
    // =============================================================================

    async Task ReadWithErrorHandling(string url)
    {
        try
        {
            await using var reader = await OmeZarrReader.OpenAsync(url);
            var image = reader.AsMultiscaleImage();
            var level = await image.OpenResolutionLevelAsync(0);

            var plane = await level.ReadPlaneAsync(t: 0, c: 0, z: 0);
            Console.WriteLine($"Success: {plane}");
        }
        catch (HttpRequestException ex)
        {
            Console.WriteLine($"Network error: {ex.Message}");
            Console.WriteLine("Check your internet connection and URL.");
        }
        catch (TimeoutException ex)
        {
            Console.WriteLine($"Request timed out: {ex.Message}");
            Console.WriteLine("The dataset may be very large or the server is slow.");
        }
        catch (FileNotFoundException ex)
        {
            Console.WriteLine($"Dataset not found: {ex.Message}");
            Console.WriteLine("The Zarr metadata files are missing or the URL is incorrect.");
        }
    }

    // =============================================================================
    // Example 11 — Mixed local and remote (plate with remote fields)
    // =============================================================================

    async Task MixedLocalRemote()
    {
        // This demonstrates the architecture's flexibility —
        // you could theoretically have a plate where some wells are local
        // and others are remote, though in practice this is uncommon

        var localPlate = await OmeZarrReader.OpenAsync("C:/data/plate.zarr");
        var remotePlate = await OmeZarrReader.OpenAsync("https://example.com/plate.zarr");

        // Both work identically
        var localWell = await localPlate.AsPlate().OpenWellAsync("A", "1");
        var remoteWell = await remotePlate.AsPlate().OpenWellAsync("A", "1");
    }

        // =============================================================================
        // Example 12 — Real-world example: Load IDR dataset and find brightest region
        // =============================================================================

        async Task FindBrightestRegion()
        {
            var url = "https://uk1s3.embassy.ebi.ac.uk/idr/zarr/v0.4/idr0062A/6001240.zarr";

            await using var reader = await OmeZarrReader.OpenAsync(url);
            var image = reader.AsMultiscaleImage();

            // Use low-resolution level for fast scanning
            var level = await image.OpenResolutionLevelAsync(datasetIndex: 3);

            var plane = await level.ReadPlaneAsync(t: 0, c: 0, z: 0);

            Console.WriteLine($"Scanning {plane.Width}x{plane.Height} pixels...");

            var pixels = plane.As1DArray<ushort>();

            // Find max intensity
            ushort maxValue = 0;
            int maxIndex = 0;

            for (int i = 0; i < pixels.Length; i++)
            {
                if (pixels[i] > maxValue)
                {
                    maxValue = pixels[i];
                    maxIndex = i;
                }
            }

            int y = maxIndex / plane.Width;
            int x = maxIndex % plane.Width;

            Console.WriteLine($"Brightest pixel at ({x}, {y}) with value {maxValue}");

            var fullRes = await image.OpenResolutionLevelAsync(0);
            var fullResWidth = fullRes.Shape[4];  // x is at index 4 in [t,c,z,y,x]
            var scaleFactor = fullResWidth / plane.Width;  // ✓ Works!
            var fullResX = x * scaleFactor;
            var fullResY = y * scaleFactor;

            var cropSize = 512;
            var cropRegion = new ZarrNET.Core.OmeZarr.Coordinates.PixelRegion(
                start: [0, 0, 0, Math.Max(0, fullResY - cropSize / 2), Math.Max(0, fullResX - cropSize / 2)],
                end: [1, 1, 1, Math.Min(fullRes.Shape[3], fullResY + cropSize/2),
                         Math.Min(fullRes.Shape[4], fullResX + cropSize/2)]
            );

            var crop = await fullRes.ReadPixelRegionAsync(cropRegion);
            Console.WriteLine($"Loaded high-res crop: {crop}");
        }
    }
}