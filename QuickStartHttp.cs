using OmeZarr.Core.OmeZarr.Helpers;
using ZarrNET;
using ZarrNET.Core;
using ZarrNET.Core.OmeZarr;
namespace ZarrNET
{
    public static class QuickStartHttp
    {
        public async static void Starts(string source)
        {
            // =============================================================================
            // Quick Start - Reading from HTTP/S3
            // =============================================================================

            // Works with any URL - local paths, HTTP, S3, Azure, GCS
            var dataSource = source;
            if (source == null) 
            {
                dataSource = "https://uk1s3.embassy.ebi.ac.uk/idr/zarr/v0.4/idr0062A/6001240.zarr";
            }
            // Or your local file:
            // var dataSource = @"C:\data\image.zarr";

            Console.WriteLine($"Opening: {dataSource}\n");

            await using var reader = await OmeZarrReader.OpenAsync(dataSource);

            // Auto-detect what kind of dataset this is
            Console.WriteLine($"Dataset type: {reader.RootNodeType}");

            // Open as multiscale image
            var image = reader.AsMultiscaleImage();
            var multiscale = image.Multiscales[0];

            Console.WriteLine($"Image: {multiscale.Name ?? "(unnamed)"}");
            Console.WriteLine($"Axes: {string.Join(", ", multiscale.Axes.Select(a => $"{a.Name}[{a.Unit}]"))}");
            Console.WriteLine($"Resolution levels: {multiscale.Datasets.Length}\n");

            // Open the lowest resolution level for fast preview
            var lastLevel = multiscale.Datasets.Length - 1;
            var thumbnail = await image.OpenResolutionLevelAsync(lastLevel);

            Console.WriteLine($"Thumbnail level {lastLevel}:");
            Console.WriteLine($"  Shape: [{string.Join(", ", thumbnail.Shape)}]");
            Console.WriteLine($"  Pixel size: {string.Join(" x ", thumbnail.GetPixelSize().Select(p => $"{p:G4}"))}\n");

            // Read a plane
            var plane = await thumbnail.ReadPlaneAsync(t: 0, c: 0, z: 0);
            Console.WriteLine($"Loaded plane: {plane}");

            // Extract as typed array
            var pixels = plane.As1DArray<ushort>();
            Console.WriteLine($"Pixel stats:");
            Console.WriteLine($"  Min: {pixels.Min()}");
            Console.WriteLine($"  Max: {pixels.Max()}");
            Console.WriteLine($"  Mean: {pixels.Average(p => (double)p):F1}");

            // Now load full resolution
            Console.WriteLine("\nLoading full resolution...");
            var fullRes = await image.OpenResolutionLevelAsync(0);
            Console.WriteLine($"Full res shape: [{string.Join(", ", fullRes.Shape)}]");

            // Read a small region from full res
            var fullResPlane = await fullRes.ReadPlaneAsync(t: 0, c: 0, z: 0);
            Console.WriteLine($"Full res plane: {fullResPlane.Width}x{fullResPlane.Height}");

            Console.WriteLine("\n✓ Successfully read from remote OME-Zarr dataset!");
        }
    }
}