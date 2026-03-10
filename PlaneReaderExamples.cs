using OmeZarr.Core.OmeZarr.Helpers;
using ZarrNET;
using ZarrNET.Core;
using ZarrNET.Core.OmeZarr;

namespace ZarrNET
{
    public static class PlaneHelpers
    {
        // =============================================================================
        // Example 1 — Read a single 2D plane from 5D data (t, c, z, y, x)
        // =============================================================================

        public static async Task ReadSinglePlane(string zarrPath)
        {
            await using var reader = await OmeZarrReader.OpenAsync(zarrPath);
            var image = reader.AsMultiscaleImage();
            var level = await image.OpenResolutionLevelAsync(datasetIndex: 0);

            // Read timepoint 2, channel 1, z-slice 5
            var plane = await level.ReadPlaneAsync(t: 2, c: 1, z: 5);

            Console.WriteLine($"Plane: {plane}");  // e.g. "Plane [456 × 462] uint16 - 421,344 bytes"
            Console.WriteLine($"Width: {plane.Width}, Height: {plane.Height}");

            // Extract as typed 2D array
            var pixels = plane.As2DArray<ushort>();  // for uint16 data

            // Access pixel at row 100, column 50
            var pixelValue = pixels[100, 50];
            Console.WriteLine($"Pixel[100,50] = {pixelValue}");

            // Or get as flat 1D array for faster processing
            var flatPixels = plane.As1DArray<ushort>();
            var maxValue = flatPixels.Max();
            var minValue = flatPixels.Min();
            Console.WriteLine($"Value range: {minValue} - {maxValue}");
        }

        // =============================================================================
        // Example 2 — Iterate through all z-slices in a channel
        // =============================================================================

        public static async Task ProcessZStack(string zarrPath)
    {
        await using var reader = await OmeZarrReader.OpenAsync(zarrPath);
        var image = reader.AsMultiscaleImage();
        var level = await image.OpenResolutionLevelAsync(datasetIndex: 0);

        var axes = level.Multiscale.Axes;
        var zAxisIndex = Array.FindIndex(axes, a => a.Name.Equals("z", StringComparison.OrdinalIgnoreCase));
        var numZSlices = (int)level.Shape[zAxisIndex];

        Console.WriteLine($"Processing {numZSlices} z-slices...");

        for (int z = 0; z < numZSlices; z++)
        {
            var plane = await level.ReadPlaneAsync(t: 0, c: 0, z: z);
            var pixels = plane.As1DArray<ushort>();

            // Process this z-slice
            var meanIntensity = pixels.Average(p => (double)p);
            Console.WriteLine($"  Z={z}: mean intensity = {meanIntensity:F1}");
        }
    }

        // =============================================================================
        // Example 3 — Read all channels for a specific t,z as separate planes
        // =============================================================================

        public static async Task ReadAllChannels(string zarrPath)
    {
        await using var reader = await OmeZarrReader.OpenAsync(zarrPath);
        var image = reader.AsMultiscaleImage();
        var level = await image.OpenResolutionLevelAsync(datasetIndex: 0);

        var axes = level.Multiscale.Axes;
        var cAxisIndex = Array.FindIndex(axes, a => a.Name.Equals("c", StringComparison.OrdinalIgnoreCase));
        var numChannels = (int)level.Shape[cAxisIndex];

        var channelData = new List<ushort[,]>();

        for (int c = 0; c < numChannels; c++)
        {
            var plane = await level.ReadPlaneAsync(t: 0, c: c, z: 0);
            var pixels = plane.As2DArray<ushort>();
            channelData.Add(pixels);

            Console.WriteLine($"Channel {c}: {plane}");
        }

        // Now you have separate 2D arrays for each channel
        // e.g., create RGB composite, etc.
    }

        // =============================================================================
        // Example 4 — HCS plate/well field reading (4D: c, z, y, x)
        // =============================================================================

        public static async Task ReadHcsField(string zarrPath)
    {
        await using var reader = await OmeZarrReader.OpenAsync(zarrPath);
        var plate = reader.AsPlate();
        var well = await plate.OpenWellAsync("B", "3");
        var field = await well.OpenFieldAsync(fieldIndex: 0);
        var level = await field.OpenResolutionLevelAsync(datasetIndex: 0);

        // HCS fields often don't have a time axis - use the 4D helper
        var plane = await level.ReadPlane4DAsync(c: 0, z: 0);

        var pixels = plane.As2DArray<ushort>();
        Console.WriteLine($"Field image: {plane.Width}x{plane.Height}");
    }

        // =============================================================================
        // Example 5 — Read with physical coordinates (ROI in microns)
        // =============================================================================

        public static async Task ReadPhysicalROI(string zarrPath)
    {
        await using var reader = await OmeZarrReader.OpenAsync(zarrPath);
        var image = reader.AsMultiscaleImage();
        var level = await image.OpenResolutionLevelAsync(datasetIndex: 0);

        // Read a 100µm × 100µm region centered at 50µm, 50µm
        // at timepoint 0s, channel 0, z-slice at 5µm
        var plane = await level.ReadPlanePhysicalAsync(
            tCenter: 0,          // 0 seconds
            cCenter: 0,          // channel 0 (dimensionless)
            zCenter: 5.0,        // 5 microns
            spatialOriginY: 0,   // start at 0µm
            spatialOriginX: 0,
            spatialSizeY: 100,   // 100µm height
            spatialSizeX: 100    // 100µm width
        );

        Console.WriteLine($"Physical ROI plane: {plane}");
    }

        // =============================================================================
        // Example 6 — Simple 3D volume (z, y, x) - no time or channels
        // =============================================================================

        public static async Task Read3DVolume(string zarrPath)
    {
        await using var reader = await OmeZarrReader.OpenAsync(zarrPath);
        var image = reader.AsMultiscaleImage();
        var level = await image.OpenResolutionLevelAsync(datasetIndex: 0);

        // Read z-slice 10
        var plane = await level.ReadPlane3DAsync(z: 10);
        var pixels = plane.As2DArray<ushort>();

        Console.WriteLine($"Z-slice 10: {plane}");
    }

        // =============================================================================
        // Example 7 — Read entire plane with null parameters (all indices)
        // =============================================================================

        public static async Task ReadEntirePlaneAllDimensions(string zarrPath)
    {
        await using var reader = await OmeZarrReader.OpenAsync(zarrPath);
        var image = reader.AsMultiscaleImage();
        var level = await image.OpenResolutionLevelAsync(datasetIndex: 0);

        // Read ALL timepoints, channel 0, ALL z-slices
        // Result will be multi-dimensional (not 2D)
        var result = await level.ReadPlaneAsync(t: null, c: 0, z: null);

        Console.WriteLine($"Multi-dimensional result: {result}");
        Console.WriteLine($"Shape: [{string.Join(", ", result.Shape)}]");

        // Can't call As2DArray() here since it's not 2D
        // Use As1DArray() or work with result.Data directly
        var flatPixels = result.As1DArray<ushort>();
    }

        // =============================================================================
        // Example 8 — Data type handling for different pixel types
        // =============================================================================

        public static async Task HandleDifferentDataTypes(string zarrPath)
        {
            await using var reader = await OmeZarrReader.OpenAsync(zarrPath);
            var image = reader.AsMultiscaleImage();
            var level = await image.OpenResolutionLevelAsync(datasetIndex: 0);

            var plane = await level.ReadPlaneAsync(t: 0, c: 0, z: 0);

            // Choose the right type based on DataType
            switch (plane.DataType)
            {
                case "uint8":
                    var pixels8 = plane.As2DArray<byte>();
                    break;

                case "uint16":
                    var pixels16 = plane.As2DArray<ushort>();
                    break;

                case "uint32":
                    var pixels32 = plane.As2DArray<uint>();
                    break;

                case "float32":
                    var pixelsF32 = plane.As2DArray<float>();
                    break;

                case "float64":
                    var pixelsF64 = plane.As2DArray<double>();
                    break;

                default:
                    throw new NotSupportedException($"Unsupported data type: {plane.DataType}");
            }
        }
    }
}