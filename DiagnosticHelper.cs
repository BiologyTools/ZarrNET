using ZarrNET;
using ZarrNET.Core.OmeZarr.Coordinates;
using ZarrNET.Core;
namespace ZarrNET
{
    public static class DiagnosticHelper
    {
        // Diagnostic: Read a specific pixel region to verify raw data reading
        public static async Task DiagnoseBlankData(string zarrPath)
        {
            await using var reader = await OmeZarrReader.OpenAsync(zarrPath);
            var image = reader.AsMultiscaleImage();
            var level = await image.OpenResolutionLevelAsync(datasetIndex: 0);

            Console.WriteLine($"Array shape: [{string.Join(", ", level.Shape)}]");
            Console.WriteLine($"DataType: {level.DataType}");
            Console.WriteLine($"Axes: {string.Join(", ", level.Multiscale.Axes.Select(a => a.Name))}");

            var pixelSize = level.GetPixelSize();
            Console.WriteLine($"Pixel size: [{string.Join(", ", pixelSize.Select(p => p.ToString("G4")))}]");
            // Try reading a small region in PIXEL coordinates first to eliminate
            // coordinate transform issues
            var rank = level.Rank;
            var pixelRegion = new PixelRegion(
                start: new long[rank],  // all zeros - start of array
                end: Enumerable.Range(0, rank).Select(d => Math.Min(10, level.Shape[d])).ToArray()  // 10 pixels per axis or array size
            );

            Console.WriteLine($"\nReading pixel region: {pixelRegion}");
            var result = await level.ReadPixelRegionAsync(pixelRegion);

            Console.WriteLine($"Result: {result}");
            Console.WriteLine($"Data length: {result.Data.Length} bytes");

            // Check if data is all zeros
            var hasNonZero = result.Data.Any(b => b != 0);
            Console.WriteLine($"Contains non-zero values: {hasNonZero}");

            if (!hasNonZero)
            {
                Console.WriteLine("WARNING: All data is zero. This could mean:");
                Console.WriteLine("  1. The chunks are legitimately empty");
                Console.WriteLine("  2. There's a decoding issue");
                Console.WriteLine("  3. The array dimensions need verification");
            }
            else
            {
                // Show some sample values
                if (result.DataType == "uint16")
                {
                    var pixels = new ushort[Math.Min(20, result.ElementCount)];
                    Buffer.BlockCopy(result.Data, 0, pixels, 0, pixels.Length * 2);
                    Console.WriteLine($"First {pixels.Length} pixel values: {string.Join(", ", pixels)}");
                }
                else if (result.DataType == "uint8")
                {
                    var pixels = result.Data.Take(20).ToArray();
                    Console.WriteLine($"First {pixels.Length} pixel values: {string.Join(", ", pixels)}");
                }
            }
        }
    }
}