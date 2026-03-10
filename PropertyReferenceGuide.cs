// =============================================================================
// Quick Reference - Property Access Guide
// =============================================================================

using ZarrNET;
using ZarrNET.Core.OmeZarr;
using ZarrNET.Core.OmeZarr.Coordinates;
using ZarrNET.Core.OmeZarr.Metadata;
using OmeZarr.Core.OmeZarr.Nodes;
using ZarrNET.Core;
using OmeZarr.Core.OmeZarr.Helpers;
namespace ZarrNET
{
    public class PropertyReferanceGuide()
    {
        public static MultiscaleNode image;
        // =============================================================================
        // ResolutionLevelNode - Array-level properties
        // =============================================================================
        public static async Task Start(MultiscaleNode node)
        {
            await using var reader = await OmeZarrReader.OpenAsync("path/to/data.zarr");
            image = reader.AsMultiscaleImage();
            var level = await image.OpenResolutionLevelAsync(0);

            // ✓ CORRECT - Available on ResolutionLevelNode:
            long[] shape = level.Shape;              // [t, c, z, y, x] - full array dimensions
            string dtype = level.DataType;           // "uint16", "float32", etc.
            int rank = level.Rank;                   // Number of dimensions (e.g., 5 for t,c,z,y,x)
            double[] pixelSize = level.GetPixelSize(); // Physical size per pixel [µm, µm, µm...]

            // Example: Get width and height from Shape
            var axes = level.Multiscale.Axes;
            var yIndex = Array.FindIndex(axes, a => a.Name.Equals("y", StringComparison.OrdinalIgnoreCase));
            var xIndex = Array.FindIndex(axes, a => a.Name.Equals("x", StringComparison.OrdinalIgnoreCase));
            long arrayHeight = level.Shape[yIndex];  // Full array height
            long arrayWidth = level.Shape[xIndex];   // Full array width

            // ✗ WRONG - These don't exist on ResolutionLevelNode:
            // int width = level.Width;    // ❌ Compile error
            // int height = level.Height;  // ❌ Compile error

            // =============================================================================
            // PlaneResult - 2D plane properties (after reading)
            // =============================================================================

            var plane = await level.ReadPlaneAsync(t: 0, c: 0, z: 0);

            // ✓ CORRECT - Available on PlaneResult:
            int width = plane.Width;                 // Width of the read plane (x dimension)
            int height = plane.Height;               // Height of the read plane (y dimension)
                                                     //byte[] data = plane.Data;                // Raw bytes
                                                     //int[] shape = plane.Shape;               // Shape of the read region
                                                     //string dtype = plane.DataType;           // Data type string

            // Extract data:
            ushort[,] pixels2D = plane.As2DArray<ushort>();
            ushort[] pixels1D = plane.As1DArray<ushort>();
            byte[] bytes = plane.ToBytes<ushort>(OmeZarr.Core.OmeZarr.Helpers.PixelFormat.Gray8);

            // =============================================================================
            // RegionResult - Multi-dimensional region (may not be 2D)
            // =============================================================================

            var roi = new PhysicalROI(
                origin: [0, 0, 0, 0, 0],
                size: [5, 4, 10, 100, 100]  // Multiple timepoints, channels, z-slices
            );
            var result = await level.ReadRegionAsync(roi);

            // ✓ CORRECT - Available on RegionResult:
            var data = result.Data;               // Raw bytes
            shape = result.Shape;              // [5, 4, 10, 100, 100] in this case
            dtype = result.DataType;          // Data type
            rank = result.Rank;                  // Number of dimensions
            var elementCount = result.ElementCount; // Total elements

            // ✗ WRONG - Width/Height only exist if it's a 2D plane:
            // int width = result.Width;    // ❌ Not available on RegionResult
            // int height = result.Height;  // ❌ Not available on RegionResult
        }
        // =============================================================================
        // Common Patterns
        // =============================================================================

        // Pattern 1: Get full array dimensions
        public static async Task GetArrayDimensions()
        {
            var level = await image.OpenResolutionLevelAsync(0);

            // Method 1: From Shape with axis names
            var axes = level.Multiscale.Axes;
            for (int i = 0; i < axes.Length; i++)
            {
                Console.WriteLine($"{axes[i].Name}: {level.Shape[i]}");
            }

            // Method 2: Direct indexing (if you know the layout)
            // Typical 5D: [t, c, z, y, x]
            if (level.Rank == 5)
            {
                long t = level.Shape[0];
                long c = level.Shape[1];
                long z = level.Shape[2];
                long y = level.Shape[3];
                long x = level.Shape[4];
                Console.WriteLine($"Array: {t}t x {c}c x {z}z x {y}y x {x}x");
            }
        }

        // Pattern 2: Get plane dimensions after reading
        public static async Task GetPlaneDimensions()
        {
            var level = await image.OpenResolutionLevelAsync(0);
            var plane = await level.ReadPlaneAsync(t: 0, c: 0, z: 0);

            // ✓ CORRECT - plane has Width and Height
            Console.WriteLine($"Plane: {plane.Width} x {plane.Height}");

            // Also available:
            Console.WriteLine($"Shape: [{string.Join(", ", plane.Shape)}]");
        }

        // Pattern 3: Validate dimensions before processing
        public async static Task ValidateDimensions()
        {
            var level = await image.OpenResolutionLevelAsync(0);

            // Check if it's a 5D dataset
            if (level.Rank != 5)
            {
                throw new InvalidOperationException(
                    $"Expected 5D data (t,c,z,y,x), got {level.Rank}D");
            }

            // Check spatial dimensions
            var yIndex = Array.FindIndex(level.Multiscale.Axes,
                a => a.Name.Equals("y", StringComparison.OrdinalIgnoreCase));
            var xIndex = Array.FindIndex(level.Multiscale.Axes,
                a => a.Name.Equals("x", StringComparison.OrdinalIgnoreCase));

            long height = level.Shape[yIndex];
            long width = level.Shape[xIndex];

            Console.WriteLine($"Spatial dimensions: {width} x {height}");
        }

        // Pattern 4: Read and process plane data
        public async static Task ProcessPlane()
        {
            var level = await image.OpenResolutionLevelAsync(0);
            var plane = await level.ReadPlaneAsync(t: 0, c: 0, z: 0);

            // Get dimensions from the plane result
            int w = plane.Width;
            int h = plane.Height;

            // Process the data
            var pixels = plane.As2DArray<ushort>();
            for (int y = 0; y < h; y++)
            {
                for (int x = 0; x < w; x++)
                {
                    ushort value = pixels[y, x];
                    // Process pixel...
                }
            }
        }
    }
}

/*
ResolutionLevelNode (array metadata):
  - Shape (long[])
  - DataType (string)
  - Rank (int)
  - GetPixelSize() → double[]
  - GetPhysicalExtent() → PhysicalROI
  - ReadPlaneAsync() → PlaneResult
  - ReadPixelRegionAsync() → RegionResult
  - ReadRegionAsync() → RegionResult

PlaneResult (2D plane data):
  - Data (byte[])
  - Width (int)
  - Height (int)
  - Shape (int[])
  - DataType (string)
  - As2DArray<T>() → T[,]
  - As1DArray<T>() → T[]
  - ToBytes<T>(format) → byte[]

RegionResult (N-D region data):
  - Data (byte[])
  - Shape (int[])
  - DataType (string)
  - Rank (int)
  - ElementCount (long)
  - Axes (AxisMetadata[])
*/
