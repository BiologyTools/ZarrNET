using OmeZarr.Core.OmeZarr.Nodes;
using ZarrNET.Core.OmeZarr.Coordinates;
using ZarrNET.Core.OmeZarr.Nodes;

namespace OmeZarr.Core.OmeZarr.Helpers;

/// <summary>
/// Extension methods for common OME-Zarr plane extraction operations.
/// </summary>
public static class PlaneReader
{
    // =========================================================================
    // 2D Plane extraction
    // =========================================================================

    /// <summary>
    /// Reads a single 2D plane from a multidimensional image.
    /// Automatically detects spatial axes (y, x) and allows selecting specific
    /// timepoints, channels, and z-slices.
    /// </summary>
    /// <param name="level">The resolution level to read from</param>
    /// <param name="t">Timepoint index (0-based). Use null to read all timepoints.</param>
    /// <param name="c">Channel index (0-based). Use null to read all channels.</param>
    /// <param name="z">Z-slice index (0-based). Use null to read all z-slices.</param>
    /// <returns>A PlaneResult containing the 2D (or 3D/4D if nulls used) image data</returns>
    public static async Task<PlaneResult> ReadPlaneAsync(
        this ResolutionLevelNode level,
        int? t = 0,
        int? c = 0,
        int? z = 0,
        CancellationToken ct = default)
    {
        var axes = level.EffectiveAxes;
        var shape = level.Shape;

        // Build pixel region for the requested plane
        var start = new long[axes.Length];
        var end = new long[axes.Length];

        for (int i = 0; i < axes.Length; i++)
        {
            var axisName = axes[i].Name.ToLowerInvariant();

            start[i] = axisName switch
            {
                "t" => t ?? 0,
                "c" => c ?? 0,
                "z" => z ?? 0,
                _ => 0  // y, x - read full extent
            };

            end[i] = axisName switch
            {
                "t" => t.HasValue ? t.Value + 1 : shape[i],
                "c" => c.HasValue ? c.Value + 1 : shape[i],
                "z" => z.HasValue ? z.Value + 1 : shape[i],
                _ => shape[i]  // y, x - read full extent
            };
        }

        var region = new PixelRegion(start, end);
        var result = await level.ReadPixelRegionAsync(region, ct: ct).ConfigureAwait(false);

        return new PlaneResult(result, axes);
    }
    /// <summary>
    /// Reads a tile from a multidimensional image using pixel coordinates.
    /// </summary>
    /// <param name="level">Resolution level</param>
    /// <param name="tileOriginX">Tile origin X in pixels</param>
    /// <param name="tileOriginY">Tile origin Y in pixels</param>
    /// <param name="tileSizeX">Tile width in pixels</param>
    /// <param name="tileSizeY">Tile height in pixels</param>
    /// <param name="t">Time index (null = all)</param>
    /// <param name="c">Channel index (null = all)</param>
    /// <param name="z">Z index (null = all)</param>
    public static async Task<PlaneResult> ReadTileAsync(
        this ResolutionLevelNode level,
        int tileOriginX,
        int tileOriginY,
        int tileSizeX,
        int tileSizeY,
        int? z = 0,
        int? c = 0,
        int? t = 0,
        CancellationToken ct = default)
    {
        if (tileSizeX <= 0)
            throw new ArgumentOutOfRangeException(
                nameof(tileSizeX), tileSizeX, "Tile width must be > 0.");
        if (tileSizeY <= 0)
            throw new ArgumentOutOfRangeException(
                nameof(tileSizeY), tileSizeY, "Tile height must be > 0.");

        var axes = level.EffectiveAxes;
        var shape = level.Shape;

        var start = new long[axes.Length];
        var end = new long[axes.Length];

        for (int i = 0; i < axes.Length; i++)
        {
            var axisName = axes[i].Name.ToLowerInvariant();

            switch (axisName)
            {
                case "t":
                    start[i] = t ?? 0;
                    end[i] = t.HasValue ? t.Value + 1 : shape[i];
                    break;

                case "c":
                    start[i] = c ?? 0;
                    end[i] = c.HasValue ? c.Value + 1 : shape[i];
                    break;

                case "z":
                    start[i] = z ?? 0;
                    end[i] = z.HasValue ? z.Value + 1 : shape[i];
                    break;

                case "y":
                    start[i] = Math.Max(0, tileOriginY);
                    end[i] = Math.Min(shape[i], tileOriginY + tileSizeY);
                    break;

                case "x":
                    start[i] = Math.Max(0, tileOriginX);
                    end[i] = Math.Min(shape[i], tileOriginX + tileSizeX);
                    break;

                default:
                    // unknown axes → read full range
                    start[i] = 0;
                    end[i] = shape[i];
                    break;
            }
        }

        // Validate that every axis has a positive extent. This catches two
        // problems at once: (a) the stored shape has a zero dimension (writer
        // bug) and (b) the tile origin is beyond the array extent.
        for (int i = 0; i < axes.Length; i++)
        {
            if (end[i] <= start[i])
                throw new ArgumentException(
                    $"Tile region is empty on the '{axes[i].Name}' axis " +
                    $"(start={start[i]}, end={end[i]}, shape={shape[i]}). " +
                    $"The tile origin may be outside the image extent, " +
                    $"or the array shape has a zero-sized dimension.");
        }

        var region = new PixelRegion(start, end);

        var result = await level
            .ReadPixelRegionAsync(region, ct: ct)
            .ConfigureAwait(false);

        return new PlaneResult(result, axes);
    }

    public static Dictionary<string, double> GetAxisPixelSizes(
    this ResolutionLevelNode level)
    {
        var dict = new Dictionary<string, double>(StringComparer.OrdinalIgnoreCase);

        var axes = level.EffectiveAxes;
        var pixelSizes = level.GetPixelSize();

        int spatialIndex = 0;

        foreach (var axis in axes)
        {
            var name = axis.Name.ToLowerInvariant();

            if (name == "x" || name == "y" || name == "z")
            {
                if (spatialIndex < pixelSizes.Length)
                {
                    dict[name] = pixelSizes[spatialIndex++];
                }
                else
                {
                    // fallback if metadata is inconsistent
                    dict[name] = 1.0;
                }
            }
            else
            {
                // non-spatial axes (t, c, etc.)
                dict[name] = 1.0;
            }
        }

        return dict;
    }

    /// <summary>
    /// Reads a 2D plane using physical coordinates (in microns, seconds, etc.).
    /// Specify the center point and size for each non-spatial axis, and the full
    /// extent or a sub-region for spatial axes.
    /// </summary>
    /// <param name="level">The resolution level to read from</param>
    /// <param name="tCenter">Time center in physical units (e.g., seconds). Use null for index 0.</param>
    /// <param name="cCenter">Channel center (typically 0 since channels are dimensionless). Use null for index 0.</param>
    /// <param name="zCenter">Z center in physical units (e.g., microns). Use null for index 0.</param>
    /// <param name="spatialOriginY">Y origin in physical units. Use null for 0.</param>
    /// <param name="spatialOriginX">X origin in physical units. Use null for 0.</param>
    /// <param name="spatialSizeY">Y size in physical units. Use null for full extent.</param>
    /// <param name="spatialSizeX">X size in physical units. Use null for full extent.</param>
    public static async Task<PlaneResult> ReadPlanePhysicalAsync(
        this ResolutionLevelNode level,
        double? tCenter = null,
        double? cCenter = null,
        double? zCenter = null,
        double? spatialOriginY = null,
        double? spatialOriginX = null,
        double? spatialSizeY = null,
        double? spatialSizeX = null,
        CancellationToken ct = default)
    {
        var axes = level.EffectiveAxes;
        var pixelSize = level.GetPixelSize();
        var extent = level.GetPhysicalExtent();

        var origin = new double[axes.Length];
        var size = new double[axes.Length];

        for (int i = 0; i < axes.Length; i++)
        {
            var axisName = axes[i].Name.ToLowerInvariant();

            (origin[i], size[i]) = axisName switch
            {
                "t" => (tCenter ?? 0, pixelSize[i]),
                "c" => (cCenter ?? 0, pixelSize[i]),
                "z" => (zCenter ?? 0, pixelSize[i]),
                "y" => (spatialOriginY ?? 0, spatialSizeY ?? extent.Size[i]),
                "x" => (spatialOriginX ?? 0, spatialSizeX ?? extent.Size[i]),
                _ => (0, extent.Size[i])
            };
        }

        var roi = new PhysicalROI(origin, size);
        var result = await level.ReadRegionAsync(roi, ct: ct).ConfigureAwait(false);

        return new PlaneResult(result, axes);
    }

    // =========================================================================
    // Convenience overloads for common axis layouts
    // =========================================================================

    /// <summary>
    /// Reads a single 2D plane from a 5D image (t, c, z, y, x).
    /// </summary>
    public static Task<PlaneResult> ReadPlane5DAsync(
        this ResolutionLevelNode level,
        int t, int c, int z,
        CancellationToken ct = default)
        => ReadPlaneAsync(level, t, c, z, ct);

    /// <summary>
    /// Reads a single 2D plane from a 4D image (c, z, y, x) - no time axis.
    /// Common in HCS field data.
    /// </summary>
    public static Task<PlaneResult> ReadPlane4DAsync(
        this ResolutionLevelNode level,
        int c, int z,
        CancellationToken ct = default)
        => ReadPlaneAsync(level, t: null, c, z, ct);

    /// <summary>
    /// Reads a single 2D plane from a 3D image (z, y, x) - no time or channel.
    /// </summary>
    public static Task<PlaneResult> ReadPlane3DAsync(
        this ResolutionLevelNode level,
        int z,
        CancellationToken ct = default)
        => ReadPlaneAsync(level, t: null, c: null, z, ct);

    /// <summary>
    /// Reads a 2D image (y, x) - no extra dimensions.
    /// </summary>
    public static Task<PlaneResult> ReadPlane2DAsync(
        this ResolutionLevelNode level,
        CancellationToken ct = default)
        => ReadPlaneAsync(level, t: null, c: null, z: null, ct);
}

// =============================================================================
// PlaneResult - typed wrapper around RegionResult for 2D planes
// =============================================================================

/// <summary>
/// Result of reading a 2D plane. Provides convenience methods for extracting
/// the actual 2D array and metadata about which axes were selected.
/// </summary>
public sealed class PlaneResult
{
    private readonly RegionResult _regionResult;
    private readonly ZarrNET.Core.OmeZarr.Metadata.AxisMetadata[] _axes;

    public byte[] Data => _regionResult.Data;
    public long[] Shape => _regionResult.Shape;
    public string DataType => _regionResult.DataType;
    public int Width { get; }
    public int Height { get; }

    internal PlaneResult(
        RegionResult regionResult,
        ZarrNET.Core.OmeZarr.Metadata.AxisMetadata[] axes)
    {
        _regionResult = regionResult;
        _axes = axes;

        // Find spatial axes dimensions (y, x)
        var yIndex = Array.FindIndex(axes, a => a.Name.Equals("y", StringComparison.OrdinalIgnoreCase));
        var xIndex = Array.FindIndex(axes, a => a.Name.Equals("x", StringComparison.OrdinalIgnoreCase));

        Height =(int)(yIndex >= 0 ? Shape[yIndex] : Shape[^2]);  // fallback to 2nd-to-last axis
        Width = (int)(xIndex >= 0 ? Shape[xIndex] : Shape[^1]);   // fallback to last axis
    }

    /// <summary>
    /// Extracts the plane data as a typed 2D array.
    /// Only works if the result is actually 2D (all non-spatial axes were selected as single indices).
    /// </summary>
    public T[,] As2DArray<T>() where T : struct
    {
        if (Shape.Length != 2)
            throw new InvalidOperationException(
                $"Cannot convert to 2D array - result has {Shape.Length} dimensions. " +
                $"Use ReadPlaneAsync with specific t/c/z indices to get a true 2D plane.");

        var height = Shape[0];
        var width = Shape[1];

        var array = new T[height, width];
        var elementSize = System.Runtime.InteropServices.Marshal.SizeOf<T>();

        if (Data.Length != height * width * elementSize)
            throw new InvalidOperationException(
                $"Data size mismatch. Expected {height * width * elementSize} bytes, got {Data.Length} bytes.");

        Buffer.BlockCopy(Data, 0, array, 0, Data.Length);
        return array;
    }

    /// <summary>
    /// Extracts the plane data as a flat 1D array of the specified type.
    /// Useful when you want to process the data without the 2D indexing overhead.
    /// </summary>
    public T[] As1DArray<T>() where T : struct
    {
        var elementSize = System.Runtime.InteropServices.Marshal.SizeOf<T>();
        var elementCount = Data.Length / elementSize;

        var array = new T[elementCount];
        Buffer.BlockCopy(Data, 0, array, 0, Data.Length);
        return array;
    }

    /// <summary>
    /// Returns the raw byte array exactly as stored in the Zarr array.
    /// This is the native byte representation - endianness matches the stored format.
    /// </summary>
    public byte[] GetRawBytes() => Data;

    /// <summary>
    /// Converts the plane data to a byte array with the specified pixel format.
    /// Useful for interop with imaging libraries that expect specific byte layouts.
    /// </summary>
    public byte[] ToBytes<T>(PixelFormat format = PixelFormat.Native) where T : struct
    {
        var pixels = As1DArray<T>();
        
        return format switch
        {
            PixelFormat.Native => Data,
            PixelFormat.Gray8 => ConvertToGray8(pixels),
            PixelFormat.Gray16 => ConvertToGray16(pixels),
            PixelFormat.Bgr24 => ConvertToBgr24(pixels),
            PixelFormat.Bgra32 => ConvertToBgra32(pixels),
            _ => throw new NotSupportedException($"Pixel format {format} not supported")
        };
    }

    // -------------------------------------------------------------------------
    // Pixel format conversions
    // -------------------------------------------------------------------------

    private byte[] ConvertToGray8<T>(T[] pixels) where T : struct
    {
        var output = new byte[pixels.Length];
        
        for (int i = 0; i < pixels.Length; i++)
        {
            output[i] = pixels[i] switch
            {
                byte b => b,
                ushort us => (byte)(us >> 8),  // Take upper 8 bits
                uint ui => (byte)(ui >> 24),
                float f => (byte)Math.Clamp(f * 255, 0, 255),
                double d => (byte)Math.Clamp(d * 255, 0, 255),
                _ => 0
            };
        }
        
        return output;
    }

    private byte[] ConvertToGray16<T>(T[] pixels) where T : struct
    {
        var output = new byte[pixels.Length * 2];
        
        for (int i = 0; i < pixels.Length; i++)
        {
            ushort value = pixels[i] switch
            {
                byte b => (ushort)(b << 8),
                ushort us => us,
                uint ui => (ushort)(ui >> 16),
                float f => (ushort)Math.Clamp(f * 65535, 0, 65535),
                double d => (ushort)Math.Clamp(d * 65535, 0, 65535),
                _ => 0
            };
            
            // Little-endian
            output[i * 2] = (byte)(value & 0xFF);
            output[i * 2 + 1] = (byte)(value >> 8);
        }
        
        return output;
    }

    private byte[] ConvertToBgr24<T>(T[] pixels) where T : struct
    {
        var output = new byte[pixels.Length * 3];
        
        for (int i = 0; i < pixels.Length; i++)
        {
            byte gray = pixels[i] switch
            {
                byte b => b,
                ushort us => (byte)(us >> 8),
                uint ui => (byte)(ui >> 24),
                float f => (byte)Math.Clamp(f * 255, 0, 255),
                double d => (byte)Math.Clamp(d * 255, 0, 255),
                _ => 0
            };
            
            // BGR order
            output[i * 3] = gray;     // B
            output[i * 3 + 1] = gray; // G
            output[i * 3 + 2] = gray; // R
        }
        
        return output;
    }

    private byte[] ConvertToBgra32<T>(T[] pixels) where T : struct
    {
        var output = new byte[pixels.Length * 4];
        
        for (int i = 0; i < pixels.Length; i++)
        {
            byte gray = pixels[i] switch
            {
                byte b => b,
                ushort us => (byte)(us >> 8),
                uint ui => (byte)(ui >> 24),
                float f => (byte)Math.Clamp(f * 255, 0, 255),
                double d => (byte)Math.Clamp(d * 255, 0, 255),
                _ => 0
            };
            
            // BGRA order
            output[i * 4] = gray;     // B
            output[i * 4 + 1] = gray; // G
            output[i * 4 + 2] = gray; // R
            output[i * 4 + 3] = 255;  // A (fully opaque)
        }
        
        return output;
    }

    public override string ToString()
        => $"Plane [{Width} × {Height}] {DataType} - {Data.Length:N0} bytes";
}

// =============================================================================
// PixelFormat enum
// =============================================================================

/// <summary>
/// Pixel format for byte array conversions.
/// </summary>
public enum PixelFormat
{
    /// <summary>Native format as stored in Zarr (no conversion).</summary>
    Native,
    
    /// <summary>8-bit grayscale (1 byte per pixel).</summary>
    Gray8,
    
    /// <summary>16-bit grayscale, little-endian (2 bytes per pixel).</summary>
    Gray16,
    
    /// <summary>24-bit BGR (3 bytes per pixel, Blue-Green-Red order).</summary>
    Bgr24,
    
    /// <summary>32-bit BGRA (4 bytes per pixel, Blue-Green-Red-Alpha order).</summary>
    Bgra32
}
