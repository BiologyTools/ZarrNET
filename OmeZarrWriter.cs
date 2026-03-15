using System.Text;
using System.Text.Json;
using ZarrNET.Core;
using ZarrNET;
using ZarrNET.Core;
using ZarrNET.Core.Zarr;
using ZarrNET.Core.Zarr.Store;

namespace ZarrNET.Core;

// =============================================================================
// Image descriptor — caller fills this in before handing off to the writer
// =============================================================================

/// <summary>
/// Everything the writer needs to know about a 5D image being saved.
/// Shape is always interpreted as (T, C, Z, Y, X) in this descriptor.
/// The caller converts from their internal BioImage representation into this.
/// All dimension sizes must be at least 1.
/// </summary>
public class BioImageDescriptor
{
    public string  Name         { get; init; } = "image";
    public string  DataType     { get; init; } = "uint16";   // Zarr v3 data_type string

    public int     SizeT        { get; }
    public int     SizeC        { get; }
    public int     SizeZ        { get; }
    public int     SizeY        { get; }
    public int     SizeX        { get; }

    // Physical pixel sizes (µm). Used to populate coordinate transformations.
    public double  PhysicalSizeZ { get; init; } = 1.0;
    public double  PhysicalSizeY { get; init; } = 1.0;
    public double  PhysicalSizeX { get; init; } = 1.0;

    // Chunk layout — sensible defaults for a single full XY plane per chunk.
    public int ChunkT { get; init; } = 1;
    public int ChunkC { get; init; } = 1;
    public int ChunkZ { get; init; } = 1;
    public int ChunkY { get; init; } = 512;
    public int ChunkX { get; init; } = 512;

    public long[] Shape    => [SizeT, SizeC, SizeZ, SizeY, SizeX];
    public int[]  Chunks   => [ChunkT, ChunkC, ChunkZ, ChunkY, ChunkX];

    public BioImageDescriptor(int sizeX, int sizeY, ZCT coord)
    {
        SizeX = sizeX;
        SizeY = sizeY;
        SizeZ = coord.Z;
        SizeC = coord.C;
        SizeT = coord.T;
    }
}

// =============================================================================
// Writer
// =============================================================================

/// <summary>
/// Creates a new OME-Zarr v3 dataset on disk and writes pixel data into it.
///
/// Writing always goes through two phases:
///   1. Bootstrap — write zarr.json metadata for the root group and each array.
///   2. Fill — open the bootstrapped arrays and stream pixel data in.
///
/// Usage:
/// <code>
///   var descriptor = new BioImageDescriptor { SizeY = 1024, SizeX = 1024, ... };
///
///   await using var writer = OmeZarrWriter.Create("/path/to/output.zarr", descriptor);
///   await writer.WritePixelDataAsync(pixelBytes);
/// </code>
/// </summary>
public sealed class OmeZarrWriter : IAsyncDisposable
{
    private readonly IZarrStore         _store;
    private readonly BioImageDescriptor _descriptor;
    private readonly string             _arrayPath = "0";    // single-level, resolution 0
    private bool                        _disposed;

    private OmeZarrWriter(IZarrStore store, BioImageDescriptor descriptor)
    {
        _store      = store;
        _descriptor = descriptor;
    }

    // -------------------------------------------------------------------------
    // Public factory
    // -------------------------------------------------------------------------

    /// <summary>
    /// Creates the output directory, writes OME-Zarr metadata into it, and
    /// returns a writer ready to receive pixel data.
    /// </summary>
    public static async Task<OmeZarrWriter> CreateAsync(
        string              outputPath,
        BioImageDescriptor  descriptor,
        CancellationToken   ct = default)
    {
        Directory.CreateDirectory(outputPath);

        var store  = new LocalFileSystemStore(outputPath);
        var writer = new OmeZarrWriter(store, descriptor);

        await writer.BootstrapMetadataAsync(ct).ConfigureAwait(false);

        return writer;
    }

    // -------------------------------------------------------------------------
    // Public write API
    // -------------------------------------------------------------------------

    /// <summary>
    /// Writes the full pixel buffer for the entire image in one call.
    /// <paramref name="pixelData"/> must be a flat C-order byte array whose
    /// length equals Product(Shape) × ElementSizeBytes.
    /// </summary>
    public async Task WritePixelDataAsync(
        byte[]            pixelData,
        CancellationToken ct = default)
    {
        ThrowIfDisposed();

        var array       = await OpenArrayAsync(ct).ConfigureAwait(false);
        var regionStart = new long[array.Metadata.Rank];
        var regionEnd   = array.Metadata.Shape;

        await array.WriteRegionAsync(regionStart, regionEnd, pixelData, ct)
                   .ConfigureAwait(false);
    }

    /// <summary>
    /// Writes a single Z-plane (all T and C) into the array.
    /// Useful for streaming plane-by-plane from a source that can't hold
    /// the entire volume in memory at once.
    /// </summary>
    public async Task WritePlaneAsync(
        int               zIndex,
        byte[]            planeData,
        CancellationToken ct = default)
    {
        ThrowIfDisposed();

        var d   = _descriptor;
        var arr = await OpenArrayAsync(ct).ConfigureAwait(false);

        var regionStart = new long[] { 0,      0,      zIndex,     0,      0 };
        var regionEnd   = new long[] { d.SizeT, d.SizeC, zIndex + 1, d.SizeY, d.SizeX };

        await arr.WriteRegionAsync(regionStart, regionEnd, planeData, ct)
                 .ConfigureAwait(false);
    }

    /// <summary>
    /// Writes an arbitrary sub-region into the 5D array.
    ///
    /// This is the tile-friendly entry point: callers can write a single
    /// (t, c, z) tile of size (height × width) at any YX offset without
    /// needing to hold a full plane in memory.
    ///
    /// <paramref name="data"/> must contain exactly
    /// height × width × ElementSizeBytes bytes in C-order.
    /// </summary>
    public async Task WriteRegionAsync(
        int    t,       int c,      int z,
        int    yOffset, int xOffset,
        int    height,  int width,
        byte[] data,
        CancellationToken ct = default)
    {
        ThrowIfDisposed();

        var arr = await OpenArrayAsync(ct).ConfigureAwait(false);

        var regionStart = new long[] { t,     c,     z,     yOffset,          xOffset };
        var regionEnd   = new long[] { t + 1, c + 1, z + 1, yOffset + height, xOffset + width };

        await arr.WriteRegionAsync(regionStart, regionEnd, data, ct)
                 .ConfigureAwait(false);
    }

    // -------------------------------------------------------------------------
    // Metadata bootstrap
    // -------------------------------------------------------------------------

    private async Task BootstrapMetadataAsync(CancellationToken ct)
    {
        await WriteRootGroupMetadataAsync(ct).ConfigureAwait(false);
        await WriteArrayMetadataAsync(ct).ConfigureAwait(false);
    }

    private async Task WriteRootGroupMetadataAsync(CancellationToken ct)
    {
        var d = _descriptor;

        // Coordinate transformation: one scale entry per axis (T, C, Z, Y, X).
        // T and C are dimensionless (scale = 1), spatial axes carry physical size.
        var scaleTransform = new
        {
            type  = "scale",
            scale = new[] { 1.0, 1.0, d.PhysicalSizeZ, d.PhysicalSizeY, d.PhysicalSizeX }
        };

        var multiscale = new
        {
            version = "0.5",
            name    = d.Name,
            axes    = new object[]
            {
                new { name = "t", type = "time"    },
                new { name = "c", type = "channel" },
                new { name = "z", type = "space", unit = "micrometer" },
                new { name = "y", type = "space", unit = "micrometer" },
                new { name = "x", type = "space", unit = "micrometer" }
            },
            datasets = new object[]
            {
                new
                {
                    path = _arrayPath,
                    coordinateTransformations = new[] { scaleTransform }
                }
            },
            coordinateTransformations = new[] { scaleTransform }
        };

        // NGFF 0.5: OME metadata is wrapped under an "ome" envelope in zarr.json
        // attributes. This replaces the flat layout used in 0.4.
        var rootGroupDoc = new
        {
            zarr_format = 3,
            node_type   = "group",
            attributes  = new
            {
                ome = new
                {
                    version     = "0.5",
                    multiscales = new[] { multiscale }
                }
            }
        };

        await WriteJsonAsync("zarr.json", rootGroupDoc, ct).ConfigureAwait(false);
    }

    private async Task WriteArrayMetadataAsync(CancellationToken ct)
    {
        var d = _descriptor;

        // Element size drives the Blosc shuffle typesize — must match the
        // data type so that shuffle/unshuffle transposes at the correct
        // element boundary (e.g. 2 for uint16, 4 for float32).
        var elementSize = ZarrDataType.Parse(d.DataType).ElementSize;

        // NGFF 0.5 requires dimension_names in array metadata (MUST).
        // These must match the axes declared in the multiscales metadata.
        var arrayDoc = new
        {
            zarr_format = 3,
            node_type   = "array",
            shape       = d.Shape,
            data_type   = d.DataType,
            chunk_grid  = new
            {
                name          = "regular",
                configuration = new { chunk_shape = d.Chunks }
            },
            chunk_key_encoding = new
            {
                name          = "default",
                configuration = new { separator = "/" }
            },
            fill_value      = 0,
            dimension_names = new[] { "t", "c", "z", "y", "x" },
            codecs          = new object[]
            {
                new
                {
                    name          = "blosc",
                    configuration = new
                    {
                        cname     = "lz4",
                        clevel    = 5,
                        shuffle   = "byteshuffle",
                        typesize  = elementSize,
                        blocksize = 0
                    }
                }
            }
        };

        var arrayKey = $"{_arrayPath}/zarr.json";
        await WriteJsonAsync(arrayKey, arrayDoc, ct).ConfigureAwait(false);
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private async Task<ZarrArray> OpenArrayAsync(CancellationToken ct)
    {
        var rootGroup = await ZarrGroup.OpenRootAsync(_store, ct).ConfigureAwait(false);
        return await rootGroup.OpenArrayAsync(_arrayPath, ct).ConfigureAwait(false);
    }

    private async Task WriteJsonAsync(string key, object document, CancellationToken ct)
    {
        var options = new JsonSerializerOptions { WriteIndented = true };
        var json    = JsonSerializer.SerializeToUtf8Bytes(document, options);

        await _store.WriteAsync(key, json, ct).ConfigureAwait(false);
    }

    private void ThrowIfDisposed()
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(OmeZarrWriter));
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;
        await _store.DisposeAsync().ConfigureAwait(false);
    }
}
