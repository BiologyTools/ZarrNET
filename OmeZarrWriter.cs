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
// Per-level descriptor — one per pyramid resolution
// =============================================================================

/// <summary>
/// Describes a single resolution level in a multi-scale pyramid.
/// SizeX and SizeY are the dimensions at this level; all other dimensions
/// are inherited from the parent <see cref="BioImageDescriptor"/>.
/// </summary>
public class ResolutionLevelDescriptor
{
    public int    SizeX      { get; }
    public int    SizeY      { get; }
    /// <summary>
    /// Downsample factor relative to level 0 (full resolution).
    /// Level 0 = 1.0, level 1 = 2.0, etc.
    /// </summary>
    public double Downsample { get; }

    public ResolutionLevelDescriptor(int sizeX, int sizeY, double downsample)
    {
        SizeX      = sizeX;
        SizeY      = sizeY;
        Downsample = downsample;
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
/// Supports both single-resolution and multi-scale pyramid output.
/// When resolution levels are provided via <see cref="CreateAsync(string, BioImageDescriptor, IReadOnlyList{ResolutionLevelDescriptor}, CancellationToken)"/>,
/// the writer creates one Zarr array per level (paths "0", "1", "2", …) and
/// a multi-scale datasets entry for each in the root zarr.json.
///
/// Usage:
/// <code>
///   var descriptor = new BioImageDescriptor { SizeY = 1024, SizeX = 1024, ... };
///   var levels = new[] {
///       new ResolutionLevelDescriptor(1024, 1024, 1.0),
///       new ResolutionLevelDescriptor(512,  512,  2.0),
///   };
///   await using var writer = OmeZarrWriter.CreateAsync("/path/to/output.zarr", descriptor, levels);
///   await writer.WriteRegionAsync(t, c, z, y, x, h, w, bytes, levelIndex: 0);
///   await writer.WriteRegionAsync(t, c, z, y, x, h, w, bytes, levelIndex: 1);
/// </code>
/// </summary>
public sealed class OmeZarrWriter : IAsyncDisposable
{
    private readonly IZarrStore                          _store;
    private readonly BioImageDescriptor                  _descriptor;
    private readonly IReadOnlyList<ResolutionLevelDescriptor> _levels;
    private bool                                         _disposed;

    private OmeZarrWriter(
        IZarrStore                               store,
        BioImageDescriptor                       descriptor,
        IReadOnlyList<ResolutionLevelDescriptor> levels)
    {
        _store      = store;
        _descriptor = descriptor;
        _levels     = levels;
    }

    // -------------------------------------------------------------------------
    // Public factories
    // -------------------------------------------------------------------------

    /// <summary>
    /// Single-resolution convenience overload (backward-compatible).
    /// Creates a single array at path "0".
    /// </summary>
    public static Task<OmeZarrWriter> CreateAsync(
        string             outputPath,
        BioImageDescriptor descriptor,
        CancellationToken  ct = default)
    {
        var singleLevel = new[]
        {
            new ResolutionLevelDescriptor(descriptor.SizeX, descriptor.SizeY, 1.0)
        };
        return CreateAsync(outputPath, descriptor, singleLevel, ct);
    }

    /// <summary>
    /// Multi-scale factory. Creates one Zarr array per entry in
    /// <paramref name="levels"/>, with paths "0", "1", "2", … and a full
    /// OME-NGFF multiscales block pointing at all of them.
    /// </summary>
    public static async Task<OmeZarrWriter> CreateAsync(
        string                                   outputPath,
        BioImageDescriptor                       descriptor,
        IReadOnlyList<ResolutionLevelDescriptor> levels,
        CancellationToken                        ct = default)
    {
        if (levels == null || levels.Count == 0)
            throw new ArgumentException("At least one resolution level is required.", nameof(levels));

        Directory.CreateDirectory(outputPath);

        var store  = new LocalFileSystemStore(outputPath);
        var writer = new OmeZarrWriter(store, descriptor, levels);

        await writer.BootstrapMetadataAsync(ct).ConfigureAwait(false);

        return writer;
    }

    // -------------------------------------------------------------------------
    // Public write API
    // -------------------------------------------------------------------------

    /// <summary>
    /// Writes the full pixel buffer for the entire image at level 0 in one call.
    /// <paramref name="pixelData"/> must be a flat C-order byte array whose
    /// length equals Product(Shape) × ElementSizeBytes.
    /// </summary>
    public async Task WritePixelDataAsync(
        byte[]            pixelData,
        CancellationToken ct = default)
    {
        ThrowIfDisposed();

        var array       = await OpenArrayAsync(0, ct).ConfigureAwait(false);
        var regionStart = new long[array.Metadata.Rank];
        var regionEnd   = array.Metadata.Shape;

        await array.WriteRegionAsync(regionStart, regionEnd, pixelData, ct)
                   .ConfigureAwait(false);
    }

    /// <summary>
    /// Writes a single Z-plane (all T and C) into the array at level 0.
    /// </summary>
    public async Task WritePlaneAsync(
        int               zIndex,
        byte[]            planeData,
        CancellationToken ct = default)
    {
        ThrowIfDisposed();

        var d   = _descriptor;
        var arr = await OpenArrayAsync(0, ct).ConfigureAwait(false);

        var regionStart = new long[] { 0,       0,       zIndex,     0,       0 };
        var regionEnd   = new long[] { d.SizeT, d.SizeC, zIndex + 1, d.SizeY, d.SizeX };

        await arr.WriteRegionAsync(regionStart, regionEnd, planeData, ct)
                 .ConfigureAwait(false);
    }

    /// <summary>
    /// Writes an arbitrary sub-region into the 5D array at the given resolution
    /// <paramref name="levelIndex"/> (0 = full resolution).
    ///
    /// <paramref name="data"/> must contain exactly
    /// height × width × ElementSizeBytes bytes in C-order.
    /// </summary>
    public async Task WriteRegionAsync(
        int    t,       int c,      int z,
        int    yOffset, int xOffset,
        int    height,  int width,
        byte[] data,
        int    levelIndex     = 0,
        CancellationToken ct  = default)
    {
        ThrowIfDisposed();

        var arr = await OpenArrayAsync(levelIndex, ct).ConfigureAwait(false);

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

        for (int i = 0; i < _levels.Count; i++)
            await WriteArrayMetadataAsync(i, ct).ConfigureAwait(false);
    }

    private async Task WriteRootGroupMetadataAsync(CancellationToken ct)
    {
        var d = _descriptor;

        // Build one datasets entry per resolution level.
        // Each level gets its own scale transform reflecting its downsample factor.
        var datasets = _levels.Select((lvl, i) => new
        {
            path = i.ToString(),
            coordinateTransformations = new object[]
            {
                new
                {
                    type  = "scale",
                    scale = new[]
                    {
                        1.0,
                        1.0,
                        d.PhysicalSizeZ,
                        d.PhysicalSizeY * lvl.Downsample,
                        d.PhysicalSizeX * lvl.Downsample
                    }
                }
            }
        }).ToArray();

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
            datasets                  = datasets,
            coordinateTransformations = new object[]
            {
                new
                {
                    type  = "scale",
                    scale = new[] { 1.0, 1.0, d.PhysicalSizeZ, d.PhysicalSizeY, d.PhysicalSizeX }
                }
            }
        };

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

    private async Task WriteArrayMetadataAsync(int levelIndex, CancellationToken ct)
    {
        var d   = _descriptor;
        var lvl = _levels[levelIndex];

        var elementSize = ZarrDataType.Parse(d.DataType).ElementSize;

        // Chunk sizes: clamp to the level's actual dimensions so we never
        // create chunks larger than the array they belong to.
        int chunkY = Math.Min(d.ChunkY, lvl.SizeY);
        int chunkX = Math.Min(d.ChunkX, lvl.SizeX);

        var arrayDoc = new
        {
            zarr_format = 3,
            node_type   = "array",
            shape       = new long[] { d.SizeT, d.SizeC, d.SizeZ, lvl.SizeY, lvl.SizeX },
            data_type   = d.DataType,
            chunk_grid  = new
            {
                name          = "regular",
                configuration = new { chunk_shape = new[] { d.ChunkT, d.ChunkC, d.ChunkZ, chunkY, chunkX } }
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

        var arrayKey = $"{levelIndex}/zarr.json";
        await WriteJsonAsync(arrayKey, arrayDoc, ct).ConfigureAwait(false);
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private async Task<ZarrArray> OpenArrayAsync(int levelIndex, CancellationToken ct)
    {
        var arrayPath = levelIndex.ToString();
        var rootGroup = await ZarrGroup.OpenRootAsync(_store, ct).ConfigureAwait(false);
        return await rootGroup.OpenArrayAsync(arrayPath, ct).ConfigureAwait(false);
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
