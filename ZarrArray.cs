using System.Buffers;
using System.Collections.Concurrent;
using ZarrNET;
using ZarrNET;
using ZarrNET.Core.Zarr.Store;

namespace ZarrNET.Core.Zarr;

/// <summary>
/// Represents a single Zarr v3 array. Knows how to read and write chunks
/// via the store using the array's codec pipeline and chunk key encoding.
///
/// When sharding is active (Metadata.Sharding is non-null), the outer
/// ChunkShape represents the shard shape and the actual data granularity
/// is the inner chunk shape. ReadRegionAsync transparently handles both
/// cases — callers do not need to know whether sharding is in use.
///
/// Does not interpret array contents — that is the responsibility of callers
/// who know the OME axis semantics.
/// </summary>
public sealed class ZarrArray
{
    private readonly IZarrStore _store;
    private readonly string _arrayPath;   // store-relative path to the array root
    private readonly CodecPipeline _pipeline;

    // Effective chunk shape — inner chunk shape when sharding, outer chunk shape otherwise.
    // This is the granularity at which ReadRegionAsync iterates and assembles data.
    private readonly long[] _chunkShapeLong;
    private readonly long   _chunkElementCount;

    public ZarrArrayMetadata Metadata { get; }

    internal ZarrArray(IZarrStore store, string arrayPath, ZarrArrayMetadata metadata)
    {
        _store = store;
        _arrayPath = arrayPath.TrimEnd('/');
        Metadata = metadata;
        _pipeline = CodecFactory.BuildPipeline(metadata);

        // When sharding is active, the effective chunk shape is the inner chunk shape.
        // The outer chunk shape (shard shape) is used only for building store keys.
        var effectiveChunkShape = metadata.Sharding?.InnerChunkShape ?? metadata.ChunkShape;

        _chunkShapeLong = effectiveChunkShape.Select(s => (long)s).ToArray();
        _chunkElementCount = effectiveChunkShape.Aggregate(1L, (acc, s) => acc * s);
    }

    // -------------------------------------------------------------------------
    // Public API
    // -------------------------------------------------------------------------
    public const int MaxParallelChunks = 16;
    /// <summary>
    /// Reads a region of the array defined by per-axis [start, end) ranges.
    /// Returns the decoded bytes for that region, assembled from the
    /// relevant chunks. Shape of the returned region is (end - start) per axis.
    ///
    /// Chunks are fetched and decoded in parallel, bounded by maxParallelChunks.
    /// Each chunk maps to a non-overlapping slice of the output buffer, so the
    /// copy step is safe without locking.
    /// </summary>
    /// <param name="regionStart">Per-axis inclusive start indices.</param>
    /// <param name="regionEnd">Per-axis exclusive end indices.</param>
    /// <param name="maxParallelChunks">
    /// Maximum number of chunks to fetch/decode concurrently.
    /// Defaults to <see cref="Environment.ProcessorCount"/>.
    /// HTTP callers may benefit from higher values (e.g. 16–32);
    /// local disk callers can leave the default or lower it.
    /// Pass 1 to disable parallelism.
    /// </param>
    /// <param name="ct">Cancellation token.</param>
    public async Task<byte[]> ReadRegionAsync(
        long[] regionStart,
        long[] regionEnd,
        int? maxParallelChunks = MaxParallelChunks,
        CancellationToken ct = default)
    {
        ValidateRegion(regionStart, regionEnd);

        var regionShape = ComputeRegionShape(regionStart, regionEnd);
        var elementSize = Metadata.DataType.ElementSize;
        var totalElements = ComputeTotalElements(regionShape);
        var outputBuffer = new byte[totalElements * elementSize];

        var chunkCoords = EnumerateChunkCoordinates(regionStart, regionEnd);
        var parallelism = maxParallelChunks ?? Environment.ProcessorCount;

        // When sharding is active, cache shard file bytes so that multiple inner
        // chunks within the same shard don't trigger redundant store reads.
        // The cache is scoped to this single ReadRegionAsync call.
        var shardCache = Metadata.Sharding is not null
            ? new ConcurrentDictionary<string, Task<byte[]?>>(StringComparer.Ordinal)
            : null;

        var options = new ParallelOptions
        {
            MaxDegreeOfParallelism = Math.Max(1, parallelism),
            CancellationToken = ct
        };

        await Parallel.ForEachAsync(chunkCoords, options, async (chunkCoord, token) =>
        {
            var chunkData = await ReadChunkAsync(chunkCoord, shardCache, token).ConfigureAwait(false);

            CopyChunkRegionToOutput(
                chunkCoord,
                chunkData,
                regionStart,
                regionEnd,
                regionShape,
                outputBuffer,
                elementSize);
        }).ConfigureAwait(false);

        return outputBuffer;
    }

    /// <summary>
    /// Writes a region of the array defined by per-axis [start, end) ranges.
    /// Data must be a flat byte array matching the region shape exactly.
    /// Performs read-modify-write for partial chunk writes.
    /// </summary>
    public async Task WriteRegionAsync(
        long[] regionStart,
        long[] regionEnd,
        byte[] data,
        CancellationToken ct = default)
    {
        ValidateRegion(regionStart, regionEnd);

        var regionShape = ComputeRegionShape(regionStart, regionEnd);
        var elementSize = Metadata.DataType.ElementSize;
        var expectedBytes = ComputeTotalElements(regionShape) * elementSize;

        if (data.Length != expectedBytes)
            throw new ArgumentException(
                $"Data length {data.Length} does not match region size {expectedBytes} bytes.");

        var chunkCoords = EnumerateChunkCoordinates(regionStart, regionEnd);

        foreach (var chunkCoord in chunkCoords)
        {
            ct.ThrowIfCancellationRequested();

            await WriteChunkRegionAsync(
                chunkCoord,
                regionStart,
                regionEnd,
                regionShape,
                data,
                elementSize,
                ct).ConfigureAwait(false);
        }
    }

    // -------------------------------------------------------------------------
    // Chunk reading / writing
    // -------------------------------------------------------------------------

    private async Task<byte[]> ReadChunkAsync(
        long[] chunkCoord,
        ConcurrentDictionary<string, Task<byte[]?>>? shardCache,
        CancellationToken ct)
    {
        // Sharded path — chunkCoord addresses an inner chunk, not a shard
        if (Metadata.Sharding is not null)
            return await ReadShardedChunkAsync(chunkCoord, shardCache!, ct).ConfigureAwait(false);

        // Non-sharded path — one store key per chunk
        return await ReadDirectChunkAsync(chunkCoord, ct).ConfigureAwait(false);
    }

    // -------------------------------------------------------------------------
    // Non-sharded chunk read
    // -------------------------------------------------------------------------

    private async Task<byte[]> ReadDirectChunkAsync(long[] chunkCoord, CancellationToken ct)
    {
        var key = BuildChunkKey(chunkCoord);
        var bytes = await _store.ReadAsync(key, ct).ConfigureAwait(false);

        if (bytes is null)
            return BuildFillValueChunk();

        var decoded = await _pipeline.DecodeAsync(bytes, ct).ConfigureAwait(false);

        return PadOrValidateDecodedChunk(decoded, chunkCoord);
    }

    // -------------------------------------------------------------------------
    // Sharded chunk read
    // -------------------------------------------------------------------------

    /// <summary>
    /// Reads an inner chunk from within a shard file. The chunkCoord here is
    /// the global inner-chunk coordinate (based on inner chunk shape). We compute
    /// which shard it belongs to, fetch that shard (with caching), and extract
    /// the inner chunk.
    /// </summary>
    private async Task<byte[]> ReadShardedChunkAsync(
        long[] chunkCoord,
        ConcurrentDictionary<string, Task<byte[]?>> shardCache,
        CancellationToken ct)
    {
        var sharding   = Metadata.Sharding!;
        var rank       = Metadata.Rank;

        // Compute which shard this inner chunk belongs to, and its position within
        var shardCoord      = new long[rank];
        var innerChunkCoord = new long[rank];

        for (int d = 0; d < rank; d++)
        {
            var innersPerShard   = sharding.InnerChunksPerShard[d];
            var globalInnerIndex = chunkCoord[d];

            shardCoord[d]      = globalInnerIndex / innersPerShard;
            innerChunkCoord[d] = globalInnerIndex % innersPerShard;
        }

        // Fetch the shard file, using the cache to avoid redundant reads
        var shardKey   = BuildChunkKey(shardCoord);
        var shardBytes = await shardCache.GetOrAdd(
            shardKey,
            key => _store.ReadAsync(key, ct)
        ).ConfigureAwait(false);

        if (shardBytes is null)
            return BuildFillValueChunk();

        // Extract the inner chunk from the shard
        var innerChunkBytes = await ShardReader.ReadInnerChunkAsync(
            shardBytes, innerChunkCoord, sharding, ct).ConfigureAwait(false);

        if (innerChunkBytes is null)
            return BuildFillValueChunk();

        return PadOrValidateDecodedChunk(innerChunkBytes, chunkCoord);
    }

    // -------------------------------------------------------------------------
    // Decoded chunk validation / padding (shared by both paths)
    // -------------------------------------------------------------------------

    private byte[] PadOrValidateDecodedChunk(byte[] decoded, long[] chunkCoord)
    {
        var expectedBytes = _chunkElementCount * Metadata.DataType.ElementSize;
        var actualBytes   = decoded.Length;

        if (actualBytes == expectedBytes)
            return decoded;

        // Truncated edge chunks: some implementations write only the valid portion
        if (actualBytes < expectedBytes)
        {
            int elementSize    = Metadata.DataType.ElementSize;
            var padded         = BuildFillValueChunk();
            var truncatedShape = ComputeTruncatedChunkShape(chunkCoord);
            var expectedTruncatedBytes = ComputeTotalElements(truncatedShape) * elementSize;

            if (actualBytes == (int)expectedTruncatedBytes)
                ExpandTruncatedChunk(decoded, truncatedShape, padded, _chunkShapeLong, elementSize);
            else
                Array.Copy(decoded, 0, padded, 0, decoded.Length);  // unknown truncation — best effort

            return padded;
        }

        throw new InvalidOperationException(
            $"Decoded chunk at {string.Join(",", chunkCoord)} has {actualBytes} bytes, " +
            $"expected {expectedBytes} bytes. Chunk shape: [{string.Join(", ", _chunkShapeLong)}], " +
            $"element size: {Metadata.DataType.ElementSize} bytes.");
    }

    private async Task WriteChunkAsync(long[] chunkCoord, byte[] decodedData, CancellationToken ct)
    {
        var encoded = await _pipeline.EncodeAsync(decodedData, ct).ConfigureAwait(false);
        var key = BuildChunkKey(chunkCoord);

        await _store.WriteAsync(key, encoded, ct).ConfigureAwait(false);
    }

    private async Task WriteChunkRegionAsync(
        long[] chunkCoord,
        long[] regionStart,
        long[] regionEnd,
        long[] regionShape,
        byte[] sourceData,
        int elementSize,
        CancellationToken ct)
    {
        // Write path doesn't use shard caching — pass null (non-sharded only for now)
        var chunkData = await ReadChunkAsync(chunkCoord, shardCache: null, ct).ConfigureAwait(false);
        var chunkOrigin = ComputeChunkOrigin(chunkCoord);
        var clampedStart = ClampToChunk(regionStart, chunkOrigin, _chunkShapeLong, clampToStart: true);
        var clampedEnd = ClampToChunk(regionEnd, chunkOrigin, _chunkShapeLong, clampToStart: false);

        CopyNdRegion(
            src: sourceData,
            srcOrigin: regionStart,
            srcShape: regionShape,
            dst: chunkData,
            dstOrigin: chunkOrigin,
            dstShape: _chunkShapeLong,
            copyStart: clampedStart,
            copyEnd: clampedEnd,
            elementSize: elementSize);

        await WriteChunkAsync(chunkCoord, chunkData, ct).ConfigureAwait(false);
    }

    // -------------------------------------------------------------------------
    // Region/chunk copy — row-contiguous fast path
    // -------------------------------------------------------------------------

    /// <summary>
    /// Copies the relevant portion of a decoded chunk into the output buffer
    /// at the correct offset for the requested region.
    ///
    /// Uses a row-contiguous fast path: the innermost (last) axis is copied
    /// with a single Buffer.BlockCopy per row rather than element-by-element.
    /// The outer axes are iterated with a reusable coordinate array to avoid
    /// per-element heap allocations.
    /// </summary>
    private void CopyChunkRegionToOutput(
        long[] chunkCoord,
        byte[] chunkData,
        long[] regionStart,
        long[] regionEnd,
        long[] regionShape,
        byte[] outputBuffer,
        int elementSize)
    {
        var chunkOrigin = ComputeChunkOrigin(chunkCoord);

        var rank = Metadata.Rank;
        var copyStart = new long[rank];
        var copyEnd = new long[rank];

        for (int d = 0; d < rank; d++)
        {
            copyStart[d] = Math.Max(regionStart[d], chunkOrigin[d]);
            copyEnd[d] = Math.Min(regionEnd[d], chunkOrigin[d] + _chunkShapeLong[d]);
        }

        CopyNdRegion(
            src: chunkData,
            srcOrigin: chunkOrigin,
            srcShape: _chunkShapeLong,
            dst: outputBuffer,
            dstOrigin: regionStart,
            dstShape: regionShape,
            copyStart: copyStart,
            copyEnd: copyEnd,
            elementSize: elementSize);
    }

    /// <summary>
    /// General-purpose N-dimensional copy between two C-order flat buffers.
    /// Copies the region [copyStart, copyEnd) from src into dst, where each
    /// buffer has its own origin and shape.
    ///
    /// The innermost axis is copied as a contiguous row with a single
    /// Buffer.BlockCopy call. Outer axes are iterated with a single reusable
    /// coordinate array — no per-element heap allocations.
    /// </summary>
    private static void CopyNdRegion(
        byte[] src,
        long[] srcOrigin,
        long[] srcShape,
        byte[] dst,
        long[] dstOrigin,
        long[] dstShape,
        long[] copyStart,
        long[] copyEnd,
        int elementSize)
    {
        var rank = srcShape.Length;

        // How many elements to copy along the innermost axis per row
        var innerCount = copyEnd[rank - 1] - copyStart[rank - 1];
        var rowBytes = (int)(innerCount * elementSize);

        if (rowBytes <= 0)
            return;

        // Pre-compute strides for src and dst (C-order: last axis has stride 1)
        var srcStrides = ComputeStrides(srcShape);
        var dstStrides = ComputeStrides(dstShape);

        // For rank-1 arrays, just do one copy
        if (rank == 1)
        {
            var srcByteOffset = (int)((copyStart[0] - srcOrigin[0]) * srcStrides[0] * elementSize);
            var dstByteOffset = (int)((copyStart[0] - dstOrigin[0]) * dstStrides[0] * elementSize);
            Buffer.BlockCopy(src, srcByteOffset, dst, dstByteOffset, rowBytes);
            return;
        }

        // Iterate outer axes [0..rank-2], copy full inner row each time.
        // Uses a single reusable coordinate array — no per-iteration allocations.
        var outerRank = rank - 1;
        var current = new long[outerRank];
        for (int d = 0; d < outerRank; d++)
            current[d] = copyStart[d];

        while (true)
        {
            // Compute flat byte offsets for the start of this row in src and dst
            long srcElement = (copyStart[rank - 1] - srcOrigin[rank - 1]);
            long dstElement = (copyStart[rank - 1] - dstOrigin[rank - 1]);

            for (int d = 0; d < outerRank; d++)
            {
                srcElement += (current[d] - srcOrigin[d]) * srcStrides[d];
                dstElement += (current[d] - dstOrigin[d]) * dstStrides[d];
            }

            var srcByteOffset = (int)(srcElement * elementSize);
            var dstByteOffset = (int)(dstElement * elementSize);

            Buffer.BlockCopy(src, srcByteOffset, dst, dstByteOffset, rowBytes);

            // Advance outer coordinates (last outer axis first, C order)
            int axis = outerRank - 1;
            while (axis >= 0)
            {
                current[axis]++;
                if (current[axis] < copyEnd[axis])
                    break;
                current[axis] = copyStart[axis];
                axis--;
            }

            if (axis < 0)
                break;
        }
    }

    /// <summary>
    /// Computes C-order strides for a given shape.
    /// stride[d] = product of shape[d+1..rank-1].  stride[rank-1] = 1.
    /// </summary>
    private static long[] ComputeStrides(long[] shape)
    {
        var rank = shape.Length;
        var strides = new long[rank];
        strides[rank - 1] = 1;

        for (int d = rank - 2; d >= 0; d--)
            strides[d] = strides[d + 1] * shape[d + 1];

        return strides;
    }

    // -------------------------------------------------------------------------
    // Chunk enumeration and key building
    // -------------------------------------------------------------------------

    private IEnumerable<long[]> EnumerateChunkCoordinates(long[] regionStart, long[] regionEnd)
    {
        var rank = Metadata.Rank;

        // Use inner chunk shape when sharded, outer chunk shape otherwise.
        // _chunkShapeLong is already set to the effective shape in the constructor.
        var chunkShape = _chunkShapeLong;

        var firstChunk = new long[rank];
        var lastChunkExclusive = new long[rank];

        for (int d = 0; d < rank; d++)
        {
            firstChunk[d] = regionStart[d] / chunkShape[d];

            // Last chunk that intersects the region, then +1 for exclusive end
            lastChunkExclusive[d] = ((regionEnd[d] - 1) / chunkShape[d]) + 1;
        }

        return IterateNdCoordinates(firstChunk, lastChunkExclusive, rank);
    }

    private string BuildChunkKey(long[] chunkCoord)
    {
        var sep = Metadata.ChunkKeySeparator;
        var coord = string.Join(sep, chunkCoord);

        // Zarr v2: chunks directly under array path
        //   - With "." separator: "arrayPath/0.0.0.0.0"
        //   - With "/" separator: "arrayPath/0/0/0/0/0"
        //
        // Zarr v3: chunks under c/ subdirectory with "/" separator
        //   - Always: "arrayPath/c/0/0/0/0/0"

        if (Metadata.ZarrVersion == 2)
        {
            return $"{_arrayPath}/{coord}";
        }
        else  // v3
        {
            return $"{_arrayPath}/c/{coord}";
        }
    }

    // -------------------------------------------------------------------------
    // Fill value
    // -------------------------------------------------------------------------

    private byte[] BuildFillValueChunk()
    {
        return new byte[_chunkElementCount * Metadata.DataType.ElementSize];
        // Fill value of 0 is used — a more complete implementation would
        // deserialise the fill_value from ZarrJsonDocument and populate here.
    }

    // -------------------------------------------------------------------------
    // Index / offset mathematics
    // -------------------------------------------------------------------------

    private long[] ComputeChunkOrigin(long[] chunkCoord)
    {
        var origin = new long[Metadata.Rank];
        for (int d = 0; d < Metadata.Rank; d++)
            origin[d] = chunkCoord[d] * _chunkShapeLong[d];
        return origin;
    }

    /// <summary>
    /// Returns the actual element count per dimension for a chunk at chunkCoord.
    /// For interior chunks this equals the effective chunk shape. For edge chunks
    /// it is clamped to the array extent, matching the layout of truncated
    /// edge-chunk files.
    /// </summary>
    private long[] ComputeTruncatedChunkShape(long[] chunkCoord)
    {
        var rank = Metadata.Rank;
        var truncated = new long[rank];

        for (int d = 0; d < rank; d++)
        {
            var origin = chunkCoord[d] * _chunkShapeLong[d];
            truncated[d] = Math.Min(Metadata.Shape[d] - origin, _chunkShapeLong[d]);
        }

        return truncated;
    }

    /// <summary>
    /// Copies elements from a C-order truncated chunk buffer (srcShape strides) into
    /// the correct positions of a full-size C-order buffer (dstShape strides).
    /// This is necessary when a Zarr implementation stores edge chunks without
    /// fill-value padding — the decoded rows are narrower than a full chunk row,
    /// so a flat copy would produce wrong strides starting from the second row.
    ///
    /// Uses the same row-contiguous fast path as CopyNdRegion: copies full rows
    /// along the innermost axis rather than element-by-element.
    /// </summary>
    private static void ExpandTruncatedChunk(
        byte[] src,
        long[] srcShape,
        byte[] dst,
        long[] dstShape,
        int elementSize)
    {
        var rank = srcShape.Length;
        var start = new long[rank];

        // src covers [0, srcShape) and dst covers [0, dstShape).
        // Both origins are zero, so we can use CopyNdRegion directly.
        var zeroOrigin = new long[rank];

        CopyNdRegion(
            src: src,
            srcOrigin: zeroOrigin,
            srcShape: srcShape,
            dst: dst,
            dstOrigin: zeroOrigin,
            dstShape: dstShape,
            copyStart: zeroOrigin,
            copyEnd: srcShape,
            elementSize: elementSize);
    }

    private static long[] ClampToChunk(
        long[] values,
        long[] chunkOrigin,
        long[] chunkShape,
        bool clampToStart)
    {
        var result = new long[values.Length];
        for (int d = 0; d < values.Length; d++)
        {
            result[d] = clampToStart
                ? Math.Max(values[d], chunkOrigin[d])
                : Math.Min(values[d], chunkOrigin[d] + chunkShape[d]);
        }
        return result;
    }

    private static long ComputeFlatIndex(long[] localIndices, long[] shape)
    {
        long index = 0;
        long stride = 1;

        for (int d = shape.Length - 1; d >= 0; d--)
        {
            index += localIndices[d] * stride;
            stride *= shape[d];
        }

        return index;
    }

    private static long[] ComputeRegionShape(long[] start, long[] end)
    {
        var shape = new long[start.Length];
        for (int d = 0; d < start.Length; d++)
            shape[d] = end[d] - start[d];
        return shape;
    }

    private static long ComputeTotalElements(long[] shape)
        => shape.Aggregate(1L, (acc, s) => acc * s);

    // -------------------------------------------------------------------------
    // N-dimensional iteration helpers
    // All iteration uses exclusive end: [start, end)
    // -------------------------------------------------------------------------

    private static IEnumerable<long[]> IterateNdCoordinates(
        long[] start,
        long[] end,    // EXCLUSIVE - [start, end)
        int rank)
    {
        var current = (long[])start.Clone();

        while (true)
        {
            yield return (long[])current.Clone();

            // Advance last-axis-first (C order / row-major)
            int d = rank - 1;
            while (d >= 0)
            {
                current[d]++;
                if (current[d] < end[d])  // Exclusive end
                    break;
                current[d] = start[d];
                d--;
            }

            if (d < 0)
                yield break;
        }
    }

    // -------------------------------------------------------------------------
    // Validation
    // -------------------------------------------------------------------------

    private void ValidateRegion(long[] regionStart, long[] regionEnd)
    {
        var rank = Metadata.Rank;

        if (regionStart.Length != rank)
            throw new ArgumentException(
                $"regionStart has {regionStart.Length} dimensions, expected {rank}.");

        if (regionEnd.Length != rank)
            throw new ArgumentException(
                $"regionEnd has {regionEnd.Length} dimensions, expected {rank}.");

        for (int d = 0; d < rank; d++)
        {
            if (regionStart[d] < 0 || regionStart[d] >= Metadata.Shape[d])
                throw new ArgumentOutOfRangeException(
                    $"regionStart[{d}] = {regionStart[d]} is out of bounds [0, {Metadata.Shape[d]}).");

            if (regionEnd[d] <= regionStart[d] || regionEnd[d] > Metadata.Shape[d])
                throw new ArgumentOutOfRangeException(
                    $"regionEnd[{d}] = {regionEnd[d]} is out of bounds ({regionStart[d]}, {Metadata.Shape[d]}].");
        }
    }
}
