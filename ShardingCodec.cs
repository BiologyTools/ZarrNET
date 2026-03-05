using System.Buffers.Binary;
using OmeZarr.Core.Zarr.Metadata;
using OmeZarr.Core.Zarr.Store;

namespace OmeZarr.Core.Zarr.Codecs;

/// <summary>
/// Configuration for a sharding_indexed codec parsed from zarr.json.
///
/// Sharding packs multiple "inner chunks" (sub-chunks) into a single shard
/// file. The shard file contains the compressed inner chunks plus a binary
/// index that maps each inner chunk's grid position to its (offset, length)
/// within the shard.
///
/// zarr.json example:
/// <code>
///   "codecs": [{
///     "name": "sharding_indexed",
///     "configuration": {
///       "chunk_shape": [64, 64],
///       "codecs": [{"name": "bytes"}, {"name": "blosc", ...}],
///       "index_codecs": [{"name": "bytes", "configuration": {"endian": "little"}}],
///       "index_location": "end"
///     }
///   }]
/// </code>
///
/// The outer chunk_grid.chunk_shape in zarr.json defines the shard shape.
/// The inner chunk_shape here defines the sub-chunk shape within each shard.
/// </summary>
public sealed class ShardingConfig
{
    /// <summary>Inner chunk (sub-chunk) shape within each shard.</summary>
    public int[]         InnerChunkShape  { get; init; } = Array.Empty<int>();

    /// <summary>Codec pipeline applied to each inner chunk.</summary>
    public CodecPipeline InnerPipeline    { get; init; } = null!;

    /// <summary>Codec pipeline applied to the binary shard index.</summary>
    public CodecPipeline IndexPipeline    { get; init; } = null!;

    /// <summary>Where the index sits within the shard file: "end" or "start".</summary>
    public ShardIndexLocation IndexLocation { get; init; } = ShardIndexLocation.End;

    /// <summary>
    /// Number of inner chunks per shard along each axis.
    /// Computed from shard shape / inner chunk shape.
    /// </summary>
    public int[]         InnerChunksPerShard { get; init; } = Array.Empty<int>();

    /// <summary>Total number of inner chunks per shard (product of InnerChunksPerShard).</summary>
    public int           TotalInnerChunks { get; init; }

    /// <summary>
    /// Total byte size of the encoded shard index as stored on disk.
    /// This is the raw index data (TotalInnerChunks × 16 bytes) plus any
    /// overhead added by the index codecs (e.g. crc32c adds 4 bytes).
    ///
    /// Used by ShardReader to know how many bytes to slice from the shard
    /// file before feeding them into the index codec pipeline for decoding.
    /// </summary>
    public int           IndexEncodedSize { get; init; }
}

public enum ShardIndexLocation
{
    End,
    Start
}

/// <summary>
/// Reads individual inner chunks from a shard file.
///
/// Shard binary layout (index_location = "end"):
///   [inner chunk 0 bytes] [inner chunk 1 bytes] ... [index]
///
/// Index layout:
///   For each inner chunk in C-order grid position:
///     uint64 offset   (byte offset from start of shard, or 2^64-1 if empty)
///     uint64 nbytes   (byte count of the compressed inner chunk)
///
/// The index is decoded by the index_codecs pipeline (typically just "bytes"
/// with little-endian). Empty chunks are indicated by offset = 0xFFFFFFFFFFFFFFFF
/// and nbytes = 0xFFFFFFFFFFFFFFFF.
/// </summary>
public static class ShardReader
{
    private const ulong EmptyMarker = 0xFFFF_FFFF_FFFF_FFFF;

    // -------------------------------------------------------------------------
    // Public API
    // -------------------------------------------------------------------------

    /// <summary>
    /// Reads a single inner chunk from a shard file.
    ///
    /// <paramref name="shardBytes"/> is the raw shard file content.
    /// <paramref name="innerChunkCoord"/> is the inner chunk's grid position
    /// within the shard (not the global chunk coordinate).
    /// </summary>
    public static async Task<byte[]?> ReadInnerChunkAsync(
        byte[]          shardBytes,
        long[]          innerChunkCoord,
        ShardingConfig  config,
        CancellationToken ct = default)
    {
        // Parse the shard index
        var index = await ParseShardIndexAsync(shardBytes, config, ct).ConfigureAwait(false);

        // Find this inner chunk in the index
        var flatIndex    = ComputeFlatInnerIndex(innerChunkCoord, config.InnerChunksPerShard);
        var indexEntry   = index[flatIndex];

        // Empty chunk — caller should use fill value
        if (indexEntry.Offset == EmptyMarker && indexEntry.NBytes == EmptyMarker)
            return null;

        // Extract and decode the inner chunk bytes
        var offset      = (int)indexEntry.Offset;
        var nbytes      = (int)indexEntry.NBytes;
        var compressed  = new byte[nbytes];
        Array.Copy(shardBytes, offset, compressed, 0, nbytes);

        var decoded = await config.InnerPipeline.DecodeAsync(compressed, ct).ConfigureAwait(false);
        return decoded;
    }

    // -------------------------------------------------------------------------
    // Index parsing
    // -------------------------------------------------------------------------

    /// <summary>
    /// Extracts and decodes the shard index from the shard file bytes.
    /// The index is an array of (offset, nbytes) pairs, one per inner chunk.
    /// </summary>
    private static async Task<ShardIndexEntry[]> ParseShardIndexAsync(
        byte[]          shardBytes,
        ShardingConfig  config,
        CancellationToken ct)
    {
        var entryCount      = config.TotalInnerChunks;
        var encodedIndexSize = config.IndexEncodedSize;

        // Extract the encoded index bytes from the shard.
        // The encoded size includes any overhead from index codecs (e.g. crc32c
        // appends 4 bytes). We must slice exactly this many bytes so the codec
        // pipeline receives the complete encoded payload.
        byte[] encodedIndexBytes;
        if (config.IndexLocation == ShardIndexLocation.End)
        {
            var indexStart = shardBytes.Length - encodedIndexSize;
            encodedIndexBytes = new byte[encodedIndexSize];
            Array.Copy(shardBytes, indexStart, encodedIndexBytes, 0, encodedIndexSize);
        }
        else // Start
        {
            encodedIndexBytes = new byte[encodedIndexSize];
            Array.Copy(shardBytes, 0, encodedIndexBytes, 0, encodedIndexSize);
        }

        // Decode through the index codec pipeline (handles crc32c + endianness)
        var decodedIndex = await config.IndexPipeline.DecodeAsync(encodedIndexBytes, ct)
            .ConfigureAwait(false);

        // Parse the (offset, nbytes) pairs
        var entries = new ShardIndexEntry[entryCount];
        for (int i = 0; i < entryCount; i++)
        {
            var byteOffset = i * 16;
            var offset     = BinaryPrimitives.ReadUInt64LittleEndian(
                                decodedIndex.AsSpan(byteOffset, 8));
            var nbytes     = BinaryPrimitives.ReadUInt64LittleEndian(
                                decodedIndex.AsSpan(byteOffset + 8, 8));

            entries[i] = new ShardIndexEntry(offset, nbytes);
        }

        return entries;
    }

    // -------------------------------------------------------------------------
    // Coordinate mapping
    // -------------------------------------------------------------------------

    /// <summary>
    /// Converts an N-dimensional inner chunk coordinate to a flat index
    /// in C-order (row-major) within the shard's inner chunk grid.
    /// </summary>
    private static int ComputeFlatInnerIndex(long[] innerCoord, int[] innerChunksPerShard)
    {
        int index  = 0;
        int stride = 1;

        for (int d = innerCoord.Length - 1; d >= 0; d--)
        {
            index  += (int)innerCoord[d] * stride;
            stride *= innerChunksPerShard[d];
        }

        return index;
    }

    // -------------------------------------------------------------------------
    // Types
    // -------------------------------------------------------------------------

    private readonly record struct ShardIndexEntry(ulong Offset, ulong NBytes);
}
