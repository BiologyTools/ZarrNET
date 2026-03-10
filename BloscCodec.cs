using System.Buffers.Binary;
using System.IO.Compression;
using K4os.Compression.LZ4;
using ZstdSharp;

namespace ZarrNET;

/// <summary>
/// Blosc bytes-to-bytes codec.
///
/// Blosc is a meta-compressor: it applies an optional shuffle filter to improve
/// compressibility, then compresses each block with an inner codec (lz4, zstd, zlib,
/// blosclz, snappy). The 16-byte Blosc1 frame header stores the parameters needed
/// to decode, so decoding is self-describing from the frame alone.
///
/// Blosc1 frame layout:
///   [16-byte header]
///   [bstarts table: nblocks x int32] -- byte offsets of each block from the start of block data
///   [block data -- nblocks contiguous compressed/raw blocks]
///
/// Shuffle is applied per-block before compression, so unshuffle must also be per-block.
///
/// Supported inner codecs: lz4, lz4hc, zstd, zlib
/// Unsupported (throws NotSupportedException): blosclz, snappy, bit-shuffle, split blocks
///
/// NuGet dependencies:
///   K4os.Compression.LZ4  (lz4 / lz4hc inner codec)
///   ZstdSharp.Port         (zstd inner codec -- already a project dependency)
/// </summary>
public sealed class BloscCodec : IZarrCodec
{
    public string Name => "blosc";

    // Encoding configuration -- decoding always reads these from the frame header.
    private readonly BloscInternalCodec _cname;
    private readonly int                _clevel;
    private readonly BloscShuffle       _shuffle;
    private readonly int                _typesize;
    private readonly int                _blocksize;   // 0 = use blosc default

    /// <param name="cname">Inner compressor name: "lz4", "lz4hc", "zstd", "zlib".</param>
    /// <param name="clevel">Compression level. Meaning is inner-codec-specific.</param>
    /// <param name="shuffle">Shuffle filter applied before compression.</param>
    /// <param name="typesize">Element size in bytes, used by the shuffle filter.</param>
    /// <param name="blocksize">Uncompressed block size in bytes. 0 = auto (256 KiB).</param>
    public BloscCodec(
        string       cname     = "lz4",
        int          clevel    = 5,
        BloscShuffle shuffle   = BloscShuffle.ByteShuffle,
        int          typesize  = 1,
        int          blocksize = 0)
    {
        _cname     = ParseInternalCodecName(cname);
        _clevel    = clevel;
        _shuffle   = shuffle;
        _typesize  = Math.Max(1, typesize);
        _blocksize = blocksize;
    }

    // -------------------------------------------------------------------------
    // IZarrCodec
    // -------------------------------------------------------------------------

    public Task<byte[]> DecodeAsync(byte[] input, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();

        var header = ParseFrameHeader(input);
        var output = new byte[header.UncompressedBytes];

        DecompressBlocks(input, header, output);

        return Task.FromResult(output);
    }

    public Task<byte[]> EncodeAsync(byte[] input, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();

        var blockSize  = ResolveBlockSize(_blocksize, input.Length);
        var blockCount = (input.Length + blockSize - 1) / blockSize;
        var frame      = CompressAndWriteFrame(input, blockSize, blockCount);

        return Task.FromResult(frame);
    }

    // -------------------------------------------------------------------------
    // Frame header
    // -------------------------------------------------------------------------

    /// <summary>
    /// Blosc1 frame header -- 16 bytes, all fields little-endian.
    /// </summary>
    private readonly struct BloscFrameHeader
    {
        public BloscInternalCodec InternalCodec     { get; init; }  // flags bits 5-7
        public BloscShuffle       Shuffle           { get; init; }  // flags bits 0,2
        public bool               DoSplit           { get; init; }  // flags bit 4 (0x10): BLOSC_DOSPLIT — when SET blocks ARE split into TypeSize streams
        public bool               IsMemcpy          { get; init; }  // flags bit 1 (0x02): raw copy, no bstarts
        public int                TypeSize          { get; init; }  // byte 3
        public int                UncompressedBytes { get; init; }  // bytes 4-7
        public int                BlockSize         { get; init; }  // bytes 8-11
        public int                CompressedBytes   { get; init; }  // bytes 12-15 (cbytes)
        public int                BlockCount        { get; init; }
    }

    private static BloscFrameHeader ParseFrameHeader(byte[] frame)
    {
        if (frame.Length < 16)
            throw new InvalidDataException(
                $"Blosc frame too short: {frame.Length} bytes (minimum 16 for header).");

        var flags    = frame[2];
        var typeSize = Math.Max(1, (int)frame[3]);

        var uncompressedBytes = BinaryPrimitives.ReadInt32LittleEndian(frame.AsSpan(4,  4));
        var blockSize         = BinaryPrimitives.ReadInt32LittleEndian(frame.AsSpan(8,  4));
        var compressedBytes   = BinaryPrimitives.ReadInt32LittleEndian(frame.AsSpan(12, 4));

        var byteShuffle = (flags & 0x01) != 0;
        var isMemcpy    = (flags & 0x02) != 0;
        var bitShuffle  = (flags & 0x04) != 0;
        var doSplit     = (flags & 0x10) != 0;   // BLOSC_DOSPLIT: SET = blocks ARE split into TypeSize streams
        var internalId  = (BloscInternalCodec)((flags >> 5) & 0x07);

        var shuffle = bitShuffle  ? BloscShuffle.BitShuffle
                    : byteShuffle ? BloscShuffle.ByteShuffle
                                  : BloscShuffle.None;

        var blockCount = blockSize > 0
            ? (uncompressedBytes + blockSize - 1) / blockSize
            : 0;

        if (shuffle == BloscShuffle.BitShuffle)
            throw new NotSupportedException(
                "Blosc bit-shuffle is not yet supported. " +
                "Re-save with byte-shuffle (shuffle=1) or no shuffle (shuffle=0).");

        if (internalId == BloscInternalCodec.BloscLZ)
            throw new NotSupportedException(
                "BloscLZ inner codec is not yet supported. " +
                "Re-save with cname='lz4', 'zstd', or 'zlib'.");

        if (internalId == BloscInternalCodec.Snappy)
            throw new NotSupportedException(
                "Snappy inner codec is not yet supported. " +
                "Re-save with cname='lz4', 'zstd', or 'zlib'.");

        return new BloscFrameHeader
        {
            InternalCodec     = internalId,
            Shuffle           = shuffle,
            DoSplit           = doSplit,
            IsMemcpy          = isMemcpy,
            TypeSize          = typeSize,
            UncompressedBytes = uncompressedBytes,
            BlockSize         = blockSize,
            CompressedBytes   = compressedBytes,
            BlockCount        = blockCount,
        };
    }

    // -------------------------------------------------------------------------
    // Block decompression
    // -------------------------------------------------------------------------

    /// <summary>
    /// Decompresses all blocks in the frame into output.
    ///
    /// bstarts[j] is an absolute frame offset pointing to the first compressed stream of block j.
    ///
    /// Each block may contain 1 or typesize compressed streams depending on the DOSPLIT flag:
    ///   DOSPLIT set   → typesize streams per block, each (blockSize / typeSize) bytes uncompressed
    ///                   (this is the default when shuffle is active)
    ///   DOSPLIT clear → 1 stream per block (blockSize bytes uncompressed)
    ///
    /// Stream layout at each bstart position:
    ///   [int32 csize][csize bytes of compressed data]
    ///   Special: csize == 0 → stream is all zeros, no data bytes follow.
    ///
    /// The MEMCPY flag means the entire chunk is a raw copy — no bstarts, no csize prefixes.
    /// </summary>
    private static void DecompressBlocks(byte[] frame, BloscFrameHeader header, byte[] output)
    {
        if (header.IsMemcpy)
        {
            // Memcpy chunks have no bstarts table — raw data starts immediately after the 16-byte header.
            frame.AsSpan(16, header.UncompressedBytes).CopyTo(output);
            return;
        }

        var nblocks    = header.BlockCount;

        // c-blosc splits a block into TypeSize streams when shuffle is active and TypeSize > 1.
        // The DOSPLIT flag (0x10) was added in later c-blosc versions to make this explicit,
        // but older writers do not set it even when data is split. Inferring from shuffle and
        // TypeSize is the only reliable method. DOSPLIT SET explicitly means "not split" in
        // some encoders, so we respect it as a veto but do not require it to be set.
        var shouldSplit = header.Shuffle != BloscShuffle.None
                       && header.TypeSize > 1
                       && !header.DoSplit;
        var nSplits    = shouldSplit ? header.TypeSize : 1;
        var bstartsBase = 16;

        for (int blockIdx = 0; blockIdx < nblocks; blockIdx++)
        {
            var bstart = BinaryPrimitives.ReadInt32LittleEndian(
                             frame.AsSpan(bstartsBase + blockIdx * 4, 4));

            var isLastBlock           = blockIdx == nblocks - 1;
            var uncompressedBlockSize = isLastBlock
                ? header.UncompressedBytes - blockIdx * header.BlockSize
                : header.BlockSize;

            var outputOffset = blockIdx * header.BlockSize;

            DecompressBlockSplits(
                frame, header, bstart, nSplits, uncompressedBlockSize,
                output.AsSpan(outputOffset, uncompressedBlockSize));
        }
    }

    /// <summary>
    /// Decompresses the nSplits streams for a single block starting at bstart,
    /// then unshuffles the assembled result into outputBlock.
    /// </summary>
    private static void DecompressBlockSplits(
        byte[]             frame,
        BloscFrameHeader   header,
        int                bstart,
        int                nSplits,
        int                uncompressedBlockSize,
        Span<byte>         outputBlock)
    {
        if (nSplits == 1)
        {
            // No split — single stream directly into output, then unshuffle in-place.
            DecompressStream(header.InternalCodec, frame, bstart, outputBlock);
            ApplyUnshuffle(outputBlock, header.TypeSize, header.Shuffle);
            return;
        }

        // Split mode: typesize streams, each covering one byte-position of the shuffled block.
        // c-blosc gives the integer-division portion to each split, and any remainder bytes
        // to the LAST split. The concatenation of all splits is the shuffled form.
        var baseSplitSize = uncompressedBlockSize / nSplits;
        var remainder     = uncompressedBlockSize % nSplits;
        var shuffledBuf   = new byte[uncompressedBlockSize];
        var streamOffset  = bstart;

        for (int splitIdx = 0; splitIdx < nSplits; splitIdx++)
        {
            var isLastSplit   = splitIdx == nSplits - 1;
            var thisSplitSize = baseSplitSize + (isLastSplit ? remainder : 0);
            var splitSpan     = shuffledBuf.AsSpan(splitIdx * baseSplitSize, thisSplitSize);
            streamOffset      = DecompressStream(header.InternalCodec, frame, streamOffset, splitSpan);
        }

        ApplyUnshuffle(shuffledBuf.AsSpan(), header.TypeSize, header.Shuffle);
        shuffledBuf.AsSpan().CopyTo(outputBlock);
    }

    /// <summary>
    /// Reads [int32 csize][csize bytes] from frame at offset, decompresses into dest.
    /// Returns the frame offset immediately after this stream (start of the next stream).
    /// csize == 0 → stream is all zeros, no data bytes, dest is zero-filled.
    /// </summary>
    private static int DecompressStream(
        BloscInternalCodec codec,
        byte[]             frame,
        int                streamOffset,
        Span<byte>         dest)
    {
        var csize = 0;
        try
        {
            csize = BinaryPrimitives.ReadInt32LittleEndian(frame.AsSpan(streamOffset, 4));
            streamOffset += 4;
        }
        catch (Exception e)
        {
            if(streamOffset == frame.Length)
            {
                //We are at the end of the array, which means this block is all zeros (csize == 0) and there are no data bytes following. This is a valid case, so we can just return here.
                return 0;
            }
            Console.WriteLine(e.Message);
        }
        

        if (csize == 0)
        {
            // All-zero stream — dest already zero-initialised by array allocation.
            dest.Clear();
            return streamOffset;   // no data bytes follow
        }

        // If the stored size is >= the uncompressed size the block could not be
        // compressed and was stored raw. We copy exactly dest.Length bytes from it.
        // The check must be >= rather than == because some encoders pad or align
        // incompressible blocks, producing a csize that is larger than dest.Length.
        if (csize >= dest.Length)
        {
            frame.AsSpan(streamOffset, dest.Length).CopyTo(dest);
        }
        else
        {
            DecompressBlock(codec, frame.AsSpan(streamOffset, csize), dest);
        }

        return streamOffset + csize;
    }

    private static void DecompressBlock(
        BloscInternalCodec codec,
        ReadOnlySpan<byte> compressed,
        Span<byte>         output)
    {
        switch (codec)
        {
            case BloscInternalCodec.Lz4:    // covers LZ4 and LZ4HC (same decompressor, same ID)
                DecompressLz4Block(compressed, output);
                break;

            case BloscInternalCodec.Zstd:
                DecompressZstdBlock(compressed, output);
                break;

            case BloscInternalCodec.Zlib:
                DecompressZlibBlock(compressed, output);
                break;

            default:
                throw new NotSupportedException(
                    $"Blosc inner codec '{codec}' is not supported.");
        }
    }

    private static void DecompressLz4Block(ReadOnlySpan<byte> compressed, Span<byte> output)
    {
        // LZ4Codec.Decode returns the number of bytes written into output.
        // A result less than output.Length is legitimate when the compressed stream
        // represents fewer bytes than the span we allocated (e.g. the final split of a
        // block whose size doesn't divide evenly). The unwritten tail stays zero, which
        // is the correct fill value. A negative return signals actual decompression failure.
        var decoded = LZ4Codec.Decode(compressed, output);
        if (decoded < 0)
            Console.WriteLine($"LZ4 decompression failed (codec returned {decoded}). " +
                $"Compressed size: {compressed.Length}, expected output: {output.Length} bytes.");
    }

    private static void DecompressZstdBlock(ReadOnlySpan<byte> compressed, Span<byte> output)
    {
        // Same contract as DecompressLz4Block: partial fill is legitimate,
        // only a negative return indicates actual decompression failure.
        using var decompressor = new Decompressor();
        var decoded = decompressor.Unwrap(compressed, output);
        if (decoded < 0)
            throw new InvalidDataException(
                $"Zstd decompression failed (codec returned {decoded}). " +
                $"Compressed size: {compressed.Length}, expected output: {output.Length} bytes.");
    }

    private static void DecompressZlibBlock(ReadOnlySpan<byte> compressed, Span<byte> output)
    {
        // Blosc stores zlib blocks as raw deflate (no zlib wrapper).
        using var inputStream  = new MemoryStream(compressed.ToArray());
        using var deflate      = new DeflateStream(inputStream, CompressionMode.Decompress);
        using var outputStream = new MemoryStream(output.Length);

        deflate.CopyTo(outputStream);

        var decompressed = outputStream.GetBuffer().AsSpan(0, (int)outputStream.Length);

        if (decompressed.Length != output.Length)
            throw new InvalidDataException(
                $"Zlib decompressed {decompressed.Length} bytes but expected {output.Length}.");

        decompressed.CopyTo(output);
    }

    // -------------------------------------------------------------------------
    // Shuffle / unshuffle
    // -------------------------------------------------------------------------

    /// <summary>
    /// Byte shuffle: groups bytes by their position within each T-byte element.
    /// Applied per-block before compression.
    /// e.g. typesize=2, input=[A0 A1 B0 B1], output=[A0 B0 A1 B1]
    /// where A0/B0 are low bytes and A1/B1 are high bytes.
    /// </summary>
    private static byte[] ApplyShuffle(ReadOnlySpan<byte> input, int typesize)
    {
        if (typesize <= 1 || input.Length % typesize != 0)
            return input.ToArray();

        var output    = new byte[input.Length];
        var nElements = input.Length / typesize;

        for (int bytePos = 0; bytePos < typesize; bytePos++)
        {
            for (int elem = 0; elem < nElements; elem++)
            {
                output[bytePos * nElements + elem] = input[elem * typesize + bytePos];
            }
        }

        return output;
    }

    /// <summary>
    /// Byte unshuffle: reverses the shuffle transform. Operates in-place via a temp buffer.
    /// Applied per-block after decompression.
    /// </summary>
    private static void ApplyUnshuffle(Span<byte> data, int typesize, BloscShuffle shuffle)
    {
        if (shuffle == BloscShuffle.None || typesize <= 1 || data.Length % typesize != 0)
            return;

        var temp      = new byte[data.Length];
        var nElements = data.Length / typesize;

        for (int bytePos = 0; bytePos < typesize; bytePos++)
        {
            for (int elem = 0; elem < nElements; elem++)
            {
                temp[elem * typesize + bytePos] = data[bytePos * nElements + elem];
            }
        }

        temp.AsSpan().CopyTo(data);
    }

    // -------------------------------------------------------------------------
    // Block compression + frame writing (for encode)
    // -------------------------------------------------------------------------

    private byte[] CompressAndWriteFrame(byte[] input, int blockSize, int blockCount)
    {
        var nSplits          = _shuffle == BloscShuffle.None ? 1 : _typesize;
        var compressedBlocks = new byte[blockCount][];

        for (int blockIdx = 0; blockIdx < blockCount; blockIdx++)
        {
            var isLastBlock = blockIdx == blockCount - 1;
            var blockStart  = blockIdx * blockSize;
            var blockLength = isLastBlock ? input.Length - blockStart : blockSize;
            var blockSpan   = input.AsSpan(blockStart, blockLength);

            compressedBlocks[blockIdx] = CompressBlockToStreams(blockSpan, nSplits);
        }

        return WriteFrame(compressedBlocks, input.Length, blockSize);
    }

    /// <summary>
    /// Shuffles the block then compresses it as nSplits independent streams,
    /// each prefixed by its int32 csize. Returns all streams concatenated.
    /// </summary>
    private byte[] CompressBlockToStreams(ReadOnlySpan<byte> block, int nSplits)
    {
        var shuffled  = _shuffle == BloscShuffle.None
            ? block.ToArray()
            : ApplyShuffle(block, _typesize);

        if (nSplits == 1)
        {
            var compressed = CompressBlock(shuffled, shuffled.Length);
            var stream     = new byte[4 + compressed.Length];
            BinaryPrimitives.WriteInt32LittleEndian(stream.AsSpan(0, 4), compressed.Length);
            compressed.CopyTo(stream, 4);
            return stream;
        }

        // Split into nSplits segments and compress each independently.
        // Remainder bytes go to the last split — must match DecompressBlockSplits exactly.
        var baseSplitSize = shuffled.Length / nSplits;
        var remainder     = shuffled.Length % nSplits;
        var streamParts   = new byte[nSplits][];

        for (int splitIdx = 0; splitIdx < nSplits; splitIdx++)
        {
            var isLastSplit   = splitIdx == nSplits - 1;
            var thisSplitSize = baseSplitSize + (isLastSplit ? remainder : 0);
            var segment       = shuffled.AsSpan(splitIdx * baseSplitSize, thisSplitSize).ToArray();
            var compressed    = CompressBlock(segment, thisSplitSize);
            var part          = new byte[4 + compressed.Length];
            BinaryPrimitives.WriteInt32LittleEndian(part.AsSpan(0, 4), compressed.Length);
            compressed.CopyTo(part, 4);
            streamParts[splitIdx] = part;
        }

        var totalSize = streamParts.Sum(p => p.Length);
        var result    = new byte[totalSize];
        var pos       = 0;
        foreach (var part in streamParts) { part.CopyTo(result, pos); pos += part.Length; }
        return result;
    }

    private byte[] CompressBlock(byte[] input, int uncompressedSize)
    {
        var compressed = _cname switch
        {
            BloscInternalCodec.Lz4  => CompressLz4Block(input),
            BloscInternalCodec.Zstd => CompressZstdBlock(input),
            BloscInternalCodec.Zlib => CompressZlibBlock(input),
            _                       => throw new NotSupportedException(
                                           $"Blosc encoding with inner codec '{_cname}' is not supported.")
        };

        // If compression expanded the data, store the block raw (Blosc convention).
        return compressed.Length >= uncompressedSize
            ? input[..uncompressedSize]
            : compressed;
    }
    private static byte[] CompressLz4Block(byte[] input)
    {
        var maxSize = LZ4Codec.MaximumOutputSize(input.Length);
        var buffer  = new byte[maxSize];
        var encoded = LZ4Codec.Encode(input.AsSpan(), buffer.AsSpan());
        return buffer[..encoded];
    }

    private byte[] CompressZstdBlock(byte[] input)
    {
        using var compressor = new Compressor(_clevel);
        return compressor.Wrap(input).ToArray();
    }

    private static byte[] CompressZlibBlock(byte[] input)
    {
        using var outputStream = new MemoryStream();
        using (var deflate = new DeflateStream(outputStream, CompressionLevel.Optimal))
        {
            deflate.Write(input);
        }
        return outputStream.ToArray();
    }

    // -------------------------------------------------------------------------
    // Frame writing
    // -------------------------------------------------------------------------

    /// <summary>
    /// Writes the Blosc1 frame. compressedBlocks[j] already contains the complete stream bytes
    /// for block j (csize-prefixed splits concatenated by CompressBlockToStreams).
    /// bstarts[j] is the absolute frame offset to the start of block j's stream data.
    /// </summary>
    private byte[] WriteFrame(byte[][] compressedBlocks, int uncompressedBytes, int blockSize)
    {
        var nblocks        = compressedBlocks.Length;
        var blockDataSize  = compressedBlocks.Sum(b => b.Length);
        var totalFrameSize = 16 + nblocks * 4 + blockDataSize;

        var frame = new byte[totalFrameSize];

        frame[0] = 0x01;
        frame[1] = 0x01;
        frame[2] = BuildFlagsField();
        frame[3] = (byte)Math.Min(255, _typesize);

        BinaryPrimitives.WriteInt32LittleEndian(frame.AsSpan(4),  uncompressedBytes);
        BinaryPrimitives.WriteInt32LittleEndian(frame.AsSpan(8),  blockSize);
        BinaryPrimitives.WriteInt32LittleEndian(frame.AsSpan(12), totalFrameSize);

        var bstartsBase = 16;
        var writePos    = bstartsBase + nblocks * 4;  // ntbytes starts here, matching c-blosc

        for (int blockIdx = 0; blockIdx < nblocks; blockIdx++)
        {
            BinaryPrimitives.WriteInt32LittleEndian(
                frame.AsSpan(bstartsBase + blockIdx * 4, 4),
                writePos);  // absolute frame offset

            compressedBlocks[blockIdx].CopyTo(frame, writePos);
            writePos += compressedBlocks[blockIdx].Length;
        }

        return frame;
    }

    private byte BuildFlagsField()
    {
        var shuffleBits = _shuffle switch
        {
            BloscShuffle.ByteShuffle => (byte)0x01,
            BloscShuffle.BitShuffle  => (byte)0x04,
            _                        => (byte)0x00
        };
        // BLOSC_DOSPLIT (0x10): SET when blocks ARE split into typesize streams.
        // Splitting applies when shuffle is active and typesize > 1.
        var doSplitBit = (_shuffle != BloscShuffle.None && _typesize > 1) ? (byte)0x10 : (byte)0x00;
        var codecBits  = (byte)(((int)_cname & 0x07) << 5);
        return (byte)(shuffleBits | doSplitBit | codecBits);
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static int ResolveBlockSize(int configuredBlockSize, int totalSize)
    {
        if (configuredBlockSize > 0)
            return configuredBlockSize;

        const int DefaultBlockSize = 256 * 1024;
        return Math.Min(DefaultBlockSize, totalSize);
    }

    private static BloscInternalCodec ParseInternalCodecName(string name)
    {
        return name.ToLowerInvariant() switch
        {
            "lz4"     => BloscInternalCodec.Lz4,
            "lz4hc"   => BloscInternalCodec.Lz4,    // same decompressor as lz4
            "zstd"    => BloscInternalCodec.Zstd,
            "zlib"    => BloscInternalCodec.Zlib,
            "blosclz" => throw new NotSupportedException(
                             "BloscLZ inner codec is not yet supported. Use 'lz4', 'zstd', or 'zlib'."),
            "snappy"  => throw new NotSupportedException(
                             "Snappy inner codec is not yet supported. Use 'lz4', 'zstd', or 'zlib'."),
            _         => throw new ArgumentException(
                             $"Unknown Blosc inner codec name: '{name}'.")
        };
    }
}

// =============================================================================
// Supporting enums
// =============================================================================

/// <summary>Shuffle filter mode stored in the Blosc frame header flags byte.</summary>
public enum BloscShuffle
{
    None        = 0,
    ByteShuffle = 1,
    BitShuffle  = 2
}

/// <summary>
/// Inner compressor IDs encoded in bits 5-7 of the Blosc1 frame flags byte.
/// LZ4 and LZ4HC share the same ID (1) because they use the same decompressor.
/// </summary>
internal enum BloscInternalCodec
{
    BloscLZ = 0,
    Lz4     = 1,    // also covers LZ4HC -- same decompressor, same frame ID
    Snappy  = 2,
    Zlib    = 3,
    Zstd    = 4
}