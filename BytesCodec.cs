namespace ZarrNET;

/// <summary>
/// The "bytes" array-to-bytes codec (Zarr v3 spec section 3.3.2).
/// Handles endianness conversion when translating between the in-memory
/// array representation and a flat byte sequence.
/// This is always the first codec in a Zarr v3 pipeline.
/// </summary>
public sealed class BytesCodec : IZarrCodec
{
    public string Name => "bytes";

    private readonly ByteOrder _byteOrder;

    public BytesCodec(ByteOrder byteOrder = ByteOrder.LittleEndian)
    {
        _byteOrder = byteOrder;
    }

    // -------------------------------------------------------------------------
    // IZarrCodec
    // -------------------------------------------------------------------------

    public Task<byte[]> DecodeAsync(byte[] input, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();

        var result = NeedsSwap()
            ? SwapByteOrder(input)
            : input;

        return Task.FromResult(result);
    }

    public Task<byte[]> EncodeAsync(byte[] input, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();

        // Swap is its own inverse
        var result = NeedsSwap()
            ? SwapByteOrder(input)
            : input;

        return Task.FromResult(result);
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private bool NeedsSwap()
        => _byteOrder == ByteOrder.BigEndian && BitConverter.IsLittleEndian
        || _byteOrder == ByteOrder.LittleEndian && !BitConverter.IsLittleEndian;

    /// <summary>
    /// Reverses byte order for each element. We rely on the element size being
    /// provided by the zarr data type — but BytesCodec at this layer operates on
    /// raw bytes, so we receive already-flat bytes and treat them as pairs/quads/etc.
    /// The element size is resolved by CodecPipeline before invoking this codec.
    /// </summary>
    private static byte[] SwapByteOrder(byte[] input)
    {
        // Default: swap every 2 bytes (uint16). CodecPipeline sets the real element size
        // via the overload below before the pipeline runs.
        return SwapByteOrder(input, elementSize: 2);
    }

    internal Task<byte[]> DecodeWithElementSizeAsync(byte[] input, int elementSize, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();

        var result = NeedsSwap()
            ? SwapByteOrder(input, elementSize)
            : input;

        return Task.FromResult(result);
    }

    internal Task<byte[]> EncodeWithElementSizeAsync(byte[] input, int elementSize, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();

        var result = NeedsSwap()
            ? SwapByteOrder(input, elementSize)
            : input;

        return Task.FromResult(result);
    }

    internal static byte[] SwapByteOrder(byte[] input, int elementSize)
    {
        if (elementSize <= 1)
            return input;

        var output = new byte[input.Length];

        for (int i = 0; i < input.Length; i += elementSize)
        {
            for (int j = 0; j < elementSize; j++)
                output[i + j] = input[i + elementSize - 1 - j];
        }

        return output;
    }
}

public enum ByteOrder
{
    LittleEndian,
    BigEndian
}
