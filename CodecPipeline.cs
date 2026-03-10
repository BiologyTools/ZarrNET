namespace ZarrNET;

/// <summary>
/// Applies an ordered list of codecs as a pipeline.
///
/// Zarr v3 codec pipeline:
///   Decode: last codec → ... → first codec   (compressed bytes → array bytes)
///   Encode: first codec → ... → last codec   (array bytes → compressed bytes)
///
/// The pipeline also resolves byte-order swapping for the BytesCodec based on
/// the data type element size from ZarrArrayMetadata.
///
/// When all codecs in the pipeline are synchronous (returning completed Tasks),
/// the decode/encode path avoids async state machine overhead entirely. This is
/// the common case for Blosc, Zstd, and BytesCodec pipelines.
/// </summary>
public sealed class CodecPipeline
{
    private readonly IReadOnlyList<IZarrCodec> _codecs;
    private readonly int _elementSize;
    private readonly bool _allCodecsSync;

    public CodecPipeline(IReadOnlyList<IZarrCodec> codecs, int elementSize)
    {
        _codecs      = codecs;
        _elementSize = elementSize;

        // Detect whether every codec in the pipeline is synchronous.
        // Synchronous codecs return Task.FromResult — we identify them by
        // interface marker or by known type. GzipCodec is the only async
        // codec in the codebase (uses Stream.CopyToAsync).
        _allCodecsSync = codecs.All(c => c is not GzipCodec);
    }

    // -------------------------------------------------------------------------
    // Pipeline execution
    // -------------------------------------------------------------------------

    public Task<byte[]> DecodeAsync(byte[] input, CancellationToken ct = default)
    {
        // Fast path: when all codecs are synchronous, avoid async state machine
        if (_allCodecsSync)
            return Task.FromResult(DecodeSynchronous(input, ct));

        return DecodeAsyncCore(input, ct);
    }

    public Task<byte[]> EncodeAsync(byte[] input, CancellationToken ct = default)
    {
        if (_allCodecsSync)
            return Task.FromResult(EncodeSynchronous(input, ct));

        return EncodeAsyncCore(input, ct);
    }

    // -------------------------------------------------------------------------
    // Synchronous fast path
    // -------------------------------------------------------------------------

    private byte[] DecodeSynchronous(byte[] input, CancellationToken ct)
    {
        var data = input;

        for (int i = _codecs.Count - 1; i >= 0; i--)
        {
            data = ApplyDecodeStepSync(_codecs[i], data, ct);
        }

        return data;
    }

    private byte[] EncodeSynchronous(byte[] input, CancellationToken ct)
    {
        var data = input;

        foreach (var codec in _codecs)
        {
            data = ApplyEncodeStepSync(codec, data, ct);
        }

        return data;
    }

    // -------------------------------------------------------------------------
    // Async fallback path (used when pipeline contains GzipCodec)
    // -------------------------------------------------------------------------

    private async Task<byte[]> DecodeAsyncCore(byte[] input, CancellationToken ct)
    {
        var data = input;

        for (int i = _codecs.Count - 1; i >= 0; i--)
        {
            data = await ApplyDecodeStepAsync(_codecs[i], data, ct).ConfigureAwait(false);
        }

        return data;
    }

    private async Task<byte[]> EncodeAsyncCore(byte[] input, CancellationToken ct)
    {
        var data = input;

        foreach (var codec in _codecs)
        {
            data = await ApplyEncodeStepAsync(codec, data, ct).ConfigureAwait(false);
        }

        return data;
    }

    // -------------------------------------------------------------------------
    // Step helpers — sync
    // -------------------------------------------------------------------------

    private byte[] ApplyDecodeStepSync(IZarrCodec codec, byte[] data, CancellationToken ct)
    {
        if (codec is BytesCodec bytesCodec)
            return bytesCodec.DecodeWithElementSizeAsync(data, _elementSize, ct).GetAwaiter().GetResult();

        // All non-Gzip codecs return completed tasks, so .Result is safe
        return codec.DecodeAsync(data, ct).GetAwaiter().GetResult();
    }

    private byte[] ApplyEncodeStepSync(IZarrCodec codec, byte[] data, CancellationToken ct)
    {
        if (codec is BytesCodec bytesCodec)
            return bytesCodec.EncodeWithElementSizeAsync(data, _elementSize, ct).GetAwaiter().GetResult();

        return codec.EncodeAsync(data, ct).GetAwaiter().GetResult();
    }

    // -------------------------------------------------------------------------
    // Step helpers — async
    // -------------------------------------------------------------------------

    private Task<byte[]> ApplyDecodeStepAsync(IZarrCodec codec, byte[] data, CancellationToken ct)
    {
        // BytesCodec needs element size to handle endianness correctly
        if (codec is BytesCodec bytesCodec)
            return bytesCodec.DecodeWithElementSizeAsync(data, _elementSize, ct);
        return codec.DecodeAsync(data, ct);
    }

    private Task<byte[]> ApplyEncodeStepAsync(IZarrCodec codec, byte[] data, CancellationToken ct)
    {
        if (codec is BytesCodec bytesCodec)
            return bytesCodec.EncodeWithElementSizeAsync(data, _elementSize, ct);

        return codec.EncodeAsync(data, ct);
    }
}
