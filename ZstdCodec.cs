using ZstdSharp;

namespace ZarrNET;

/// <summary>
/// Zstandard bytes-to-bytes codec. Uses ZstdSharp (managed wrapper around
/// the native zstd library, available as a NuGet package).
/// NuGet: ZstdSharp.Port (pure managed port, no native deps)
/// </summary>
public sealed class ZstdCodec : IZarrCodec
{
    public string Name => "zstd";

    private readonly int _level;

    /// <param name="level">Compression level 1–22. Default 3 matches zstd's default.</param>
    public ZstdCodec(int level = 3)
    {
        _level = Math.Clamp(level, 1, 22);
    }

    // -------------------------------------------------------------------------
    // IZarrCodec
    // -------------------------------------------------------------------------

    public Task<byte[]> DecodeAsync(byte[] input, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();

        using var decompressor = new Decompressor();
        var result = decompressor.Unwrap(input);

        return Task.FromResult(result.ToArray());
    }

    public Task<byte[]> EncodeAsync(byte[] input, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();

        using var compressor = new Compressor(_level);
        var result = compressor.Wrap(input);

        return Task.FromResult(result.ToArray());
    }
}
