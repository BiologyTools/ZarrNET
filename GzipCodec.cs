using System.IO.Compression;

namespace ZarrNET;

/// <summary>
/// Gzip bytes-to-bytes codec. Wraps System.IO.Compression — no native dependencies.
/// Configuration mirrors the zarr.json "gzip" codec configuration object.
/// </summary>
public sealed class GzipCodec : IZarrCodec
{
    public string Name => "gzip";

    private readonly CompressionLevel _compressionLevel;

    public GzipCodec(int level = 6)
    {
        _compressionLevel = level switch
        {
            0         => CompressionLevel.NoCompression,
            1         => CompressionLevel.Fastest,
            >= 7      => CompressionLevel.SmallestSize,
            _         => CompressionLevel.Optimal
        };
    }

    // -------------------------------------------------------------------------
    // IZarrCodec
    // -------------------------------------------------------------------------

    public async Task<byte[]> DecodeAsync(byte[] input, CancellationToken ct = default)
    {
        await using var inputStream  = new MemoryStream(input);
        await using var gzipStream   = new GZipStream(inputStream, CompressionMode.Decompress);
        await using var outputStream = new MemoryStream();

        await gzipStream.CopyToAsync(outputStream, ct).ConfigureAwait(false);

        return outputStream.ToArray();
    }

    public async Task<byte[]> EncodeAsync(byte[] input, CancellationToken ct = default)
    {
        await using var outputStream = new MemoryStream();

        await using (var gzipStream = new GZipStream(outputStream, _compressionLevel, leaveOpen: true))
        {
            await gzipStream.WriteAsync(input, ct).ConfigureAwait(false);
        }

        return outputStream.ToArray();
    }
}
