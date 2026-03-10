namespace ZarrNET;

/// <summary>
/// A single step in a Zarr v3 codec pipeline. Codecs are applied in order
/// during encoding (array → bytes → compressed) and reversed during decoding.
/// Each codec is responsible for one transformation only.
/// </summary>
public interface IZarrCodec
{
    /// <summary>Codec name as it appears in zarr.json (e.g. "gzip", "zstd", "bytes").</summary>
    string Name { get; }

    /// <summary>Decodes bytes produced by the previous codec step.</summary>
    Task<byte[]> DecodeAsync(byte[] input, CancellationToken ct = default);

    /// <summary>Encodes bytes for the next codec step.</summary>
    Task<byte[]> EncodeAsync(byte[] input, CancellationToken ct = default);
}
