using System.Text.Json;
using System.Text.Json.Serialization;

namespace ZarrNET;

/// <summary>
/// Raw deserialisation of a zarr.json node document (Zarr v3 spec).
/// This is the only place we touch raw JSON — all higher layers work
/// with typed metadata objects derived from this.
/// </summary>
public sealed class ZarrJsonDocument
{
    [JsonPropertyName("zarr_format")]
    public int ZarrFormat { get; init; }

    [JsonPropertyName("node_type")]
    public string NodeType { get; init; } = string.Empty;

    /// <summary>
    /// Present on array nodes. Null on group nodes.
    /// </summary>
    [JsonPropertyName("shape")]
    public long[]? Shape { get; init; }

    [JsonPropertyName("data_type")]
    public string? DataType { get; init; }

    [JsonPropertyName("chunk_grid")]
    public ChunkGridDocument? ChunkGrid { get; init; }

    [JsonPropertyName("chunk_key_encoding")]
    public ChunkKeyEncodingDocument? ChunkKeyEncoding { get; init; }

    [JsonPropertyName("fill_value")]
    public JsonElement? FillValue { get; init; }

    [JsonPropertyName("codecs")]
    public CodecDocument[]? Codecs { get; init; }

    [JsonPropertyName("dimension_names")]
    public string[]? DimensionNames { get; init; }

    /// <summary>
    /// Freeform attributes — this is where OME-Zarr metadata lives.
    /// Kept as a raw JsonElement so the OME layer can parse it independently.
    /// </summary>
    [JsonPropertyName("attributes")]
    public JsonElement? Attributes { get; init; }

    // -------------------------------------------------------------------------
    // Parsing
    // -------------------------------------------------------------------------

    private static readonly JsonSerializerOptions _jsonOptions = new()
    {
        PropertyNameCaseInsensitive = true,
        ReadCommentHandling         = JsonCommentHandling.Skip,
        AllowTrailingCommas         = true
    };

    public static ZarrJsonDocument Parse(string json)
        => JsonSerializer.Deserialize<ZarrJsonDocument>(json, _jsonOptions)
           ?? throw new InvalidOperationException("Failed to deserialize zarr.json: null result.");

    public static ZarrJsonDocument Parse(byte[] utf8Json)
        => JsonSerializer.Deserialize<ZarrJsonDocument>(utf8Json, _jsonOptions)
           ?? throw new InvalidOperationException("Failed to deserialize zarr.json: null result.");
}

// -------------------------------------------------------------------------
// Supporting document types
// -------------------------------------------------------------------------

public sealed class ChunkGridDocument
{
    [JsonPropertyName("name")]
    public string Name { get; init; } = string.Empty;

    [JsonPropertyName("configuration")]
    public ChunkGridConfigurationDocument? Configuration { get; init; }
}

public sealed class ChunkGridConfigurationDocument
{
    [JsonPropertyName("chunk_shape")]
    public int[]? ChunkShape { get; init; }
}

public sealed class ChunkKeyEncodingDocument
{
    [JsonPropertyName("name")]
    public string Name { get; init; } = string.Empty;

    [JsonPropertyName("configuration")]
    public ChunkKeyEncodingConfigurationDocument? Configuration { get; init; }
}

public sealed class ChunkKeyEncodingConfigurationDocument
{
    [JsonPropertyName("separator")]
    public string Separator { get; init; } = "/";
}

public sealed class CodecDocument
{
    [JsonPropertyName("name")]
    public string Name { get; init; } = string.Empty;

    [JsonPropertyName("configuration")]
    public JsonElement? Configuration { get; init; }
}
