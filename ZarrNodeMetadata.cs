using System.Text.Json;
using ZarrNET;

namespace ZarrNET;

/// <summary>
/// Typed metadata for a Zarr array node.
/// Knows everything needed to read/write chunks — shape, data type,
/// chunk layout, codec pipeline configuration.
/// No OME-Zarr knowledge here.
/// </summary>
public sealed class ZarrArrayMetadata
{
    public long[]        Shape             { get; }
    public int[]         ChunkShape        { get; }
    public ZarrDataType  DataType          { get; }
    public string        ChunkKeySeparator { get; }
    public CodecInfo[]   Codecs            { get; }
    public string[]?     DimensionNames    { get; }
    public JsonElement?  RawAttributes     { get; }
    public int           ZarrVersion       { get; }   // 2 or 3 — drives chunk key format

    /// <summary>
    /// Non-null when the codec pipeline contains a sharding_indexed codec.
    /// In this case, the outer ChunkShape represents the shard shape and
    /// actual data chunks are the inner chunks within each shard.
    /// </summary>
    public ShardingConfig? Sharding { get; internal set; }

    public int Rank => Shape.Length;

    private ZarrArrayMetadata(
        long[]       shape,
        int[]        chunkShape,
        ZarrDataType dataType,
        string       chunkKeySeparator,
        CodecInfo[]  codecs,
        string[]?    dimensionNames,
        JsonElement? rawAttributes,
        int          zarrVersion)
    {
        Shape             = shape;
        ChunkShape        = chunkShape;
        DataType          = dataType;
        ChunkKeySeparator = chunkKeySeparator;
        Codecs            = codecs;
        DimensionNames    = dimensionNames;
        RawAttributes     = rawAttributes;
        ZarrVersion       = zarrVersion;
    }

    // -------------------------------------------------------------------------
    // Factory — Zarr v3
    // -------------------------------------------------------------------------

    public static ZarrArrayMetadata FromDocument(ZarrJsonDocument doc)
    {
        if (doc.NodeType != "array")
            throw new InvalidOperationException(
                $"Expected node_type 'array', got '{doc.NodeType}'.");

        if (doc.Shape is null)
            throw new InvalidOperationException("Array zarr.json is missing 'shape'.");

        if (doc.DataType is null)
            throw new InvalidOperationException("Array zarr.json is missing 'data_type'.");

        var shape      = doc.Shape;
        var dataType   = ZarrDataType.Parse(doc.DataType);
        var chunkShape = ResolveChunkShape(doc);
        var separator  = doc.ChunkKeyEncoding?.Configuration?.Separator ?? "/";
        var codecs     = ResolveCodecs(doc);

        return new ZarrArrayMetadata(
            shape,
            chunkShape,
            dataType,
            separator,
            codecs,
            doc.DimensionNames,
            doc.Attributes,
            zarrVersion: 3);
    }

    // -------------------------------------------------------------------------
    // Factory — Zarr v2
    // -------------------------------------------------------------------------

    /// <summary>
    /// Creates array metadata from a Zarr v2 .zarray document.
    /// </summary>
    /// <param name="arrayDoc">Parsed .zarray contents.</param>
    /// <param name="attributes">Parsed .zattrs contents (optional).</param>
    /// <param name="separatorOverride">
    /// If provided, overrides the dimension separator regardless of what .zarray declares.
    /// Used by the chunk-key probing logic when .zarray omits dimension_separator
    /// and the store is found to use "/" (nested) chunk keys.
    /// </param>
    public static ZarrArrayMetadata FromV2Document(
        ZarrV2ArrayDocument   arrayDoc,
        JsonElement?          attributes,
        string?               separatorOverride = null)
    {
        if (arrayDoc.ZarrFormat != 2)
            throw new InvalidOperationException(
                $"Expected zarr_format 2, got {arrayDoc.ZarrFormat}.");

        var (dataType, byteOrder) = NumpyDtypeParser.Parse(arrayDoc.Dtype);

        var separator = separatorOverride
                     ?? arrayDoc.DimensionSeparator
                     ?? ".";

        var codecs = CodecFactory.BuildV2CodecPipeline(arrayDoc.Compressor, byteOrder);

        return new ZarrArrayMetadata(
            arrayDoc.Shape,
            arrayDoc.Chunks,
            dataType,
            separator,
            codecs,
            dimensionNames: null,   // v2 doesn't have dimension_names
            attributes,
            zarrVersion: 2);
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static int[] ResolveChunkShape(ZarrJsonDocument doc)
    {
        if (doc.ChunkGrid?.Name != "regular")
            throw new NotSupportedException(
                $"Only 'regular' chunk grids are supported. Got: '{doc.ChunkGrid?.Name}'.");

        var chunkShape = doc.ChunkGrid.Configuration?.ChunkShape;

        if (chunkShape is null || chunkShape.Length == 0)
            throw new InvalidOperationException(
                "Array zarr.json has a regular chunk grid but no chunk_shape.");

        return chunkShape;
    }

    private static CodecInfo[] ResolveCodecs(ZarrJsonDocument doc)
    {
        if (doc.Codecs is null || doc.Codecs.Length == 0)
            return Array.Empty<CodecInfo>();

        return doc.Codecs
            .Select(c => new CodecInfo(c.Name, c.Configuration))
            .ToArray();
    }
}

/// <summary>
/// Typed metadata for a Zarr v3 group node.
/// Groups are pure containers — no data, no shape, just attributes and children.
/// </summary>
public sealed class ZarrGroupMetadata
{
    public JsonElement? RawAttributes { get; }

    private ZarrGroupMetadata(JsonElement? rawAttributes)
    {
        RawAttributes = rawAttributes;
    }

    public static ZarrGroupMetadata FromDocument(ZarrJsonDocument doc)
    {
        if (doc.NodeType != "group")
            throw new InvalidOperationException(
                $"Expected node_type 'group', got '{doc.NodeType}'.");

        return new ZarrGroupMetadata(doc.Attributes);
    }

    public static ZarrGroupMetadata FromV2Document(JsonElement? attributes)
    {
        return new ZarrGroupMetadata(attributes);
    }
}

/// <summary>
/// Lightweight codec descriptor parsed from zarr.json or synthesised from a v2 compressor.
/// The actual IZarrCodec instances are built by CodecFactory from this.
/// </summary>
public sealed record CodecInfo(string Name, JsonElement? Configuration);
