using System.Text.Json;
using System.Text.Json.Serialization;

namespace ZarrNET.Core.OmeZarr.Metadata;

/// <summary>
/// Detects the OME-Zarr node type from a raw attributes JsonElement and
/// deserializes the relevant metadata models. This is the single entry
/// point between the raw Zarr attributes and the typed OME metadata layer.
///
/// Handles both NGFF 0.4 (flat) and 0.5 ("ome"-wrapped) attribute layouts:
///
///   0.4 / v2:  { "multiscales": [...] }
///   0.5 / v3:  { "ome": { "version": "0.5", "multiscales": [...] } }
///
/// The "ome" envelope is unwrapped transparently so all downstream parsing
/// sees the same structure regardless of spec version.
/// </summary>
public static class OmeAttributesParser
{
    private static readonly JsonSerializerOptions _options = new()
    {
        PropertyNameCaseInsensitive = true,
        ReadCommentHandling         = JsonCommentHandling.Skip,
        AllowTrailingCommas         = true,
        NumberHandling              = JsonNumberHandling.AllowReadingFromString,
        Converters                  = { new AxisMetadataJsonConverter() }
    };

    // -------------------------------------------------------------------------
    // Node type detection
    // -------------------------------------------------------------------------

    public enum OmeNodeType
    {
        Unknown,
        MultiscaleImage,
        Plate,
        Well,
        LabelGroup,
        LabelImage,                 // a multiscale that also carries image-label metadata
        Bioformats2RawCollection    // bioformats2raw.layout wrapper containing numbered series
    }

    public static OmeNodeType DetectNodeType(JsonElement? attributes)
    {
        if (attributes is null)
            return OmeNodeType.Unknown;

        var attrs = ResolveOmeAttributes(attributes.Value);

        // Plate takes precedence even if bioformats2raw.layout is also present (per spec)
        if (attrs.TryGetProperty("plate", out _))
            return OmeNodeType.Plate;

        // bioformats2raw wrapper — checked early because the wrapper root
        // won't have multiscales/well/labels keys at this level
        if (attrs.TryGetProperty("bioformats2raw.layout", out _))
            return OmeNodeType.Bioformats2RawCollection;

        if (attrs.TryGetProperty("well", out _))
            return OmeNodeType.Well;

        if (attrs.TryGetProperty("labels", out _))
            return OmeNodeType.LabelGroup;

        if (attrs.TryGetProperty("multiscales", out _))
        {
            return attrs.TryGetProperty("image-label", out _)
                ? OmeNodeType.LabelImage
                : OmeNodeType.MultiscaleImage;
        }

        return OmeNodeType.Unknown;
    }

    // -------------------------------------------------------------------------
    // NGFF version detection
    // -------------------------------------------------------------------------

    /// <summary>
    /// Detects the NGFF specification version from the attributes.
    ///
    /// Checks two locations:
    ///   1. The "ome.version" field (0.5+ envelope style)
    ///   2. The "multiscales[0].version" field (0.4 and earlier)
    ///
    /// Returns null if no version can be determined.
    /// </summary>
    public static string? DetectNgffVersion(JsonElement? attributes)
    {
        if (attributes is null)
            return null;

        var raw = attributes.Value;

        // Check for "ome" envelope version first — this is the 0.5+ indicator
        if (raw.TryGetProperty("ome", out var omeEl) &&
            omeEl.TryGetProperty("version", out var omeVersionEl) &&
            omeVersionEl.ValueKind == JsonValueKind.String)
        {
            return omeVersionEl.GetString();
        }

        // Fall back to multiscales[0].version (0.4 and earlier)
        var resolved = ResolveOmeAttributes(raw);

        if (resolved.TryGetProperty("multiscales", out var msEl) &&
            msEl.ValueKind == JsonValueKind.Array &&
            msEl.GetArrayLength() > 0)
        {
            var first = msEl[0];
            if (first.TryGetProperty("version", out var versionEl) &&
                versionEl.ValueKind == JsonValueKind.String)
            {
                return versionEl.GetString();
            }
        }

        return null;
    }

    // -------------------------------------------------------------------------
    // Typed metadata parsing
    // -------------------------------------------------------------------------

    public static MultiscaleMetadata[] ParseMultiscales(JsonElement attributes)
    {
        var resolved = ResolveOmeAttributes(attributes);

        if (!resolved.TryGetProperty("multiscales", out var multiscalesEl))
            throw new InvalidOperationException("Attributes do not contain a 'multiscales' key.");

        return JsonSerializer.Deserialize<MultiscaleMetadataJson[]>(multiscalesEl.GetRawText(), _options)
               !.Select(ToMultiscaleMetadata)
               .ToArray();
    }

    public static PlateMetadata ParsePlate(JsonElement attributes)
    {
        var resolved = ResolveOmeAttributes(attributes);

        if (!resolved.TryGetProperty("plate", out var plateEl))
            throw new InvalidOperationException("Attributes do not contain a 'plate' key.");

        return JsonSerializer.Deserialize<PlateMetadataJson>(plateEl.GetRawText(), _options)
               !.ToModel();
    }

    public static WellMetadata ParseWell(JsonElement attributes)
    {
        var resolved = ResolveOmeAttributes(attributes);

        if (!resolved.TryGetProperty("well", out var wellEl))
            throw new InvalidOperationException("Attributes do not contain a 'well' key.");

        return JsonSerializer.Deserialize<WellMetadataJson>(wellEl.GetRawText(), _options)
               !.ToModel();
    }

    public static LabelGroupMetadata ParseLabelGroup(JsonElement attributes)
    {
        var resolved = ResolveOmeAttributes(attributes);

        if (!resolved.TryGetProperty("labels", out var labelsEl))
            throw new InvalidOperationException("Attributes do not contain a 'labels' key.");

        var labels = JsonSerializer.Deserialize<string[]>(labelsEl.GetRawText(), _options)
                     ?? Array.Empty<string>();

        return new LabelGroupMetadata { Labels = labels };
    }

    public static ImageLabelMetadata ParseImageLabel(JsonElement attributes)
    {
        var resolved = ResolveOmeAttributes(attributes);

        if (!resolved.TryGetProperty("image-label", out var labelEl))
            throw new InvalidOperationException("Attributes do not contain an 'image-label' key.");

        return JsonSerializer.Deserialize<ImageLabelMetadataJson>(labelEl.GetRawText(), _options)
               !.ToModel();
    }

    /// <summary>
    /// Parses bioformats2raw.layout metadata from the root attributes and
    /// optionally the OME sub-group attributes (which carry the series list).
    /// </summary>
    public static Bioformats2RawMetadata ParseBioformats2Raw(
        JsonElement      rootAttributes,
        JsonElement?     omeGroupAttributes,
        OmeXmlMetadata?  omeXml = null)
    {
        var resolved = ResolveOmeAttributes(rootAttributes);

        // Layout version (always 3 in practice, but parse whatever is there)
        int layoutVersion = 0;
        if (resolved.TryGetProperty("bioformats2raw.layout", out var layoutEl) &&
            layoutEl.ValueKind == JsonValueKind.Number)
        {
            layoutVersion = layoutEl.GetInt32();
        }

        // Series paths come from the OME sub-group's "series" attribute
        string[]? seriesPaths = null;
        if (omeGroupAttributes is not null)
        {
            var omeAttrs = omeGroupAttributes.Value;
            if (omeAttrs.TryGetProperty("series", out var seriesEl) &&
                seriesEl.ValueKind == JsonValueKind.Array)
            {
                seriesPaths = JsonSerializer.Deserialize<string[]>(
                    seriesEl.GetRawText(), _options);
            }
        }

        return new Bioformats2RawMetadata
        {
            LayoutVersion = layoutVersion,
            SeriesPaths   = seriesPaths,
            OmeXml        = omeXml
        };
    }

    // -------------------------------------------------------------------------
    // "ome" envelope resolution
    // -------------------------------------------------------------------------

    /// <summary>
    /// If the attributes contain an "ome" object (NGFF 0.5+ layout), returns
    /// the inner element so callers see the same key structure as NGFF 0.4.
    /// Otherwise returns the original element unchanged.
    ///
    /// NGFF 0.4:  { "multiscales": [...] }              → returned as-is
    /// NGFF 0.5:  { "ome": { "multiscales": [...] } }   → returns the "ome" inner object
    /// </summary>
    private static JsonElement ResolveOmeAttributes(JsonElement attributes)
    {
        if (attributes.TryGetProperty("ome", out var omeEl) &&
            omeEl.ValueKind == JsonValueKind.Object)
        {
            return omeEl;
        }

        return attributes;
    }

    // -------------------------------------------------------------------------
    // JSON intermediate models → typed metadata models
    // These exist so we can handle JSON naming conventions separately from
    // the clean domain model, keeping the metadata classes free of JsonProperty noise.
    // -------------------------------------------------------------------------

    private static MultiscaleMetadata ToMultiscaleMetadata(MultiscaleMetadataJson json) =>
        new()
        {
            Version  = json.Version ?? string.Empty,
            Name     = json.Name,
            Type     = json.Type,
            Axes     = json.Axes?.Select(ToAxisMetadata).ToArray() ?? Array.Empty<AxisMetadata>(),
            Datasets = json.Datasets?.Select(ToDatasetMetadata).ToArray() ?? Array.Empty<DatasetMetadata>(),
            CoordinateTransformations = json.CoordinateTransformations?
                .Select(ToCoordinateTransformation).ToArray(),
            Omero = json.Omero is null ? null : ToOmeMetadata(json.Omero)
        };

    private static AxisMetadata ToAxisMetadata(AxisMetadataJson json) =>
        new() { Name = json.Name ?? string.Empty, Type = json.Type, Unit = json.Unit };

    private static DatasetMetadata ToDatasetMetadata(DatasetMetadataJson json) =>
        new()
        {
            Path = json.Path ?? string.Empty,
            CoordinateTransformations = json.CoordinateTransformations?
                .Select(ToCoordinateTransformation).ToArray() ?? Array.Empty<CoordinateTransformation>()
        };

    private static CoordinateTransformation ToCoordinateTransformation(CoordinateTransformationJson json) =>
        new()
        {
            Type        = json.Type ?? string.Empty,
            Scale       = json.Scale,
            Translation = json.Translation,
            Path        = json.Path
        };

    private static OmeMetadata ToOmeMetadata(OmeMetadataJson json) =>
        new()
        {
            Channels = json.Channels?.Select(ToChannelMetadata).ToArray() ?? Array.Empty<ChannelDisplayMetadata>(),
            Rdefs    = json.Rdefs is null ? null : new RenderingWindowMetadata
            {
                DefaultZ = json.Rdefs.DefaultZ,
                DefaultT = json.Rdefs.DefaultT,
                Model    = json.Rdefs.Model
            }
        };

    private static ChannelDisplayMetadata ToChannelMetadata(ChannelDisplayMetadataJson json) =>
        new()
        {
            Active = json.Active,
            Color  = json.Color,
            Label  = json.Label,
            Window = json.Window is null ? null : new WindowMetadata
            {
                Min   = json.Window.Min,
                Max   = json.Window.Max,
                Start = json.Window.Start,
                End   = json.Window.End
            }
        };

    // -------------------------------------------------------------------------
    // JSON intermediate types (private — not part of the public API)
    // -------------------------------------------------------------------------

    private sealed class MultiscaleMetadataJson
    {
        [JsonPropertyName("version")]     public string? Version  { get; init; }
        [JsonPropertyName("name")]        public string? Name     { get; init; }
        [JsonPropertyName("type")]        public string? Type     { get; init; }
        [JsonPropertyName("axes")]        public AxisMetadataJson[]? Axes { get; init; }
        [JsonPropertyName("datasets")]    public DatasetMetadataJson[]? Datasets { get; init; }
        [JsonPropertyName("coordinateTransformations")]
        public CoordinateTransformationJson[]? CoordinateTransformations { get; init; }
        [JsonPropertyName("omero")]       public OmeMetadataJson? Omero { get; init; }
    }

    private sealed class AxisMetadataJson
    {
        [JsonPropertyName("name")] public string? Name { get; init; }
        [JsonPropertyName("type")] public string? Type { get; init; }
        [JsonPropertyName("unit")] public string? Unit { get; init; }
    }

    /// <summary>
    /// Handles both OME-Zarr axis representations:
    ///   v0.1/v0.2 — axes as a plain string array:  ["t", "c", "z", "y", "x"]
    ///   v0.3+     — axes as an object array:        [{"name":"t","type":"time"}, ...]
    /// When a string token is encountered the name is taken from the string value
    /// directly; type and unit are left null and inferred later by EffectiveAxes.
    /// </summary>
    private sealed class AxisMetadataJsonConverter : JsonConverter<AxisMetadataJson>
    {
        public override AxisMetadataJson Read(
            ref Utf8JsonReader   reader,
            Type                 typeToConvert,
            JsonSerializerOptions options)
        {
            if (reader.TokenType == JsonTokenType.String)
                return new AxisMetadataJson { Name = reader.GetString() };

            if (reader.TokenType != JsonTokenType.StartObject)
                throw new JsonException(
                    $"Unexpected token {reader.TokenType} reading AxisMetadataJson.");

            string? name = null, type = null, unit = null;

            while (reader.Read() && reader.TokenType != JsonTokenType.EndObject)
            {
                if (reader.TokenType != JsonTokenType.PropertyName)
                    continue;

                var propertyName = reader.GetString()!.ToLowerInvariant();
                reader.Read();

                switch (propertyName)
                {
                    case "name": name = reader.GetString(); break;
                    case "type": type = reader.GetString(); break;
                    case "unit": unit = reader.GetString(); break;
                    default:     reader.Skip();             break;
                }
            }

            return new AxisMetadataJson { Name = name, Type = type, Unit = unit };
        }

        public override void Write(
            Utf8JsonWriter        writer,
            AxisMetadataJson      value,
            JsonSerializerOptions options)
        {
            writer.WriteStartObject();
            if (value.Name is not null) writer.WriteString("name", value.Name);
            if (value.Type is not null) writer.WriteString("type", value.Type);
            if (value.Unit is not null) writer.WriteString("unit", value.Unit);
            writer.WriteEndObject();
        }
    }

    private sealed class DatasetMetadataJson
    {
        [JsonPropertyName("path")] public string? Path { get; init; }
        [JsonPropertyName("coordinateTransformations")]
        public CoordinateTransformationJson[]? CoordinateTransformations { get; init; }
    }

    private sealed class CoordinateTransformationJson
    {
        [JsonPropertyName("type")]        public string?   Type        { get; init; }
        [JsonPropertyName("scale")]       public double[]? Scale       { get; init; }
        [JsonPropertyName("translation")] public double[]? Translation { get; init; }
        [JsonPropertyName("path")]        public string?   Path        { get; init; }
    }

    private sealed class PlateMetadataJson
    {
        [JsonPropertyName("name")]         public string? Name         { get; init; }
        [JsonPropertyName("columns")]      public ColumnJson[]? Columns { get; init; }
        [JsonPropertyName("rows")]         public RowJson[]? Rows       { get; init; }
        [JsonPropertyName("wells")]        public WellReferenceJson[]? Wells { get; init; }
        [JsonPropertyName("acquisitions")] public AcquisitionJson[]? Acquisitions { get; init; }
        [JsonPropertyName("field_count")]  public int? FieldCount { get; init; }
        [JsonPropertyName("version")]      public string? Version { get; init; }

        public PlateMetadata ToModel() => new()
        {
            Name         = Name ?? string.Empty,
            Columns      = Columns?.Select(c => new ColumnMetadata { Name = c.Name ?? "" }).ToArray() ?? Array.Empty<ColumnMetadata>(),
            Rows         = Rows?.Select(r => new RowMetadata { Name = r.Name ?? "" }).ToArray() ?? Array.Empty<RowMetadata>(),
            Wells        = Wells?.Select(w => new WellReference { ColumnIndex = w.ColumnIndex ?? "", RowIndex = w.RowIndex ?? "", Path = w.Path ?? "" }).ToArray() ?? Array.Empty<WellReference>(),
            Acquisitions = Acquisitions?.Select(a => new AcquisitionMetadata { Id = a.Id, Name = a.Name, Description = a.Description }).ToArray() ?? Array.Empty<AcquisitionMetadata>(),
            Version      = Version
        };
    }

    private sealed class ColumnJson
    {
        [JsonPropertyName("name")] public string? Name { get; init; }
    }

    private sealed class RowJson
    {
        [JsonPropertyName("name")] public string? Name { get; init; }
    }

    private sealed class WellReferenceJson
    {
        [JsonPropertyName("columnIndex")] public string? ColumnIndex { get; init; }
        [JsonPropertyName("rowIndex")]    public string? RowIndex    { get; init; }
        [JsonPropertyName("path")]        public string? Path        { get; init; }
    }

    private sealed class AcquisitionJson
    {
        [JsonPropertyName("id")]          public int     Id          { get; init; }
        [JsonPropertyName("name")]        public string? Name        { get; init; }
        [JsonPropertyName("description")] public string? Description { get; init; }
        [JsonPropertyName("starttime")]   public long?   StartTime   { get; init; }
        [JsonPropertyName("endtime")]     public long?   EndTime     { get; init; }
    }

    private sealed class WellMetadataJson
    {
        [JsonPropertyName("images")]  public FieldReferenceJson[]? Images  { get; init; }
        [JsonPropertyName("version")] public string?               Version { get; init; }

        public WellMetadata ToModel() => new()
        {
            Images  = Images?.Select(i => new FieldReference { AcquisitionId = i.Acquisition, Path = i.Path ?? "" }).ToArray() ?? Array.Empty<FieldReference>(),
            Version = Version
        };
    }

    private sealed class FieldReferenceJson
    {
        [JsonPropertyName("acquisition")] public int     Acquisition { get; init; }
        [JsonPropertyName("path")]        public string? Path        { get; init; }
    }

    private sealed class ImageLabelMetadataJson
    {
        [JsonPropertyName("version")]    public string?              Version    { get; init; }
        [JsonPropertyName("colors")]     public LabelColorJson[]?    Colors     { get; init; }
        [JsonPropertyName("properties")] public LabelPropertyJson[]? Properties { get; init; }
        [JsonPropertyName("source")]     public LabelSourceJson?     Source     { get; init; }

        public ImageLabelMetadata ToModel() => new()
        {
            Version    = Version,
            Colors     = Colors?.Select(c => new LabelColorEntry { LabelValue = c.LabelValue, Rgba = c.Rgba }).ToArray() ?? Array.Empty<LabelColorEntry>(),
            Properties = Properties?.Select(p => new LabelProperty { LabelValue = p.LabelValue }).ToArray() ?? Array.Empty<LabelProperty>(),
            Source     = Source is null ? null : new LabelSourceLink { Href = Source.Href }
        };
    }

    private sealed class LabelColorJson
    {
        [JsonPropertyName("label-value")] public int    LabelValue { get; init; }
        [JsonPropertyName("rgba")]        public int[]? Rgba       { get; init; }
    }

    private sealed class LabelPropertyJson
    {
        [JsonPropertyName("label-value")] public int LabelValue { get; init; }
    }

    private sealed class LabelSourceJson
    {
        [JsonPropertyName("href")] public string? Href { get; init; }
    }

    private sealed class OmeMetadataJson
    {
        [JsonPropertyName("channels")] public ChannelDisplayMetadataJson[]? Channels { get; init; }
        [JsonPropertyName("rdefs")]    public RenderingWindowJson?           Rdefs    { get; init; }
    }

    private sealed class ChannelDisplayMetadataJson
    {
        [JsonPropertyName("active")] public bool?   Active { get; init; }
        [JsonPropertyName("color")]  public string? Color  { get; init; }
        [JsonPropertyName("label")]  public string? Label  { get; init; }
        [JsonPropertyName("window")] public WindowJson? Window { get; init; }
    }

    private sealed class WindowJson
    {
        [JsonPropertyName("min")]   public double? Min   { get; init; }
        [JsonPropertyName("max")]   public double? Max   { get; init; }
        [JsonPropertyName("start")] public double? Start { get; init; }
        [JsonPropertyName("end")]   public double? End   { get; init; }
    }

    private sealed class RenderingWindowJson
    {
        [JsonPropertyName("defaultZ")] public string? DefaultZ { get; init; }
        [JsonPropertyName("defaultT")] public string? DefaultT { get; init; }
        [JsonPropertyName("model")]    public string? Model    { get; init; }
    }
}
