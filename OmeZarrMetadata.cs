using System.Text.Json.Serialization;

namespace ZarrNET.Core.OmeZarr.Metadata;

// =============================================================================
// Multiscale image metadata
// =============================================================================

/// <summary>
/// Parsed "multiscales" entry from the OME-Zarr attributes object.
/// A single zarr.json attributes block may declare one or more multiscale images,
/// though in practice there is almost always exactly one.
/// </summary>
public sealed class MultiscaleMetadata
{
    public string              Version    { get; init; } = string.Empty;
    public string?             Name       { get; init; }
    public string?             Type       { get; init; }
    public AxisMetadata[]      Axes       { get; init; } = Array.Empty<AxisMetadata>();
    public DatasetMetadata[]   Datasets   { get; init; } = Array.Empty<DatasetMetadata>();
    public CoordinateTransformation[]? CoordinateTransformations { get; init; }
    public OmeMetadata?        Omero      { get; init; }
}

/// <summary>
/// A single axis in an OME-Zarr multiscale image.
/// </summary>
public sealed class AxisMetadata
{
    public string  Name { get; init; } = string.Empty;
    public string? Type { get; init; }   // "space" | "time" | "channel"
    public string? Unit { get; init; }   // "micrometer" | "nanometer" | "millisecond" etc.
}

/// <summary>
/// One resolution level (dataset) within a multiscale image.
/// Path is relative to the multiscale group, e.g. "0", "1", "2".
/// </summary>
public sealed class DatasetMetadata
{
    public string                     Path                      { get; init; } = string.Empty;
    public CoordinateTransformation[] CoordinateTransformations { get; init; }
        = Array.Empty<CoordinateTransformation>();
}

// =============================================================================
// Coordinate transformations
// =============================================================================

/// <summary>
/// A coordinate transformation that maps from array index space to physical space.
/// May be a scale, translation, or identity.
/// </summary>
public sealed class CoordinateTransformation
{
    public string    Type        { get; init; } = string.Empty;  // "scale" | "translation" | "identity"
    public double[]? Scale       { get; init; }
    public double[]? Translation { get; init; }
    public string?   Path        { get; init; }  // for "path" type transformations
}

// =============================================================================
// HCS Plate metadata
// =============================================================================

/// <summary>
/// Top-level plate metadata for HCS (High-Content Screening) data.
/// Lives in the root group's attributes under the "plate" key.
/// </summary>
public sealed class PlateMetadata
{
    public string         Name             { get; init; } = string.Empty;
    public ColumnMetadata[] Columns        { get; init; } = Array.Empty<ColumnMetadata>();
    public RowMetadata[]    Rows           { get; init; } = Array.Empty<RowMetadata>();
    public WellReference[]  Wells          { get; init; } = Array.Empty<WellReference>();
    public AcquisitionMetadata[] Acquisitions { get; init; } = Array.Empty<AcquisitionMetadata>();
    public FieldCount?      FieldCount     { get; init; }
    public string?          Version        { get; init; }
}

public sealed class ColumnMetadata
{
    public string Name { get; init; } = string.Empty;
}

public sealed class RowMetadata
{
    public string Name { get; init; } = string.Empty;
}

/// <summary>
/// Reference to a well within a plate, linking row/column names to a path.
/// </summary>
public sealed class WellReference
{
    public string ColumnIndex { get; init; } = string.Empty;
    public string RowIndex    { get; init; } = string.Empty;
    public string Path        { get; init; } = string.Empty;  // e.g. "A/1"
}

public sealed class AcquisitionMetadata
{
    public int     Id          { get; init; }
    public string? Name        { get; init; }
    public string? Description { get; init; }
    public long?   StartTime   { get; init; }
    public long?   EndTime     { get; init; }
}

public sealed class FieldCount
{
    public int MaximumFieldCount { get; init; }
}

// =============================================================================
// Bioformats2raw collection metadata
// =============================================================================

/// <summary>
/// Metadata for a bioformats2raw.layout wrapper group.
/// The wrapper contains numbered sub-groups (0, 1, 2, ...) each holding
/// a multiscale image, and optionally an OME sub-group with a series index
/// and METADATA.ome.xml.
/// </summary>
public sealed class Bioformats2RawMetadata
{
    /// <summary>Layout version — always 3 in the current spec.</summary>
    public int       LayoutVersion { get; init; }

    /// <summary>
    /// Explicit series paths from OME/.zattrs "series" attribute.
    /// Null if the OME sub-group is absent or has no "series" attribute,
    /// in which case series are discovered by probing consecutive numbered groups.
    /// </summary>
    public string[]? SeriesPaths   { get; init; }

    /// <summary>
    /// Rich metadata parsed from OME/METADATA.ome.xml, if present.
    /// Contains series names, channel info, physical sizes, and instrument
    /// details that are not available from the Zarr attributes alone.
    /// Null if the file is absent or could not be parsed.
    /// </summary>
    public OmeXmlMetadata? OmeXml  { get; init; }
}

// =============================================================================
// HCS Well metadata
// =============================================================================

/// <summary>
/// Well metadata. Lives in each well group's attributes under the "well" key.
/// Lists all fields (acquisitions/images) within the well.
/// </summary>
public sealed class WellMetadata
{
    public FieldReference[] Images  { get; init; } = Array.Empty<FieldReference>();
    public string?          Version { get; init; }
}

/// <summary>
/// Reference to a field (image) within a well.
/// </summary>
public sealed class FieldReference
{
    public int     AcquisitionId { get; init; }
    public string  Path          { get; init; } = string.Empty;  // e.g. "0"
}

// =============================================================================
// Label metadata
// =============================================================================

/// <summary>
/// Label group metadata. Lives in the "labels" sub-group's attributes.
/// Lists the names of label arrays available under this group.
/// </summary>
public sealed class LabelGroupMetadata
{
    public string[] Labels { get; init; } = Array.Empty<string>();
}

/// <summary>
/// Metadata for a single label array. The array itself has multiscale metadata
/// just like an image, with an additional "image-label" attribute.
/// </summary>
public sealed class ImageLabelMetadata
{
    public string?           Version  { get; init; }
    public LabelColorEntry[] Colors   { get; init; } = Array.Empty<LabelColorEntry>();
    public LabelProperty[]   Properties { get; init; } = Array.Empty<LabelProperty>();
    public LabelSourceLink?  Source   { get; init; }
}

public sealed class LabelColorEntry
{
    public int      LabelValue { get; init; }
    public int[]?   Rgba       { get; init; }  // [r, g, b, a] 0-255
}

public sealed class LabelProperty
{
    public int                           LabelValue  { get; init; }
    public Dictionary<string, object>?   Properties  { get; init; }
}

public sealed class LabelSourceLink
{
    public string? Href { get; init; }  // relative path to the source image
}

// =============================================================================
// Omero / display metadata (optional)
// =============================================================================

/// <summary>
/// Optional rendering metadata from Omero, carried in the multiscale attributes.
/// Used for display hints: channel colours, contrast limits, etc.
/// </summary>
public sealed class OmeMetadata
{
    public ChannelDisplayMetadata[] Channels { get; init; } = Array.Empty<ChannelDisplayMetadata>();
    public RenderingWindowMetadata? Rdefs    { get; init; }
}

public sealed class ChannelDisplayMetadata
{
    public bool?   Active    { get; init; }
    public string? Color     { get; init; }  // hex string e.g. "FF0000"
    public string? Label     { get; init; }
    public WindowMetadata? Window { get; init; }
}

public sealed class WindowMetadata
{
    public double? Min   { get; init; }
    public double? Max   { get; init; }
    public double? Start { get; init; }
    public double? End   { get; init; }
}

public sealed class RenderingWindowMetadata
{
    public string? DefaultZ { get; init; }
    public string? DefaultT { get; init; }
    public string? Model    { get; init; }  // "color" | "greyscale"
}
