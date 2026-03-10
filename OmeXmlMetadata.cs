namespace ZarrNET.Core.OmeZarr.Metadata;

// =============================================================================
// OME-XML metadata (from METADATA.ome.xml in bioformats2raw datasets)
// =============================================================================

/// <summary>
/// Top-level container for metadata parsed from METADATA.ome.xml.
/// Contains all images (series) and instruments defined in the XML.
///
/// The Image array is ordered to match the series numbering in the
/// bioformats2raw layout: Images[0] corresponds to series sub-group "0",
/// Images[1] to "1", and so on.
/// </summary>
public sealed class OmeXmlMetadata
{
    public OmeXmlImageMetadata[]      Images      { get; init; } = Array.Empty<OmeXmlImageMetadata>();
    public OmeXmlInstrumentMetadata[] Instruments { get; init; } = Array.Empty<OmeXmlInstrumentMetadata>();

    /// <summary>Number of images (series) declared in the OME-XML.</summary>
    public int ImageCount => Images.Length;
}

/// <summary>
/// Metadata for a single Image element in OME-XML.
/// Corresponds to one series in the bioformats2raw layout.
///
/// Carries the "rich" metadata that isn't available from the Zarr
/// attributes alone: series name, channel names, physical sizes with
/// units, instrument references, acquisition date, etc.
/// </summary>
public sealed class OmeXmlImageMetadata
{
    // -- Identity --
    public string? Id               { get; init; }   // e.g. "Image:0"
    public string? Name             { get; init; }   // e.g. "Slide1-Region2"
    public string? AcquisitionDate  { get; init; }
    public string? Description      { get; init; }

    // -- Pixel dimensions --
    public int?    SizeX            { get; init; }
    public int?    SizeY            { get; init; }
    public int?    SizeZ            { get; init; }
    public int?    SizeC            { get; init; }
    public int?    SizeT            { get; init; }
    public string? PixelType        { get; init; }   // "uint8", "uint16", "float", ...
    public string? DimensionOrder   { get; init; }   // "XYCZT", "XYZTC", ...
    public int?    SignificantBits  { get; init; }

    // -- Physical sizes --
    public double? PhysicalSizeX     { get; init; }
    public double? PhysicalSizeY     { get; init; }
    public double? PhysicalSizeZ     { get; init; }
    public string  PhysicalSizeXUnit { get; init; } = "µm";
    public string  PhysicalSizeYUnit { get; init; } = "µm";
    public string  PhysicalSizeZUnit { get; init; } = "µm";

    // -- Time --
    public double? TimeIncrement     { get; init; }
    public string  TimeIncrementUnit { get; init; } = "s";

    // -- Channels --
    public OmeXmlChannelMetadata[] Channels { get; init; } = Array.Empty<OmeXmlChannelMetadata>();

    // -- Instrument link --
    public OmeXmlInstrumentMetadata? Instrument  { get; init; }
    public string?                   ObjectiveId { get; init; }

    /// <summary>
    /// Convenience: resolves the objective used for this image from the
    /// linked instrument's objective list, using the ObjectiveSettings ID.
    /// Returns null if no instrument or objective is linked.
    /// </summary>
    public OmeXmlObjectiveMetadata? GetObjective()
    {
        if (Instrument is null || ObjectiveId is null)
            return null;

        return Instrument.Objectives
            .FirstOrDefault(o => string.Equals(o.Id, ObjectiveId, StringComparison.Ordinal));
    }
}

/// <summary>
/// Metadata for a single Channel element within an Image's Pixels.
/// </summary>
public sealed class OmeXmlChannelMetadata
{
    public string? Id                    { get; init; }
    public string? Name                  { get; init; }   // e.g. "DAPI", "GFP", "CH1"
    public int?    Color                 { get; init; }   // packed ARGB integer
    public double? EmissionWavelength    { get; init; }   // nm
    public double? ExcitationWavelength  { get; init; }   // nm
    public string? Fluor                 { get; init; }   // fluorophore name
    public string? IlluminationType      { get; init; }   // "Epifluorescence", "Transmitted", ...
    public string? AcquisitionMode       { get; init; }   // "LaserScanningConfocalMicroscopy", ...
    public string? ContrastMethod        { get; init; }   // "Fluorescence", "Brightfield", ...
    public double? PinholeSize           { get; init; }
    public string? PinholeSizeUnit       { get; init; }
    public int?    SamplesPerPixel       { get; init; }
}

// =============================================================================
// Instrument metadata
// =============================================================================

/// <summary>
/// Metadata for an Instrument element in OME-XML.
/// Contains the microscope, objectives, and detectors used for acquisition.
/// </summary>
public sealed class OmeXmlInstrumentMetadata
{
    public string? Id                     { get; init; }
    public string? MicroscopeModel        { get; init; }
    public string? MicroscopeManufacturer { get; init; }
    public string? MicroscopeType         { get; init; }

    public OmeXmlObjectiveMetadata[] Objectives { get; init; } = Array.Empty<OmeXmlObjectiveMetadata>();
    public OmeXmlDetectorMetadata[]  Detectors  { get; init; } = Array.Empty<OmeXmlDetectorMetadata>();
}

public sealed class OmeXmlObjectiveMetadata
{
    public string? Id                   { get; init; }
    public string? Model                { get; init; }
    public string? Manufacturer         { get; init; }
    public double? NominalMagnification { get; init; }
    public double? LensNA               { get; init; }
    public string? Immersion            { get; init; }   // "Oil", "Water", "Air", ...
    public string? Correction           { get; init; }   // "PlanApo", "PlanFluor", ...
}

public sealed class OmeXmlDetectorMetadata
{
    public string? Id           { get; init; }
    public string? Model        { get; init; }
    public string? Manufacturer { get; init; }
    public string? Type         { get; init; }   // "CCD", "PMT", "EMCCD", ...
}
