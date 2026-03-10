using System.Xml.Linq;

namespace ZarrNET.Core.OmeZarr.Metadata;

/// <summary>
/// Parses METADATA.ome.xml files found in bioformats2raw.layout datasets.
///
/// This is a focused, read-only parser that extracts key imaging metadata
/// from the OME-XML model: image/series names, pixel dimensions, physical
/// sizes, channel info, and instrument details.
///
/// It does NOT attempt to model the full OME-XML schema — only the subset
/// that is useful for navigating and interpreting bioformats2raw datasets.
///
/// The OME-XML namespace varies across schema versions (2013-06, 2016-06, etc.)
/// so all element lookups use a namespace-agnostic approach.
/// </summary>
public static class OmeXmlParser
{
    // -------------------------------------------------------------------------
    // Public entry point
    // -------------------------------------------------------------------------

    /// <summary>
    /// Parses raw METADATA.ome.xml bytes into a typed metadata model.
    /// Returns null if the bytes are empty or cannot be parsed.
    /// </summary>
    public static OmeXmlMetadata? TryParse(byte[] xmlBytes)
    {
        if (xmlBytes is null || xmlBytes.Length == 0)
            return null;

        try
        {
            return Parse(xmlBytes);
        }
        catch
        {
            return null;
        }
    }

    /// <summary>
    /// Parses raw METADATA.ome.xml bytes into a typed metadata model.
    /// Throws on malformed XML or missing required elements.
    /// </summary>
    public static OmeXmlMetadata Parse(byte[] xmlBytes)
    {
        using var stream = new MemoryStream(xmlBytes);
        var doc = XDocument.Load(stream);

        if (doc.Root is null)
            throw new InvalidOperationException("METADATA.ome.xml has no root element.");

        var instruments = ParseInstruments(doc.Root);
        var images      = ParseImages(doc.Root, instruments);

        return new OmeXmlMetadata
        {
            Images      = images,
            Instruments = instruments.Values.ToArray()
        };
    }

    // -------------------------------------------------------------------------
    // Instrument parsing
    // -------------------------------------------------------------------------

    /// <summary>
    /// Parses all Instrument elements into a dictionary keyed by their ID
    /// attribute, so Image elements can resolve InstrumentRef links.
    /// </summary>
    private static Dictionary<string, OmeXmlInstrumentMetadata> ParseInstruments(XElement root)
    {
        var result = new Dictionary<string, OmeXmlInstrumentMetadata>(StringComparer.Ordinal);

        foreach (var instrumentEl in FindLocalElements(root, "Instrument"))
        {
            var id = instrumentEl.Attribute("ID")?.Value;
            if (id is null) continue;

            var microscope  = FindLocalElement(instrumentEl, "Microscope");
            var objectives  = ParseObjectives(instrumentEl);
            var detectors   = ParseDetectors(instrumentEl);

            var instrument = new OmeXmlInstrumentMetadata
            {
                Id                    = id,
                MicroscopeModel       = microscope?.Attribute("Model")?.Value,
                MicroscopeManufacturer = microscope?.Attribute("Manufacturer")?.Value,
                MicroscopeType        = microscope?.Attribute("Type")?.Value,
                Objectives            = objectives,
                Detectors             = detectors
            };

            result[id] = instrument;
        }

        return result;
    }

    private static OmeXmlObjectiveMetadata[] ParseObjectives(XElement instrumentEl)
    {
        return FindLocalElements(instrumentEl, "Objective")
            .Select(el => new OmeXmlObjectiveMetadata
            {
                Id                     = el.Attribute("ID")?.Value,
                Model                  = el.Attribute("Model")?.Value,
                Manufacturer           = el.Attribute("Manufacturer")?.Value,
                NominalMagnification   = TryParseDouble(el.Attribute("NominalMagnification")?.Value),
                LensNA                 = TryParseDouble(el.Attribute("LensNA")?.Value),
                Immersion              = el.Attribute("Immersion")?.Value,
                Correction             = el.Attribute("Correction")?.Value
            })
            .ToArray();
    }

    private static OmeXmlDetectorMetadata[] ParseDetectors(XElement instrumentEl)
    {
        return FindLocalElements(instrumentEl, "Detector")
            .Select(el => new OmeXmlDetectorMetadata
            {
                Id           = el.Attribute("ID")?.Value,
                Model        = el.Attribute("Model")?.Value,
                Manufacturer = el.Attribute("Manufacturer")?.Value,
                Type         = el.Attribute("Type")?.Value
            })
            .ToArray();
    }

    // -------------------------------------------------------------------------
    // Image parsing
    // -------------------------------------------------------------------------

    private static OmeXmlImageMetadata[] ParseImages(
        XElement root,
        Dictionary<string, OmeXmlInstrumentMetadata> instruments)
    {
        return FindLocalElements(root, "Image")
            .Select(imageEl => ParseSingleImage(imageEl, instruments))
            .ToArray();
    }

    private static OmeXmlImageMetadata ParseSingleImage(
        XElement imageEl,
        Dictionary<string, OmeXmlInstrumentMetadata> instruments)
    {
        var id              = imageEl.Attribute("ID")?.Value;
        var name            = imageEl.Attribute("Name")?.Value;
        var acquisitionDate = FindLocalElement(imageEl, "AcquisitionDate")?.Value;
        var description     = FindLocalElement(imageEl, "Description")?.Value;

        // Resolve instrument reference
        var instrumentRefId = FindLocalElement(imageEl, "InstrumentRef")?.Attribute("ID")?.Value;
        OmeXmlInstrumentMetadata? instrument = null;
        if (instrumentRefId is not null)
            instruments.TryGetValue(instrumentRefId, out instrument);

        // Resolve objective settings
        var objectiveSettingsEl = FindLocalElement(imageEl, "ObjectiveSettings");
        string? objectiveId    = objectiveSettingsEl?.Attribute("ID")?.Value;

        // Parse the Pixels element (there is exactly one per Image in practice)
        var pixelsEl = FindLocalElement(imageEl, "Pixels");
        var channels = pixelsEl is not null ? ParseChannels(pixelsEl) : Array.Empty<OmeXmlChannelMetadata>();

        var physicalSizeX     = TryParseDouble(pixelsEl?.Attribute("PhysicalSizeX")?.Value);
        var physicalSizeY     = TryParseDouble(pixelsEl?.Attribute("PhysicalSizeY")?.Value);
        var physicalSizeZ     = TryParseDouble(pixelsEl?.Attribute("PhysicalSizeZ")?.Value);
        var physicalSizeXUnit = pixelsEl?.Attribute("PhysicalSizeXUnit")?.Value;
        var physicalSizeYUnit = pixelsEl?.Attribute("PhysicalSizeYUnit")?.Value;
        var physicalSizeZUnit = pixelsEl?.Attribute("PhysicalSizeZUnit")?.Value;
        var timeIncrement     = TryParseDouble(pixelsEl?.Attribute("TimeIncrement")?.Value);
        var timeIncrementUnit = pixelsEl?.Attribute("TimeIncrementUnit")?.Value;

        var sizeX = TryParseInt(pixelsEl?.Attribute("SizeX")?.Value);
        var sizeY = TryParseInt(pixelsEl?.Attribute("SizeY")?.Value);
        var sizeZ = TryParseInt(pixelsEl?.Attribute("SizeZ")?.Value);
        var sizeC = TryParseInt(pixelsEl?.Attribute("SizeC")?.Value);
        var sizeT = TryParseInt(pixelsEl?.Attribute("SizeT")?.Value);
        var pixelType        = pixelsEl?.Attribute("Type")?.Value;
        var dimensionOrder   = pixelsEl?.Attribute("DimensionOrder")?.Value;
        var significantBits  = TryParseInt(pixelsEl?.Attribute("SignificantBits")?.Value);

        return new OmeXmlImageMetadata
        {
            Id               = id,
            Name             = name,
            AcquisitionDate  = acquisitionDate,
            Description      = description,

            SizeX            = sizeX,
            SizeY            = sizeY,
            SizeZ            = sizeZ,
            SizeC            = sizeC,
            SizeT            = sizeT,
            PixelType        = pixelType,
            DimensionOrder   = dimensionOrder,
            SignificantBits  = significantBits,

            PhysicalSizeX     = physicalSizeX,
            PhysicalSizeY     = physicalSizeY,
            PhysicalSizeZ     = physicalSizeZ,
            PhysicalSizeXUnit = physicalSizeXUnit ?? "µm",
            PhysicalSizeYUnit = physicalSizeYUnit ?? "µm",
            PhysicalSizeZUnit = physicalSizeZUnit ?? "µm",

            TimeIncrement     = timeIncrement,
            TimeIncrementUnit = timeIncrementUnit ?? "s",

            Channels          = channels,
            Instrument        = instrument,
            ObjectiveId       = objectiveId
        };
    }

    // -------------------------------------------------------------------------
    // Channel parsing
    // -------------------------------------------------------------------------

    private static OmeXmlChannelMetadata[] ParseChannels(XElement pixelsEl)
    {
        return FindLocalElements(pixelsEl, "Channel")
            .Select(ch => new OmeXmlChannelMetadata
            {
                Id                   = ch.Attribute("ID")?.Value,
                Name                 = ch.Attribute("Name")?.Value,
                Color                = TryParseInt(ch.Attribute("Color")?.Value),
                EmissionWavelength   = TryParseDouble(ch.Attribute("EmissionWavelength")?.Value),
                ExcitationWavelength = TryParseDouble(ch.Attribute("ExcitationWavelength")?.Value),
                Fluor                = ch.Attribute("Fluor")?.Value,
                IlluminationType     = ch.Attribute("IlluminationType")?.Value,
                AcquisitionMode      = ch.Attribute("AcquisitionMode")?.Value,
                ContrastMethod       = ch.Attribute("ContrastMethod")?.Value,
                PinholeSize          = TryParseDouble(ch.Attribute("PinholeSize")?.Value),
                PinholeSizeUnit      = ch.Attribute("PinholeSizeUnit")?.Value,
                SamplesPerPixel      = TryParseInt(ch.Attribute("SamplesPerPixel")?.Value)
            })
            .ToArray();
    }

    // -------------------------------------------------------------------------
    // XML helpers — namespace-agnostic element lookup
    // -------------------------------------------------------------------------

    /// <summary>
    /// Finds child elements by local name, ignoring whatever namespace the
    /// OME-XML happens to declare. This handles the schema version variance
    /// (2013-06, 2016-06, etc.) without needing to know the exact namespace URI.
    /// </summary>
    private static IEnumerable<XElement> FindLocalElements(XElement parent, string localName)
    {
        return parent.Elements().Where(e =>
            e.Name.LocalName.Equals(localName, StringComparison.Ordinal));
    }

    private static XElement? FindLocalElement(XElement parent, string localName)
    {
        return parent.Elements().FirstOrDefault(e =>
            e.Name.LocalName.Equals(localName, StringComparison.Ordinal));
    }

    // -------------------------------------------------------------------------
    // Primitive parsing helpers
    // -------------------------------------------------------------------------

    private static double? TryParseDouble(string? value)
    {
        if (value is null) return null;
        return double.TryParse(value, System.Globalization.CultureInfo.InvariantCulture, out var result)
            ? result
            : null;
    }

    private static int? TryParseInt(string? value)
    {
        if (value is null) return null;
        return int.TryParse(value, System.Globalization.CultureInfo.InvariantCulture, out var result)
            ? result
            : null;
    }
}
