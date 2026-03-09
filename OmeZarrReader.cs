using OmeZarr.Core.OmeZarr.Metadata;
using OmeZarr.Core.OmeZarr.Nodes;
using OmeZarr.Core.Zarr;
using OmeZarr.Core.Zarr.Store;

namespace OmeZarr.Core.OmeZarr;

/// <summary>
/// Entry point for reading OME-Zarr datasets.
///
/// Usage:
/// <code>
///   await using var reader = await OmeZarrReader.OpenAsync("/path/to/dataset.zarr");
///
///   // Multiscale image:
///   var image = reader.AsMultiscaleImage();
///   var level = await image.OpenResolutionLevelAsync(datasetIndex: 0);
///   var roi   = new PhysicalROI(origin: [0, 0, 0], size: [100, 512, 512]);
///   var result = await level.ReadRegionAsync(roi);
///
///   // HCS plate:
///   var plate = reader.AsPlate();
///   var well  = await plate.OpenWellAsync("A", "1");
///   var field = await well.OpenFieldAsync(0);
///   var level = await field.OpenResolutionLevelAsync();
/// </code>
/// </summary>
public sealed class OmeZarrReader : IAsyncDisposable
{
    private readonly IZarrStore  _store;
    private readonly ZarrGroup   _rootGroup;
    private readonly System.Text.Json.JsonElement? _omeGroupAttributes;  // only for bioformats2raw
    private readonly Metadata.OmeXmlMetadata?      _omeXml;              // only for bioformats2raw
    private bool                 _disposed;

    public OmeAttributesParser.OmeNodeType RootNodeType { get; }

    /// <summary>
    /// The detected OME-NGFF specification version (e.g. "0.4", "0.5").
    /// Determined from the "ome.version" envelope (0.5+) or from
    /// "multiscales[0].version" (0.4 and earlier).
    /// Null if no version string is present in the metadata.
    /// </summary>
    public string? NgffVersion { get; }

    private OmeZarrReader(
        IZarrStore  store,
        ZarrGroup   rootGroup,
        OmeAttributesParser.OmeNodeType rootNodeType,
        string?     ngffVersion,
        System.Text.Json.JsonElement? omeGroupAttributes = null,
        Metadata.OmeXmlMetadata?      omeXml = null)
    {
        _store                = store;
        _rootGroup            = rootGroup;
        RootNodeType          = rootNodeType;
        NgffVersion           = ngffVersion;
        _omeGroupAttributes   = omeGroupAttributes;
        _omeXml               = omeXml;
    }

    // -------------------------------------------------------------------------
    // Factory
    // -------------------------------------------------------------------------

    /// <summary>
    /// Opens a Zarr store at the given path or URL and detects
    /// the OME-Zarr node type at the root.
    ///
    /// Supports:
    /// - Local filesystem paths (e.g., "C:\data\image.zarr" or "/data/image.zarr")
    /// - HTTP/HTTPS URLs (e.g., "https://example.com/data.zarr")
    /// - S3 URLs via HTTP (e.g., "https://s3.amazonaws.com/bucket/data.zarr")
    /// - S3 native URIs (e.g., "s3://bucket-name/data.zarr") — uses AWS SDK
    ///   with default credential chain or anonymous fallback
    /// - S3-compatible URIs (e.g., "s3://idr/zarr/v0.4/...") — auto-routes
    ///   known buckets to their correct endpoint (EBI Embassy Cloud, etc.)
    /// </summary>
    public static async Task<OmeZarrReader> OpenAsync(
        string            pathOrUrl,
        CancellationToken ct = default)
    {
        IZarrStore store = CreateStore(pathOrUrl);

        try
        {
            var rootGroup   = await ZarrGroup.OpenRootAsync(store, ct).ConfigureAwait(false);
            var attributes  = rootGroup.Metadata.RawAttributes;
            var nodeType    = OmeAttributesParser.DetectNodeType(attributes);
            var ngffVersion = OmeAttributesParser.DetectNgffVersion(attributes);

            // For bioformats2raw layouts, pre-load the OME sub-group attributes
            // and METADATA.ome.xml so the collection metadata is ready at construction time
            System.Text.Json.JsonElement? omeGroupAttributes = null;
            Metadata.OmeXmlMetadata?      omeXml = null;

            if (nodeType == OmeAttributesParser.OmeNodeType.Bioformats2RawCollection)
            {
                (omeGroupAttributes, omeXml) = await ReadBioformats2RawOmeDataAsync(
                    store, rootGroup, ct).ConfigureAwait(false);
            }

            return new OmeZarrReader(store, rootGroup, nodeType, ngffVersion,
                    omeGroupAttributes, omeXml);
        }
        catch
        {
            await store.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    /// <summary>
    /// Opens a Zarr store with a custom IZarrStore implementation.
    /// Useful for testing or custom storage backends.
    /// </summary>
    public static async Task<OmeZarrReader> OpenAsync(
        IZarrStore        store,
        CancellationToken ct = default)
    {
        var rootGroup   = await ZarrGroup.OpenRootAsync(store, ct).ConfigureAwait(false);
        var attributes  = rootGroup.Metadata.RawAttributes;
        var nodeType    = OmeAttributesParser.DetectNodeType(attributes);
        var ngffVersion = OmeAttributesParser.DetectNgffVersion(attributes);

        System.Text.Json.JsonElement? omeGroupAttributes = null;
        Metadata.OmeXmlMetadata?      omeXml = null;

        if (nodeType == OmeAttributesParser.OmeNodeType.Bioformats2RawCollection)
        {
            (omeGroupAttributes, omeXml) = await ReadBioformats2RawOmeDataAsync(
                store, rootGroup, ct).ConfigureAwait(false);
        }

        return new OmeZarrReader(store, rootGroup, nodeType, ngffVersion,
            omeGroupAttributes, omeXml);
    }

    // -------------------------------------------------------------------------
    // Store creation
    // -------------------------------------------------------------------------

    /// <summary>
    /// Well-known S3-compatible endpoints used in the bioimaging community.
    /// Keyed by the bucket name that appears in the s3:// URI convention.
    ///
    /// For example, IDR data is referenced as "s3://idr/zarr/v0.4/..."
    /// but the actual endpoint is https://uk1s3.embassy.ebi.ac.uk.
    /// </summary>
    private static readonly Dictionary<string, string> KnownS3Endpoints = new(StringComparer.OrdinalIgnoreCase)
    {
        ["idr"]  = "https://uk1s3.embassy.ebi.ac.uk",
        ["bia"]  = "https://uk1s3.embassy.ebi.ac.uk",   // BioImage Archive
        ["ukbb"] = "https://uk1s3.embassy.ebi.ac.uk",
    };

    private static IZarrStore CreateStore(string pathOrUrl)
    {
        // s3:// URIs — native S3 SDK store
        if (S3ZarrStore.IsS3Uri(pathOrUrl))
        {
            var bucketName = S3ZarrStore.ParseBucketName(pathOrUrl);

            // Check if this bucket maps to a known S3-compatible endpoint
            if (KnownS3Endpoints.TryGetValue(bucketName, out var serviceUrl))
                return new S3ZarrStore(pathOrUrl, serviceUrl);

            // Default: AWS S3 with region auto-detection
            return new S3ZarrStore(pathOrUrl);
        }

        // http:// and https:// — generic HTTP store
        if (Uri.TryCreate(pathOrUrl, UriKind.Absolute, out var uri))
        {
            if (uri.Scheme == "http" || uri.Scheme == "https")
            {
                return new HttpZarrStore(pathOrUrl);
            }
            else if (uri.Scheme == "file")
            {
                return new LocalFileSystemStore(uri.LocalPath);
            }
        }

        // Treat as local filesystem path
        return new LocalFileSystemStore(pathOrUrl);
    }

    // -------------------------------------------------------------------------
    // Node access — returns a typed node based on the detected root type
    // -------------------------------------------------------------------------

    /// <summary>
    /// Returns the root as a MultiscaleNode.
    /// Throws if the root is not a multiscale image.
    /// </summary>
    public MultiscaleNode AsMultiscaleImage()
    {
        EnsureNodeType(OmeAttributesParser.OmeNodeType.MultiscaleImage, nameof(AsMultiscaleImage));

        var attributes  = RequireAttributes();
        var multiscales = OmeAttributesParser.ParseMultiscales(attributes);

        return new MultiscaleNode(_rootGroup, multiscales);
    }

    /// <summary>
    /// Returns the root as a PlateNode.
    /// Throws if the root is not an HCS plate.
    /// </summary>
    public PlateNode AsPlate()
    {
        EnsureNodeType(OmeAttributesParser.OmeNodeType.Plate, nameof(AsPlate));

        var attributes = RequireAttributes();
        var plateMeta  = OmeAttributesParser.ParsePlate(attributes);

        return new PlateNode(_rootGroup, plateMeta);
    }

    /// <summary>
    /// Returns the root as a WellNode.
    /// Useful when opening a well sub-path directly as the store root.
    /// </summary>
    public WellNode AsWell()
    {
        EnsureNodeType(OmeAttributesParser.OmeNodeType.Well, nameof(AsWell));

        var attributes = RequireAttributes();
        var wellMeta   = OmeAttributesParser.ParseWell(attributes);

        return new WellNode(_rootGroup, wellMeta);
    }

    /// <summary>
    /// Returns the root as a Bioformats2RawCollectionNode.
    /// Throws if the root is not a bioformats2raw.layout wrapper.
    /// </summary>
    public Bioformats2RawCollectionNode AsBioformats2RawCollection()
    {
        EnsureNodeType(OmeAttributesParser.OmeNodeType.Bioformats2RawCollection,
            nameof(AsBioformats2RawCollection));

        var attributes = RequireAttributes();
        var meta = OmeAttributesParser.ParseBioformats2Raw(attributes, _omeGroupAttributes, _omeXml);

        return new Bioformats2RawCollectionNode(_rootGroup, meta);
    }

    /// <summary>
    /// Opens the first multiscale image, automatically unwrapping a
    /// bioformats2raw.layout wrapper if present.
    ///
    /// Use this when you want a MultiscaleNode regardless of whether the
    /// dataset is a direct multiscale image or a bioformats2raw wrapper.
    /// For multi-series bioformats2raw datasets, use AsBioformats2RawCollection()
    /// to navigate individual series explicitly.
    /// </summary>
    public async Task<MultiscaleNode> AsMultiscaleImageAsync(
        CancellationToken ct = default)
    {
        // Direct multiscale — no navigation needed
        if (RootNodeType == OmeAttributesParser.OmeNodeType.MultiscaleImage ||
            RootNodeType == OmeAttributesParser.OmeNodeType.LabelImage)
        {
            return AsMultiscaleImage();
        }

        // Auto-unwrap bioformats2raw single/first series
        if (RootNodeType == OmeAttributesParser.OmeNodeType.Bioformats2RawCollection)
        {
            var collection = AsBioformats2RawCollection();
            return await collection.OpenSeriesAsync(0, ct).ConfigureAwait(false);
        }

        // Everything else is a type mismatch — EnsureNodeType will throw
        EnsureNodeType(OmeAttributesParser.OmeNodeType.MultiscaleImage,
            nameof(AsMultiscaleImageAsync));
        return null!; // unreachable
    }

    /// <summary>
    /// Attempts to determine the root node type and return a general-purpose
    /// navigation entry point without needing to know the type in advance.
    /// Returns one of: MultiscaleNode, PlateNode, WellNode, LabelGroupNode,
    /// Bioformats2RawCollectionNode.
    /// </summary>
    public OmeZarrNode OpenRoot()
    {
        var attributes = _rootGroup.Metadata.RawAttributes;

        return RootNodeType switch
        {
            OmeAttributesParser.OmeNodeType.Plate =>
                new PlateNode(_rootGroup, OmeAttributesParser.ParsePlate(attributes!.Value)),

            OmeAttributesParser.OmeNodeType.Well =>
                new WellNode(_rootGroup, OmeAttributesParser.ParseWell(attributes!.Value)),

            OmeAttributesParser.OmeNodeType.LabelGroup =>
                new LabelGroupNode(_rootGroup, OmeAttributesParser.ParseLabelGroup(attributes!.Value)),

            OmeAttributesParser.OmeNodeType.MultiscaleImage or
            OmeAttributesParser.OmeNodeType.LabelImage =>
                new MultiscaleNode(_rootGroup, OmeAttributesParser.ParseMultiscales(attributes!.Value)),

            OmeAttributesParser.OmeNodeType.Bioformats2RawCollection =>
                new Bioformats2RawCollectionNode(
                    _rootGroup,
                    OmeAttributesParser.ParseBioformats2Raw(
                        attributes!.Value, _omeGroupAttributes, _omeXml)),

            _ => throw new InvalidOperationException(
                $"Cannot determine OME-Zarr node type. " +
                $"Root attributes do not contain a recognised OME-Zarr key " +
                $"(multiscales, plate, well, labels, bioformats2raw.layout). " +
                $"Path: {_rootGroup.GroupPath}")
        };
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /// <summary>
    /// Reads both the OME sub-group attributes (for the series list) and
    /// METADATA.ome.xml (for rich image metadata) in one pass.
    ///
    /// Both are optional per the spec. Failures reading either one do not
    /// prevent the other from being returned — they degrade independently.
    /// </summary>
    private static async Task<(System.Text.Json.JsonElement? OmeGroupAttributes, Metadata.OmeXmlMetadata? OmeXml)>
        ReadBioformats2RawOmeDataAsync(
            IZarrStore        store,
            ZarrGroup         rootGroup,
            CancellationToken ct)
    {
        System.Text.Json.JsonElement? omeGroupAttributes = null;
        Metadata.OmeXmlMetadata?      omeXml = null;

        // Check for the OME sub-group
        bool hasOmeGroup;
        try
        {
            hasOmeGroup = await rootGroup.HasChildAsync("OME", ct).ConfigureAwait(false);
        }
        catch
        {
            return (null, null);
        }

        if (!hasOmeGroup)
            return (null, null);

        // Read OME sub-group attributes (series list)
        try
        {
            var omeGroup = await rootGroup.OpenGroupAsync("OME", ct).ConfigureAwait(false);
            omeGroupAttributes = omeGroup.Metadata.RawAttributes;
        }
        catch
        {
            // OME sub-group attributes are optional — degrade gracefully
        }

        // Read METADATA.ome.xml from the store
        try
        {
            var xmlBytes = await store.ReadAsync("OME/METADATA.ome.xml", ct).ConfigureAwait(false);
            if (xmlBytes is not null)
            {
                omeXml = Metadata.OmeXmlParser.TryParse(xmlBytes);
            }
        }
        catch
        {
            // METADATA.ome.xml is optional — degrade gracefully
        }

        return (omeGroupAttributes, omeXml);
    }

    private void EnsureNodeType(OmeAttributesParser.OmeNodeType expected, string callerName)
    {
        if (RootNodeType != expected)
        {
            var attributesHint = DescribeRawAttributes();

            throw new InvalidOperationException(
                $"{callerName} requires root node type '{expected}', " +
                $"but detected '{RootNodeType}'. " +
                $"NGFF version: {NgffVersion ?? "(none)"}. " +
                $"Root attributes: {attributesHint}. " +
                (RootNodeType == OmeAttributesParser.OmeNodeType.Unknown
                    ? "The root group attributes do not contain any recognised OME-Zarr key " +
                      "(multiscales, plate, well, labels, bioformats2raw.layout) — check whether " +
                      "this is an unsupported metadata format."
                    : RootNodeType == OmeAttributesParser.OmeNodeType.Bioformats2RawCollection
                    ? "This is a bioformats2raw.layout wrapper. Use AsBioformats2RawCollection() " +
                      "to navigate series explicitly, or AsMultiscaleImageAsync() to auto-unwrap " +
                      "the first series."
                    : $"Use OpenRoot() for automatic dispatch, or " +
                      $"use the appropriate accessor (e.g. AsPlate() for plate data)."));
        }
    }

    /// <summary>
    /// Returns a short summary of the root attribute keys for diagnostics.
    /// </summary>
    private string DescribeRawAttributes()
    {
        var attrs = _rootGroup.Metadata.RawAttributes;
        if (attrs is null)
            return "(null — no attributes found)";

        try
        {
            var keys = new List<string>();
            foreach (var prop in attrs.Value.EnumerateObject())
                keys.Add(prop.Name);

            return keys.Count == 0
                ? "(empty object)"
                : $"{{ {string.Join(", ", keys)} }}";
        }
        catch
        {
            return "(unable to enumerate)";
        }
    }

    private System.Text.Json.JsonElement RequireAttributes()
    {
        if (_rootGroup.Metadata.RawAttributes is null)
            throw new InvalidOperationException(
                "Root group has no attributes. " +
                "This does not appear to be a valid OME-Zarr dataset.");

        return _rootGroup.Metadata.RawAttributes.Value;
    }

    // -------------------------------------------------------------------------
    // Disposal
    // -------------------------------------------------------------------------

    public async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;

        _disposed = true;
        await _store.DisposeAsync().ConfigureAwait(false);
    }
}
