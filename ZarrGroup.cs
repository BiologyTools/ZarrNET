using ZarrNET;
using ZarrNET.Core.Zarr.Store;

namespace ZarrNET.Core.Zarr;

/// <summary>
/// Represents a Zarr v3 group node. Provides navigation to child groups
/// and arrays by reading zarr.json entries from the store.
/// 
/// Groups are lazy — children are not loaded until requested.
/// No OME-Zarr knowledge here.
/// </summary>
public sealed class ZarrGroup
{
    private readonly IZarrStore _store;
    private readonly string     _groupPath;   // store-relative path, empty string for root

    public ZarrGroupMetadata Metadata  { get; }
    public string            GroupPath => _groupPath;

    internal ZarrGroup(IZarrStore store, string groupPath, ZarrGroupMetadata metadata)
    {
        _store     = store;
        _groupPath = groupPath.Trim('/');
        Metadata   = metadata;
    }

    // -------------------------------------------------------------------------
    // Public API
    // -------------------------------------------------------------------------

    /// <summary>Opens a child group at the given relative path.</summary>
    public async Task<ZarrGroup> OpenGroupAsync(string relativePath, CancellationToken ct = default)
    {
        var childPath = BuildChildPath(relativePath);

        // Try v3 first
        var v3Key = $"{childPath}/zarr.json";
        if (await _store.ExistsAsync(v3Key, ct).ConfigureAwait(false))
        {
            var doc = await ReadZarrJsonAsync(childPath, ct).ConfigureAwait(false);

            if (doc.NodeType != "group")
                throw new InvalidOperationException(
                    $"Node at '{childPath}' has node_type '{doc.NodeType}', expected 'group'.");

            return new ZarrGroup(_store, childPath, ZarrGroupMetadata.FromDocument(doc));
        }

        // Fall back to v2
        var v2GroupKey = $"{childPath}/.zgroup";
        if (await _store.ExistsAsync(v2GroupKey, ct).ConfigureAwait(false))
        {
            var attributes = await TryReadV2AttrsAsync(childPath, ct).ConfigureAwait(false);
            return new ZarrGroup(_store, childPath, ZarrGroupMetadata.FromV2Document(attributes));
        }

        throw new FileNotFoundException(
            $"Group not found at '{childPath}'. Neither zarr.json nor .zgroup exists.");
    }

    /// <summary>Opens a child array at the given relative path.</summary>
    public async Task<ZarrArray> OpenArrayAsync(string relativePath, CancellationToken ct = default)
    {
        var childPath = BuildChildPath(relativePath);

        // Try v3 first
        var v3Key = $"{childPath}/zarr.json";
        if (await _store.ExistsAsync(v3Key, ct).ConfigureAwait(false))
        {
            var doc = await ReadZarrJsonAsync(childPath, ct).ConfigureAwait(false);

            if (doc.NodeType != "array")
                throw new InvalidOperationException(
                    $"Node at '{childPath}' has node_type '{doc.NodeType}', expected 'array'.");

            var metadata = ZarrArrayMetadata.FromDocument(doc);
            return new ZarrArray(_store, childPath, metadata);
        }

        // Fall back to v2
        var v2ArrayKey = $"{childPath}/.zarray";
        if (await _store.ExistsAsync(v2ArrayKey, ct).ConfigureAwait(false))
        {
            var arrayDoc   = await ReadV2ArrayAsync(childPath, ct).ConfigureAwait(false);
            var attributes = await TryReadV2AttrsAsync(childPath, ct).ConfigureAwait(false);

            // When .zarray omits dimension_separator we must probe the store
            // to discover whether chunks use "/" (nested) or "." (flat) keys.
            // bioformats2raw and many IDR datasets use "/" but predate the field.
            var separatorOverride = arrayDoc.DimensionSeparator is null
                ? await ProbeChunkKeySeparatorAsync(childPath, arrayDoc.Shape.Length, ct)
                    .ConfigureAwait(false)
                : null;

            var metadata = ZarrArrayMetadata.FromV2Document(
                arrayDoc, attributes, separatorOverride);

            return new ZarrArray(_store, childPath, metadata);
        }

        throw new FileNotFoundException(
            $"Array not found at '{childPath}'. Neither zarr.json nor .zarray exists.");
    }

    /// <summary>
    /// Lists the names of all immediate children of this group.
    /// Inspects the store for zarr.json (v3) or .zarray/.zgroup (v2) entries
    /// one directory level down.
    ///
    /// Note: For HTTP/S3 stores that don't support listing, this will return
    /// an empty list. Use HasChildAsync() to check for specific children by name.
    /// </summary>
    public async Task<IReadOnlyList<string>> ListChildNamesAsync(CancellationToken ct = default)
    {
        var prefix = string.IsNullOrEmpty(_groupPath)
            ? string.Empty
            : _groupPath + "/";

        try
        {
            var allKeys = await _store.ListAsync(prefix, ct).ConfigureAwait(false);

            var childNames = allKeys
                .Where(k => k.StartsWith(prefix, StringComparison.Ordinal))
                .Select(k => k[prefix.Length..])               // strip prefix
                .Where(relative => !relative.Contains('/') ||  // direct child file
                                   IsDirectChildMetadataFile(relative))
                .Where(relative => IsMetadataFile(relative))
                .Select(relative => ExtractChildName(relative))
                .Where(name => !string.IsNullOrEmpty(name))
                .Distinct()
                .OrderBy(name => name)
                .ToList();

            return childNames;
        }
        catch (NotSupportedException)
        {
            // Store doesn't support listing (e.g., HTTP store)
            // Return empty list - users must navigate via explicit paths
            return Array.Empty<string>();
        }
    }

    /// <summary>
    /// Returns true if this group has a child node (group or array) at the given name.
    /// Checks for both v3 (zarr.json) and v2 (.zarray/.zgroup) metadata files.
    /// </summary>
    public async Task<bool> HasChildAsync(string name, CancellationToken ct = default)
    {
        var childPath = BuildChildPath(name);

        // Check v3
        var v3Key = $"{childPath}/zarr.json";
        if (await _store.ExistsAsync(v3Key, ct).ConfigureAwait(false))
            return true;

        // Check v2
        var v2ArrayKey = $"{childPath}/.zarray";
        if (await _store.ExistsAsync(v2ArrayKey, ct).ConfigureAwait(false))
            return true;

        var v2GroupKey = $"{childPath}/.zgroup";
        if (await _store.ExistsAsync(v2GroupKey, ct).ConfigureAwait(false))
            return true;

        return false;
    }

    // -------------------------------------------------------------------------
    // Factory
    // -------------------------------------------------------------------------

    /// <summary>Opens the root group of a Zarr store.</summary>
    public static async Task<ZarrGroup> OpenRootAsync(IZarrStore store, CancellationToken ct = default)
    {
        // Try v3 first
        if (await store.ExistsAsync("zarr.json", ct).ConfigureAwait(false))
        {
            var doc = await ReadZarrJsonFromStoreAsync(store, string.Empty, ct).ConfigureAwait(false);

            if (doc.NodeType != "group")
                throw new InvalidOperationException(
                    $"Root node has node_type '{doc.NodeType}', expected 'group'.");

            return new ZarrGroup(store, string.Empty, ZarrGroupMetadata.FromDocument(doc));
        }

        // Fall back to v2
        if (await store.ExistsAsync(".zgroup", ct).ConfigureAwait(false))
        {
            var attributes = await TryReadV2AttrsFromStoreAsync(store, string.Empty, ct).ConfigureAwait(false);
            return new ZarrGroup(store, string.Empty, ZarrGroupMetadata.FromV2Document(attributes));
        }

        throw new InvalidOperationException(
            "Root node is not a valid Zarr group. Neither 'zarr.json' nor '.zgroup' exists.");
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private string BuildChildPath(string relativePath)
    {
        var rel = relativePath.Trim('/');

        return string.IsNullOrEmpty(_groupPath)
            ? rel
            : $"{_groupPath}/{rel}";
    }

    private async Task<ZarrJsonDocument> ReadZarrJsonAsync(string nodePath, CancellationToken ct)
        => await ReadZarrJsonFromStoreAsync(_store, nodePath, ct).ConfigureAwait(false);

    private static async Task<ZarrJsonDocument> ReadZarrJsonFromStoreAsync(
        IZarrStore    store,
        string        nodePath,
        CancellationToken ct)
    {
        var key   = string.IsNullOrEmpty(nodePath) ? "zarr.json" : $"{nodePath}/zarr.json";
        var bytes = await store.ReadAsync(key, ct).ConfigureAwait(false);

        if (bytes is null)
            throw new FileNotFoundException(
                $"zarr.json not found for node at '{nodePath}'. Key: '{key}'");

        return ZarrJsonDocument.Parse(bytes);
    }

    // -------------------------------------------------------------------------
    // Zarr v2 file readers
    // -------------------------------------------------------------------------

    private async Task<ZarrV2ArrayDocument> ReadV2ArrayAsync(string nodePath, CancellationToken ct)
        => await ReadV2ArrayFromStoreAsync(_store, nodePath, ct).ConfigureAwait(false);

    private static async Task<ZarrV2ArrayDocument> ReadV2ArrayFromStoreAsync(
        IZarrStore        store,
        string            nodePath,
        CancellationToken ct)
    {
        var key   = string.IsNullOrEmpty(nodePath) ? ".zarray" : $"{nodePath}/.zarray";
        var bytes = await store.ReadAsync(key, ct).ConfigureAwait(false);

        if (bytes is null)
            throw new FileNotFoundException(
                $".zarray not found for node at '{nodePath}'. Key: '{key}'");

        return ZarrV2ArrayDocument.Parse(bytes);
    }

    private async Task<System.Text.Json.JsonElement?> TryReadV2AttrsAsync(
        string            nodePath,
        CancellationToken ct)
        => await TryReadV2AttrsFromStoreAsync(_store, nodePath, ct).ConfigureAwait(false);

    private static async Task<System.Text.Json.JsonElement?> TryReadV2AttrsFromStoreAsync(
        IZarrStore        store,
        string            nodePath,
        CancellationToken ct)
    {
        var key   = string.IsNullOrEmpty(nodePath) ? ".zattrs" : $"{nodePath}/.zattrs";
        var bytes = await store.ReadAsync(key, ct).ConfigureAwait(false);

        if (bytes is null)
            return null;

        var attrsDoc = ZarrV2AttrsDocument.Parse(bytes);
        return attrsDoc.Root;
    }

    // -------------------------------------------------------------------------
    // Chunk key separator probing (Zarr v2)
    // -------------------------------------------------------------------------

    /// <summary>
    /// When a v2 .zarray omits the dimension_separator field, the spec
    /// default is "." but many writers (bioformats2raw, older napari, etc.)
    /// use "/" without declaring it. This method probes the store by
    /// checking whether the chunk-zero key exists with "/" separators.
    ///
    /// Returns "/" if the slash-separated chunk-zero exists, null otherwise
    /// (letting the caller fall back to the "." default).
    ///
    /// Only adds a single ExistsAsync call, which is a HEAD request for
    /// HTTP stores — cheap and bounded.
    /// </summary>
    private async Task<string?> ProbeChunkKeySeparatorAsync(
        string            arrayPath,
        int               rank,
        CancellationToken ct)
    {
        // Chunk zero with "/" separator: arrayPath/0/0/0/0/0
        var slashChunkZero = arrayPath + "/" + string.Join("/", Enumerable.Repeat("0", rank));

        if (await _store.ExistsAsync(slashChunkZero, ct).ConfigureAwait(false))
            return "/";

        // Slash key not found — the "." default is likely correct,
        // or the array is empty (all chunks are virtual fill-value).
        // Either way, returning null lets the caller use the spec default.
        return null;
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static bool IsMetadataFile(string relativeKey)
    {
        // v3: zarr.json, or name/zarr.json
        if (relativeKey == "zarr.json" || relativeKey.EndsWith("/zarr.json", StringComparison.Ordinal))
            return true;

        // v2: .zarray, .zgroup, .zattrs, or name/.zarray, name/.zgroup
        if (relativeKey == ".zarray" || relativeKey.EndsWith("/.zarray", StringComparison.Ordinal))
            return true;

        if (relativeKey == ".zgroup" || relativeKey.EndsWith("/.zgroup", StringComparison.Ordinal))
            return true;

        return false;
    }

    private static bool IsDirectChildMetadataFile(string relativeKey)
    {
        // Matches exactly "name/zarr.json", "name/.zarray", or "name/.zgroup"
        // — one path segment before the metadata filename
        var parts = relativeKey.Split('/');
        return parts.Length == 2 && (
            parts[1] == "zarr.json" ||
            parts[1] == ".zarray" ||
            parts[1] == ".zgroup");
    }

    private static string ExtractChildName(string relativeKey)
    {
        // v3: "zarr.json" → "" (root), "name/zarr.json" → "name"
        if (relativeKey == "zarr.json")
            return string.Empty;

        if (relativeKey.EndsWith("/zarr.json", StringComparison.Ordinal))
            return relativeKey[..relativeKey.LastIndexOf('/')];

        // v2: ".zarray" → "" (root), "name/.zarray" → "name"
        if (relativeKey == ".zarray" || relativeKey == ".zgroup")
            return string.Empty;

        if (relativeKey.EndsWith("/.zarray", StringComparison.Ordinal) ||
            relativeKey.EndsWith("/.zgroup", StringComparison.Ordinal))
            return relativeKey[..relativeKey.LastIndexOf('/')];

        return string.Empty;
    }
}
