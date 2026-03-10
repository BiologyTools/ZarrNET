using System.Collections.Concurrent;
using System.Net;

namespace ZarrNET.Core.Zarr.Store;

/// <summary>
/// IZarrStore implementation for reading Zarr datasets over HTTP/HTTPS.
/// Supports public S3 buckets, Azure Blob Storage, Google Cloud Storage,
/// and any HTTP server serving Zarr files.
///
/// Caching strategy:
///   - Metadata files (.zarray, .zattrs, .zgroup, zarr.json) are cached in a
///     static ConcurrentDictionary keyed by full URL, shared across all store
///     instances targeting the same base URL. This eliminates redundant HEAD
///     and GET requests when callers create short-lived store instances per
///     tile (e.g. BioImage.OpenURL reopening the dataset on each pan).
///   - Chunk data is cached in a static ChunkLruCache (per base URL) with a
///     256 MB LRU budget, so panning back over previously-visible tiles hits
///     RAM instead of the network.
///   - Existence probes (HEAD results) are cached statically so that the
///     v3-then-v2 probing in ZarrGroup.OpenArrayAsync/OpenRootAsync does
///     not repeat on every open.
///
/// Note: Listing is limited - requires consolidated metadata (.zmetadata)
/// or works only with explicit path navigation.
/// </summary>
public sealed class HttpZarrStore : IZarrStore
{
    private HttpClient _httpClient;
    private readonly string _baseUrl;
    private readonly bool _ownsHttpClient;
    private bool _disposed;

    /// <summary>
    /// Default maximum concurrent connections per server. Higher values
    /// allow Parallel.ForEachAsync in ZarrArray to actually achieve
    /// parallel HTTP fetches rather than queuing behind 2 connections.
    /// </summary>
    public const int DefaultMaxConnectionsPerServer = 48;

    // =====================================================================
    // Static shared caches — keyed by normalised base URL
    // =====================================================================
    //
    // When an external library (e.g. BioLib's BioImage.OpenURL) creates a
    // new OmeZarrReader → HttpZarrStore per tile request, instance-level
    // caches are useless because they die with each store instance. Static
    // caches keyed by base URL let every store that points at the same
    // dataset share a single pool of already-fetched metadata and chunks.

    /// <summary>
    /// Metadata cache shared across all HttpZarrStore instances.
    /// Key = "baseUrl | storeRelativeKey", value = file bytes (null for 404).
    /// Metadata files are small and rarely change, so they live here forever
    /// (bounded only by the number of distinct arrays opened).
    /// </summary>
    private static readonly ConcurrentDictionary<string, byte[]?> s_metadataCache = new();

    /// <summary>
    /// Chunk data caches, one per base URL. Each is a 256 MB LRU cache.
    /// Using ConcurrentDictionary to lazily create one cache per dataset.
    /// </summary>
    private static readonly ConcurrentDictionary<string, ChunkLruCache> s_chunkCaches = new();

    /// <summary>
    /// Existence-probe cache shared across all instances.
    /// Key = "baseUrl | storeRelativeKey", value = true (exists) / false (404).
    /// Prevents repeated HEAD requests for the same v3/v2 probing pattern
    /// that ZarrGroup.OpenArrayAsync and OpenRootAsync perform on every call.
    /// </summary>
    private static readonly ConcurrentDictionary<string, bool> s_existsCache = new();

    public string BaseUrl => _baseUrl;

    // =====================================================================
    // Construction
    // =====================================================================

    /// <summary>
    /// Creates an HTTP Zarr store with a new HttpClient configured for
    /// high-throughput parallel chunk fetching.
    /// The HttpClient will be disposed when the store is disposed.
    /// </summary>
    public HttpZarrStore(string baseUrl)
        : this(baseUrl, CreateDefaultHttpClient(), ownsHttpClient: true)
    {
    }

    /// <summary>
    /// Creates an HTTP Zarr store with a provided HttpClient.
    /// The caller is responsible for disposing the HttpClient.
    ///
    /// For best performance with parallel chunk reads, configure the
    /// underlying SocketsHttpHandler with MaxConnectionsPerServer >= 32.
    /// </summary>
    public HttpZarrStore(string baseUrl, HttpClient httpClient, bool ownsHttpClient = false)
    {
        _baseUrl = baseUrl.TrimEnd('/');
        _httpClient = httpClient;
        _ownsHttpClient = ownsHttpClient;

        // Set reasonable defaults if not already configured
        if (_httpClient.Timeout == TimeSpan.FromSeconds(100))  // Default HttpClient timeout
            _httpClient.Timeout = TimeSpan.FromSeconds(300);  // 5 minutes for large chunks
    }

    /// <summary>
    /// Creates an HttpClient with a SocketsHttpHandler configured for
    /// high-throughput parallel chunk fetching from object stores.
    /// </summary>
    private static HttpClient CreateDefaultHttpClient()
    {
        var handler = new SocketsHttpHandler
        {
            MaxConnectionsPerServer = DefaultMaxConnectionsPerServer,
            PooledConnectionLifetime = TimeSpan.FromMinutes(10),
            EnableMultipleHttp2Connections = true,
        };

        return new HttpClient(handler);
    }

    // =====================================================================
    // IZarrStore — ReadAsync
    // =====================================================================

    public async Task<byte[]?> ReadAsync(string key, CancellationToken ct = default)
    {
        A:
        ThrowIfDisposed();

        var cacheKey = BuildCacheKey(key);

        // Fast path — metadata cache (small files, kept forever)
        if (IsMetadataKey(key) && s_metadataCache.TryGetValue(cacheKey, out var cachedMeta))
            return cachedMeta;

        // Fast path — chunk cache (large files, LRU-evicted)
        if (!IsMetadataKey(key))
        {
            var cachedChunk = GetChunkCache().TryGet(key);
            if (cachedChunk is not null)
                return cachedChunk;
        }

        var url = BuildUrl(key);

        try
        {
            if (_httpClient.BaseAddress == null)
                CreateDefaultHttpClient();
            var response = await _httpClient.GetAsync(url, ct).ConfigureAwait(false);
            System.Diagnostics.Debug.WriteLine(
    $"[HttpZarrStore] GET {url} → {(int)response.StatusCode} {response.StatusCode}");
            if (response.StatusCode == HttpStatusCode.NotFound)
            {
                if (IsMetadataKey(key))
                    s_metadataCache[cacheKey] = null;

                // Record negative existence so ExistsAsync won't HEAD again
                s_existsCache[cacheKey] = false;

                return null;
            }

            response.EnsureSuccessStatusCode();

            var data = await response.Content.ReadAsByteArrayAsync(ct).ConfigureAwait(false);

            // Record positive existence
            s_existsCache[cacheKey] = true;

            if (IsMetadataKey(key))
                s_metadataCache[cacheKey] = data;
            else
                GetChunkCache().Set(key, data);

            return data;
        }
        catch (HttpRequestException ex) when (ex.StatusCode == null)
        {
            _httpClient = CreateDefaultHttpClient();
            goto A;
        }
        catch (TaskCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (HttpRequestException ex) when (ex.StatusCode == HttpStatusCode.RequestTimeout)
        {
           _httpClient = CreateDefaultHttpClient();
            goto A;
        }
        catch (TaskCanceledException ex)
        {
            _httpClient = CreateDefaultHttpClient();
            goto A;
        }
    }

    // =====================================================================
    // IZarrStore — ExistsAsync
    // =====================================================================

    public async Task<bool> ExistsAsync(string key, CancellationToken ct = default)
    {
        ThrowIfDisposed();

        var cacheKey = BuildCacheKey(key);

        // Check the shared existence cache first — covers both metadata and chunks
        if (s_existsCache.TryGetValue(cacheKey, out var exists))
            return exists;

        // A previous ReadAsync may have populated the metadata cache without
        // an explicit ExistsAsync call. Derive existence from that.
        if (IsMetadataKey(key) && s_metadataCache.TryGetValue(cacheKey, out var cachedMeta))
        {
            var result = cachedMeta is not null;
            s_existsCache[cacheKey] = result;
            return result;
        }

        // A previous ReadAsync may have cached chunk data
        if (!IsMetadataKey(key) && GetChunkCache().TryGet(key) is not null)
        {
            s_existsCache[cacheKey] = true;
            return true;
        }

        var url = BuildUrl(key);

        try
        {
            // Try HEAD first — fast and lightweight
            using var headRequest = new HttpRequestMessage(HttpMethod.Head, url);
            var headResponse = await _httpClient.SendAsync(headRequest, ct).ConfigureAwait(false);

            if (headResponse.IsSuccessStatusCode)
            {
                s_existsCache[cacheKey] = true;
                return true;
            }

            if (headResponse.StatusCode == HttpStatusCode.NotFound)
            {
                s_existsCache[cacheKey] = false;
                return false;
            }

            // HEAD returned a non-success, non-404 status (e.g. 403, 405).
            // Some S3/Ceph implementations return 403 Forbidden for HEAD on
            // objects that are publicly readable via GET. Fall back to a GET
            // with a minimal range to confirm existence without downloading
            // the whole object.
            if (IsMetadataKey(key))
            {
                // For small metadata files, just try a full GET and cache the result.
                // This avoids an extra round-trip when ReadAsync is called next.
                var data = await ReadAsync(key, ct).ConfigureAwait(false);
                return data is not null;
            }

            // For non-metadata keys, try a range GET to avoid large downloads.
            using var rangeRequest = new HttpRequestMessage(HttpMethod.Get, url);
            rangeRequest.Headers.Range = new System.Net.Http.Headers.RangeHeaderValue(0, 0);
            var rangeResponse = await _httpClient.SendAsync(rangeRequest, ct).ConfigureAwait(false);

            var rangeExists = rangeResponse.IsSuccessStatusCode
                || rangeResponse.StatusCode == HttpStatusCode.PartialContent;

            s_existsCache[cacheKey] = rangeExists;
            return rangeExists;
        }
        catch (HttpRequestException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
        {
            s_existsCache[cacheKey] = false;
            return false;
        }
        catch (TaskCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (TaskCanceledException ex)
        {
            throw new TimeoutException(
                $"HTTP request timed out while checking existence of '{key}' at {_baseUrl}", ex);
        }
    }

    // =====================================================================
    // IZarrStore — Write / List / Delete (unchanged)
    // =====================================================================

    public async Task WriteAsync(string key, byte[] data, CancellationToken ct = default)
    {
        ThrowIfDisposed();

        throw new NotSupportedException(
            "Writing to HTTP Zarr stores is not supported. " +
            "HTTP stores are read-only. Use LocalFileSystemStore for write operations.");
    }

    public async Task<IReadOnlyList<string>> ListAsync(string prefix = "", CancellationToken ct = default)
    {
        ThrowIfDisposed();

        throw new NotSupportedException(
            "Listing keys in HTTP Zarr stores is not supported. " +
            "HTTP/S3 stores require explicit path navigation. " +
            "Use HasChildAsync() to check for specific children, or enable consolidated metadata.");
    }

    public Task DeleteAsync(string key, CancellationToken ct = default)
    {
        ThrowIfDisposed();

        throw new NotSupportedException(
            "Deleting from HTTP Zarr stores is not supported. " +
            "HTTP stores are read-only.");
    }

    // =====================================================================
    // Cache key helpers
    // =====================================================================

    /// <summary>
    /// Builds a globally-unique cache key by combining the base URL with the
    /// store-relative key. This ensures stores pointing at different datasets
    /// don't collide in the shared static caches.
    /// </summary>
    private string BuildCacheKey(string storeRelativeKey)
        => string.Concat(_baseUrl, " | ", storeRelativeKey);

    /// <summary>
    /// Returns the shared chunk LRU cache for this store's base URL.
    /// Creates one on first access (one 256 MB budget per distinct dataset).
    /// </summary>
    private ChunkLruCache GetChunkCache()
        => s_chunkCaches.GetOrAdd(_baseUrl, _ => new ChunkLruCache());

    // =====================================================================
    // URL / key helpers
    // =====================================================================

    private string BuildUrl(string key)
    {
        // URL-encode the key components to handle special characters
        var parts = key.Split('/');
        var encodedParts = parts.Select(Uri.EscapeDataString);
        var encodedKey = string.Join("/", encodedParts);

        return $"{_baseUrl}/{encodedKey}";
    }

    private static bool IsMetadataKey(string key)
    {
        return key.EndsWith("zarr.json", StringComparison.OrdinalIgnoreCase)
            || key.EndsWith(".zarray", StringComparison.OrdinalIgnoreCase)
            || key.EndsWith(".zgroup", StringComparison.OrdinalIgnoreCase)
            || key.EndsWith(".zattrs", StringComparison.OrdinalIgnoreCase)
            || key.EndsWith(".zmetadata", StringComparison.OrdinalIgnoreCase);
    }

    // =====================================================================
    // Disposal
    // =====================================================================

    public ValueTask DisposeAsync()
    {
        if (_disposed)
            return ValueTask.CompletedTask;

        _disposed = true;

        if (_ownsHttpClient)
            _httpClient?.Dispose();

        // Static caches are intentionally NOT cleared on dispose — they
        // outlive individual store instances so the next store that opens
        // the same URL benefits from already-fetched data.

        return ValueTask.CompletedTask;
    }

    private void ThrowIfDisposed()
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(HttpZarrStore));
    }
}
