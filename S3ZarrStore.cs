using System.Collections.Concurrent;
using System.Net;
using Amazon;
using Amazon.Runtime;
using Amazon.Runtime.CredentialManagement;
using Amazon.S3;
using Amazon.S3.Model;

namespace ZarrNET.Core.Zarr.Store;

/// <summary>
/// IZarrStore implementation backed by the AWS S3 SDK.
///
/// Works with both AWS S3 and S3-compatible endpoints (MinIO, Ceph, EBI
/// Embassy Cloud, etc.). This provides:
///   - Real ListObjectsV2 (solves the listing gap in HttpZarrStore)
///   - Full read/write/delete support
///   - Credential-based auth (IAM roles, profiles, access keys, SSO, etc.)
///   - Anonymous access for public buckets
///   - High-throughput parallel chunk fetching via custom HttpClientFactory
///
/// Caching strategy mirrors HttpZarrStore:
///   - Metadata: static ConcurrentDictionary keyed by "baseUri | key"
///   - Chunks: static ChunkLruCache per dataset (256 MB LRU budget)
///   - Both survive store disposal for short-lived instance reuse patterns.
/// </summary>
public sealed class S3ZarrStore : IZarrStore
{
    private readonly IAmazonS3 _s3Client;
    private readonly string    _bucketName;
    private readonly string    _keyPrefix;
    private readonly string    _cacheBaseKey;
    private readonly bool      _ownsClient;
    private bool               _disposed;

    public string BucketName => _bucketName;
    public string KeyPrefix  => _keyPrefix;

    /// <summary>
    /// Maximum concurrent connections per server. Matches HttpZarrStore so
    /// that Parallel.ForEachAsync in ZarrArray can saturate the network.
    /// </summary>
    public const int DefaultMaxConnectionsPerServer = 48;

    // =========================================================================
    // Static shared caches — keyed by normalised base URI
    // =========================================================================

    private static readonly ConcurrentDictionary<string, byte[]?> s_metadataCache = new();
    private static readonly ConcurrentDictionary<string, ChunkLruCache> s_chunkCaches = new();

    // =========================================================================
    // Construction
    // =========================================================================

    /// <summary>
    /// Creates an S3 Zarr store from an s3:// URI targeting AWS S3.
    /// Falls back to anonymous credentials if no AWS config is found.
    /// Region is auto-detected via the x-amz-bucket-region header.
    /// </summary>
    public S3ZarrStore(string s3Uri)
        : this(s3Uri, CreateDefaultAwsClient(ParseS3Uri(s3Uri).Bucket), ownsClient: true)
    {
    }

    /// <summary>
    /// Creates an S3 Zarr store targeting a custom S3-compatible endpoint
    /// (EBI Embassy Cloud, MinIO, Ceph, Wasabi, etc.).
    /// </summary>
    public S3ZarrStore(string s3Uri, string serviceUrl)
        : this(s3Uri, CreateCustomEndpointClient(serviceUrl), ownsClient: true)
    {
    }

    /// <summary>
    /// Creates an S3 Zarr store targeting AWS S3 with an explicit region.
    /// </summary>
    public S3ZarrStore(string s3Uri, RegionEndpoint region)
        : this(s3Uri, CreateDefaultAwsClient(bucketName: null, explicitRegion: region), ownsClient: true)
    {
    }

    /// <summary>
    /// Creates an S3 Zarr store with explicit credentials and region.
    /// </summary>
    public S3ZarrStore(string s3Uri, string accessKeyId, string secretAccessKey, RegionEndpoint region)
        : this(s3Uri, CreateAuthenticatedAwsClient(accessKeyId, secretAccessKey, region), ownsClient: true)
    {
    }

    /// <summary>
    /// Creates an S3 Zarr store with a caller-provided IAmazonS3 client.
    /// </summary>
    public S3ZarrStore(string s3Uri, IAmazonS3 s3Client, bool ownsClient = false)
    {
        var (bucket, prefix) = ParseS3Uri(s3Uri);

        _s3Client    = s3Client;
        _bucketName  = bucket;
        _keyPrefix   = prefix;
        _ownsClient  = ownsClient;
        _cacheBaseKey = $"s3://{_bucketName}/{_keyPrefix}";
    }

    // =========================================================================
    // Client factory — high-throughput connection pooling
    // =========================================================================
    //
    // The AWS SDK manages its own internal HttpClient. By default its connection
    // pool is too small for ZarrArray's Parallel.ForEachAsync (16 concurrent
    // chunk reads). We inject a SocketsHttpHandler with the same connection
    // limits that HttpZarrStore uses. This is done via HttpClientFactory on the
    // AmazonS3Config — the only reliable way to control the SDK's HTTP layer.

    /// <summary>
    /// HttpClientFactory that provides a SocketsHttpHandler configured for
    /// high-throughput parallel chunk fetching. Without this, the SDK's
    /// default handler pools too few connections and ZarrArray's parallel
    /// reads queue behind a bottleneck, causing timeouts.
    /// </summary>
    private sealed class HighThroughputHttpClientFactory : HttpClientFactory
    {
        public static readonly HighThroughputHttpClientFactory Instance = new();

        public override HttpClient CreateHttpClient(IClientConfig clientConfig)
        {
            var handler = new SocketsHttpHandler
            {
                MaxConnectionsPerServer       = DefaultMaxConnectionsPerServer,
                PooledConnectionLifetime      = TimeSpan.FromMinutes(10),
                EnableMultipleHttp2Connections = true,
                ConnectTimeout                = TimeSpan.FromSeconds(30),
            };

            return new HttpClient(handler)
            {
                Timeout = clientConfig.Timeout ?? TimeSpan.FromMinutes(5),
            };
        }
    }

    private static AmazonS3Config CreateBaseConfig()
    {
        return new AmazonS3Config
        {
            ForcePathStyle   = true,
            Timeout          = TimeSpan.FromMinutes(5),
            HttpClientFactory = HighThroughputHttpClientFactory.Instance,
        };
    }

    private static IAmazonS3 CreateDefaultAwsClient(
        string?         bucketName     = null,
        RegionEndpoint? explicitRegion = null)
    {
        var region      = explicitRegion ?? ResolveBucketRegion(bucketName);
        var credentials = ResolveCredentialsOrAnonymous();

        var config = CreateBaseConfig();
        config.RegionEndpoint        = region;
        config.UseAccelerateEndpoint = false;

        return new AmazonS3Client(credentials, config);
    }

    private static IAmazonS3 CreateCustomEndpointClient(string serviceUrl)
    {
        var credentials = ResolveCredentialsOrAnonymous();

        var config = CreateBaseConfig();
        config.ServiceURL           = serviceUrl.TrimEnd('/');
        config.AuthenticationRegion = "us-east-1";

        return new AmazonS3Client(credentials, config);
    }

    private static IAmazonS3 CreateAuthenticatedAwsClient(
        string accessKeyId, string secretAccessKey, RegionEndpoint region)
    {
        var credentials = new BasicAWSCredentials(accessKeyId, secretAccessKey);

        var config = CreateBaseConfig();
        config.RegionEndpoint = region;

        return new AmazonS3Client(credentials, config);
    }

    // =========================================================================
    // Credential resolution
    // =========================================================================

    private static AWSCredentials ResolveCredentialsOrAnonymous()
    {
        var accessKey = Environment.GetEnvironmentVariable("AWS_ACCESS_KEY_ID");
        var secretKey = Environment.GetEnvironmentVariable("AWS_SECRET_ACCESS_KEY");

        if (!string.IsNullOrWhiteSpace(accessKey) && !string.IsNullOrWhiteSpace(secretKey))
        {
            var sessionToken = Environment.GetEnvironmentVariable("AWS_SESSION_TOKEN");

            return string.IsNullOrWhiteSpace(sessionToken)
                ? new BasicAWSCredentials(accessKey, secretKey)
                : new SessionAWSCredentials(accessKey, secretKey, sessionToken);
        }

        try
        {
            var chain = new CredentialProfileStoreChain();
            if (chain.TryGetAWSCredentials("default", out var profileCredentials))
                return profileCredentials;
        }
        catch { }

        return new AnonymousAWSCredentials();
    }

    // =========================================================================
    // Region resolution
    // =========================================================================

    private static RegionEndpoint ResolveBucketRegion(string? bucketName)
    {
        var envRegion = Environment.GetEnvironmentVariable("AWS_REGION")
                     ?? Environment.GetEnvironmentVariable("AWS_DEFAULT_REGION");

        if (!string.IsNullOrWhiteSpace(envRegion))
        {
            var endpoint = RegionEndpoint.GetBySystemName(envRegion);
            if (endpoint != null)
                return endpoint;
        }

        try
        {
            var fallback = FallbackRegionFactory.GetRegionEndpoint();
            if (fallback != null)
                return fallback;
        }
        catch { }

        if (!string.IsNullOrWhiteSpace(bucketName))
        {
            var detected = ProbeBucketRegion(bucketName);
            if (detected != null)
                return detected;
        }

        return RegionEndpoint.USEast1;
    }

    private static RegionEndpoint? ProbeBucketRegion(string bucketName)
    {
        try
        {
            using var httpClient = new HttpClient { Timeout = TimeSpan.FromSeconds(10) };
            var probeUrl = $"https://{bucketName}.s3.amazonaws.com";
            using var request  = new HttpRequestMessage(HttpMethod.Head, probeUrl);
            var response = httpClient.Send(request, HttpCompletionOption.ResponseHeadersRead);

            if (response.Headers.TryGetValues("x-amz-bucket-region", out var values))
            {
                var regionName = values.FirstOrDefault();
                if (!string.IsNullOrWhiteSpace(regionName))
                    return RegionEndpoint.GetBySystemName(regionName);
            }
        }
        catch { }

        return null;
    }

    // =========================================================================
    // IZarrStore — Read  (hot path — optimised for parallel chunk fetching)
    // =========================================================================
    int retry = 0;
    public async Task<byte[]?> ReadAsync(string key, CancellationToken ct = default)
    {
    start:
        ThrowIfDisposed();

        var isMetadata = IsMetadataKey(key);
        var cacheKey   = BuildCacheKey(key);

        // Check static caches before hitting the network
        if (isMetadata)
        {
            if (s_metadataCache.TryGetValue(cacheKey, out var cachedMeta))
                return cachedMeta;
        }
        else
        {
            var cachedChunk = GetChunkCache().TryGet(cacheKey);
            if (cachedChunk != null)
                return cachedChunk;
        }

        // Network fetch
        var s3Key = BuildS3Key(key);

        try
        {
            var request = new GetObjectRequest
            {
                BucketName = _bucketName,
                Key        = s3Key
            };

            using var response = await _s3Client.GetObjectAsync(request, ct).ConfigureAwait(false);

            // Read the response stream into a byte array. Pre-allocate from
            // ContentLength when available to avoid MemoryStream resizing.
            byte[] data;
            var contentLength = response.ContentLength;

            if (contentLength > 0)
            {
                data = new byte[contentLength];
                int offset = 0;
                int remaining = (int)contentLength;
                while (remaining > 0)
                {
                    int read = await response.ResponseStream.ReadAsync(
                        data.AsMemory(offset, remaining), ct).ConfigureAwait(false);
                    if (read == 0) break;
                    offset    += read;
                    remaining -= read;
                }
            }
            else
            {
                using var ms = new MemoryStream();
                await response.ResponseStream.CopyToAsync(ms, ct).ConfigureAwait(false);
                data = ms.ToArray();
            }

            // Cache the result
            if (isMetadata)
                s_metadataCache[cacheKey] = data;
            else
                GetChunkCache().Set(cacheKey, data);

            return data;
        }
        catch (AmazonS3Exception ex) when (IsNotFound(ex))
        {
            if (isMetadata)
                s_metadataCache[cacheKey] = null;
            return null;
        }
        catch (AmazonS3Exception ex) when (ex.StatusCode == HttpStatusCode.Forbidden)
        {
            if (isMetadata)
                s_metadataCache[cacheKey] = null;
            return null;
        }
        catch (Exception ex)
        {
            CreateDefaultAwsClient();
            if (retry < 3)
                return null;
            retry++;
            goto start;
        }
        
    }

    // =========================================================================
    // IZarrStore — Write
    // =========================================================================

    public async Task WriteAsync(string key, byte[] data, CancellationToken ct = default)
    {
        ThrowIfDisposed();

        var s3Key = BuildS3Key(key);

        var request = new PutObjectRequest
        {
            BucketName  = _bucketName,
            Key         = s3Key,
            InputStream = new MemoryStream(data)
        };

        await _s3Client.PutObjectAsync(request, ct).ConfigureAwait(false);

        // Update caches
        var cacheKey = BuildCacheKey(key);
        if (IsMetadataKey(key))
            s_metadataCache[cacheKey] = data;
        else
            GetChunkCache().Set(cacheKey, data);
    }

    // =========================================================================
    // IZarrStore — Exists
    // =========================================================================

    public async Task<bool> ExistsAsync(string key, CancellationToken ct = default)
    {
        ThrowIfDisposed();

        var isMetadata = IsMetadataKey(key);

        // Check static caches
        if (isMetadata)
        {
            var cacheKey = BuildCacheKey(key);
            if (s_metadataCache.TryGetValue(cacheKey, out var cached))
                return cached is not null;
        }

        // Skip HEAD — unreliable across S3-compatible endpoints with
        // anonymous credentials. For metadata, ReadAsync caches the result.
        if (isMetadata)
        {
            var data = await ReadAsync(key, ct).ConfigureAwait(false);
            return data is not null;
        }

        return await ProbeExistenceViaRangeGet(BuildS3Key(key), ct).ConfigureAwait(false);
    }

    private async Task<bool> ProbeExistenceViaRangeGet(string s3Key, CancellationToken ct)
    {
        try
        {
            var request = new GetObjectRequest
            {
                BucketName = _bucketName,
                Key        = s3Key,
                ByteRange  = new ByteRange(0, 0)
            };

            using var response = await _s3Client.GetObjectAsync(request, ct).ConfigureAwait(false);
            return true;
        }
        catch (AmazonS3Exception)
        {
            return false;
        }
    }

    // =========================================================================
    // IZarrStore — List
    // =========================================================================

    public async Task<IReadOnlyList<string>> ListAsync(string prefix = "", CancellationToken ct = default)
    {
        ThrowIfDisposed();

        var s3Prefix = BuildS3Key(prefix);

        if (s3Prefix.Length > 0 && !s3Prefix.EndsWith('/'))
            s3Prefix += '/';

        var results          = new List<string>();
        string? continuation = null;

        do
        {
            var request = new ListObjectsV2Request
            {
                BucketName        = _bucketName,
                Prefix            = s3Prefix,
                ContinuationToken = continuation
            };

            var response = await _s3Client.ListObjectsV2Async(request, ct).ConfigureAwait(false);

            foreach (var obj in response.S3Objects)
            {
                var relativeKey = StripPrefix(obj.Key);
                if (relativeKey.Length > 0)
                    results.Add(relativeKey);
            }

            continuation = response.IsTruncated.Value ? response.NextContinuationToken : null;
        }
        while (continuation is not null);

        return results;
    }

    // =========================================================================
    // IZarrStore — Delete
    // =========================================================================

    public async Task DeleteAsync(string key, CancellationToken ct = default)
    {
        ThrowIfDisposed();

        var s3Key = BuildS3Key(key);

        var request = new DeleteObjectRequest
        {
            BucketName = _bucketName,
            Key        = s3Key
        };

        await _s3Client.DeleteObjectAsync(request, ct).ConfigureAwait(false);

        s_metadataCache.TryRemove(BuildCacheKey(key), out _);
    }

    // =========================================================================
    // Cache key helpers
    // =========================================================================

    private string BuildCacheKey(string storeRelativeKey)
        => string.Concat(_cacheBaseKey, " | ", storeRelativeKey);

    private ChunkLruCache GetChunkCache()
        => s_chunkCaches.GetOrAdd(_cacheBaseKey, _ => new ChunkLruCache());

    // =========================================================================
    // S3 URI parsing
    // =========================================================================

    private static (string Bucket, string Prefix) ParseS3Uri(string s3Uri)
    {
        if (!s3Uri.StartsWith("s3://", StringComparison.OrdinalIgnoreCase))
            throw new ArgumentException(
                $"Expected an s3:// URI but got: '{s3Uri}'. " +
                $"Use the format s3://bucket-name/optional/prefix.",
                nameof(s3Uri));

        var withoutScheme = s3Uri.Substring("s3://".Length);
        var slashIndex    = withoutScheme.IndexOf('/');

        if (slashIndex < 0)
            return (withoutScheme, string.Empty);

        var bucket = withoutScheme.Substring(0, slashIndex);
        var prefix = withoutScheme.Substring(slashIndex + 1).TrimEnd('/');

        return (bucket, prefix);
    }

    public static bool IsS3Uri(string uri)
        => uri.StartsWith("s3://", StringComparison.OrdinalIgnoreCase);

    public static string ParseBucketName(string s3Uri)
        => ParseS3Uri(s3Uri).Bucket;

    // =========================================================================
    // Key / path helpers
    // =========================================================================

    private string BuildS3Key(string storeRelativeKey)
    {
        if (string.IsNullOrEmpty(_keyPrefix))
            return storeRelativeKey;

        if (string.IsNullOrEmpty(storeRelativeKey))
            return _keyPrefix;

        return $"{_keyPrefix}/{storeRelativeKey}";
    }

    private string StripPrefix(string absoluteS3Key)
    {
        var expectedPrefix = string.IsNullOrEmpty(_keyPrefix)
            ? string.Empty
            : _keyPrefix + "/";

        if (expectedPrefix.Length == 0)
            return absoluteS3Key;

        if (absoluteS3Key.StartsWith(expectedPrefix, StringComparison.Ordinal))
            return absoluteS3Key.Substring(expectedPrefix.Length);

        return absoluteS3Key;
    }

    private static bool IsMetadataKey(string key)
    {
        return key.EndsWith("zarr.json",  StringComparison.OrdinalIgnoreCase)
            || key.EndsWith(".zarray",    StringComparison.OrdinalIgnoreCase)
            || key.EndsWith(".zgroup",    StringComparison.OrdinalIgnoreCase)
            || key.EndsWith(".zattrs",    StringComparison.OrdinalIgnoreCase)
            || key.EndsWith(".zmetadata", StringComparison.OrdinalIgnoreCase);
    }

    private static bool IsNotFound(AmazonS3Exception ex)
    {
        return ex.StatusCode == HttpStatusCode.NotFound
            || string.Equals(ex.ErrorCode, "NoSuchKey",    StringComparison.OrdinalIgnoreCase)
            || string.Equals(ex.ErrorCode, "NotFound",     StringComparison.OrdinalIgnoreCase)
            || string.Equals(ex.ErrorCode, "NoSuchBucket", StringComparison.OrdinalIgnoreCase);
    }

    // =========================================================================
    // Disposal
    // =========================================================================

    public ValueTask DisposeAsync()
    {
        if (_disposed)
            return ValueTask.CompletedTask;

        _disposed = true;

        if (_ownsClient && _s3Client is IDisposable disposable)
            disposable.Dispose();

        return ValueTask.CompletedTask;
    }

    private void ThrowIfDisposed()
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(S3ZarrStore));
    }
}
