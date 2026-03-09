using System.Collections.Concurrent;
using Amazon;
using Amazon.Runtime;
using Amazon.Runtime.CredentialManagement;
using Amazon.S3;
using Amazon.S3.Model;

namespace OmeZarr.Core.Zarr.Store;

/// <summary>
/// IZarrStore implementation backed by the AWS S3 SDK.
///
/// Works with both AWS S3 and S3-compatible endpoints (MinIO, Ceph, EBI
/// Embassy Cloud, etc.). This provides:
///   - Real ListObjectsV2 (solves the listing gap in HttpZarrStore)
///   - Full read/write/delete support
///   - Credential-based auth (IAM roles, profiles, access keys, SSO, etc.)
///   - Anonymous access for public buckets
///   - Proper S3 error handling and retry via the SDK
///
/// URI formats:
///   s3://bucket-name/optional/prefix         → AWS S3 (region auto-detected)
///   https://s3.embassy.ebi.ac.uk/idr/...     → S3-compatible endpoint
///
/// Caching strategy mirrors HttpZarrStore: metadata files (.zarray, .zattrs,
/// .zgroup, zarr.json) are cached in-memory to avoid redundant round-trips
/// during the probe-heavy open sequence (v3-then-v2 fallback in ZarrGroup).
/// </summary>
public sealed class S3ZarrStore : IZarrStore
{
    private readonly IAmazonS3 _s3Client;
    private readonly string    _bucketName;
    private readonly string    _keyPrefix;
    private readonly bool      _ownsClient;
    private bool               _disposed;

    // Metadata cache — small files read many times during open/probe sequences.
    // Keyed by store-relative key. Null value = confirmed 404.
    private readonly ConcurrentDictionary<string, byte[]?> _metadataCache = new();

    public string BucketName => _bucketName;
    public string KeyPrefix  => _keyPrefix;

    // =========================================================================
    // Construction
    // =========================================================================

    /// <summary>
    /// Creates an S3 Zarr store from an s3:// URI targeting AWS S3.
    ///
    /// Credential resolution: checks environment variables and ~/.aws/credentials.
    /// If no credentials are found, falls back to anonymous (unsigned) access.
    ///
    /// Region auto-detection: probes S3's global endpoint to discover the
    /// bucket's region via the x-amz-bucket-region header.
    ///
    /// For S3-compatible endpoints (MinIO, Ceph, EBI Embassy, etc.) use
    /// the overload that accepts a <paramref name="serviceUrl"/>.
    /// </summary>
    public S3ZarrStore(string s3Uri)
        : this(s3Uri, CreateDefaultAwsClient(ParseS3Uri(s3Uri).Bucket), ownsClient: true)
    {
    }

    /// <summary>
    /// Creates an S3 Zarr store targeting a custom S3-compatible endpoint.
    ///
    /// Use this for non-AWS S3 stores such as:
    ///   - EBI Embassy Cloud (https://s3.embassy.ebi.ac.uk)
    ///   - MinIO, Ceph, Wasabi, Backblaze B2, etc.
    ///
    /// The <paramref name="s3Uri"/> provides the bucket name and key prefix
    /// in the standard s3://bucket/prefix format. The <paramref name="serviceUrl"/>
    /// is the endpoint base URL (e.g. "https://s3.embassy.ebi.ac.uk").
    ///
    /// Anonymous credentials are used by default. For authenticated access
    /// to a custom endpoint, use the IAmazonS3 constructor with a pre-
    /// configured client.
    /// </summary>
    public S3ZarrStore(string s3Uri, string serviceUrl)
        : this(s3Uri, CreateCustomEndpointClient(serviceUrl), ownsClient: true)
    {
    }

    /// <summary>
    /// Creates an S3 Zarr store targeting AWS S3 with an explicit region.
    /// Skips region auto-detection.
    /// </summary>
    public S3ZarrStore(string s3Uri, RegionEndpoint region)
        : this(s3Uri, CreateDefaultAwsClient(bucketName: null, explicitRegion: region), ownsClient: true)
    {
    }

    /// <summary>
    /// Creates an S3 Zarr store with explicit credentials and region.
    /// Use this for private AWS buckets.
    /// </summary>
    public S3ZarrStore(string s3Uri, string accessKeyId, string secretAccessKey, RegionEndpoint region)
        : this(s3Uri, CreateAuthenticatedAwsClient(accessKeyId, secretAccessKey, region), ownsClient: true)
    {
    }

    /// <summary>
    /// Creates an S3 Zarr store with a caller-provided IAmazonS3 client.
    /// The caller retains ownership and is responsible for disposal unless
    /// <paramref name="ownsClient"/> is true.
    ///
    /// Use this for full control: advanced credential providers, custom
    /// endpoint configuration, shared clients across multiple stores, etc.
    /// </summary>
    public S3ZarrStore(string s3Uri, IAmazonS3 s3Client, bool ownsClient = false)
    {
        var (bucket, prefix) = ParseS3Uri(s3Uri);

        _s3Client   = s3Client;
        _bucketName = bucket;
        _keyPrefix  = prefix;
        _ownsClient = ownsClient;

        System.Diagnostics.Debug.WriteLine(
            $"[S3ZarrStore] Created: bucket={_bucketName} prefix='{_keyPrefix}' uri={s3Uri}");
    }

    // -------------------------------------------------------------------------
    // Client factory helpers
    // -------------------------------------------------------------------------

    /// <summary>
    /// Creates an S3 client for AWS endpoints with auto-detected region
    /// and anonymous fallback credentials.
    /// </summary>
    private static IAmazonS3 CreateDefaultAwsClient(
        string?          bucketName     = null,
        RegionEndpoint?  explicitRegion = null)
    {
        var region      = explicitRegion ?? ResolveBucketRegion(bucketName);
        var credentials = ResolveCredentialsOrAnonymous();

        var config = new AmazonS3Config
        {
            RegionEndpoint        = region,
            UseAccelerateEndpoint = false,
            ForcePathStyle        = true,
        };

        return new AmazonS3Client(credentials, config);
    }

    /// <summary>
    /// Creates an S3 client for a custom S3-compatible endpoint.
    /// Uses anonymous credentials (suitable for public data stores like
    /// EBI Embassy Cloud). For authenticated custom endpoints, callers
    /// should use the IAmazonS3 constructor with their own client.
    /// </summary>
    private static IAmazonS3 CreateCustomEndpointClient(string serviceUrl)
    {
        var credentials = ResolveCredentialsOrAnonymous();

        var config = new AmazonS3Config
        {
            ServiceURL       = serviceUrl.TrimEnd('/'),
            ForcePathStyle   = true,

            // Custom S3 endpoints don't use AWS auth signing in the same way.
            // We still need a region for the SDK internals, but it's not used
            // for routing. us-east-1 is the conventional placeholder.
            AuthenticationRegion = "us-east-1",
        };

        return new AmazonS3Client(credentials, config);
    }

    private static IAmazonS3 CreateAuthenticatedAwsClient(
        string accessKeyId, string secretAccessKey, RegionEndpoint region)
    {
        var credentials = new BasicAWSCredentials(accessKeyId, secretAccessKey);
        return new AmazonS3Client(credentials, region);
    }

    /// <summary>
    /// Resolves AWS credentials by checking only the providers that resolve
    /// eagerly (environment variables, shared credentials file, web identity
    /// token). Deliberately skips the EC2/ECS instance metadata provider
    /// because it returns a deferred credential object that "succeeds" at
    /// construction time but throws at request time on non-EC2 machines —
    /// an experience indistinguishable from a broken bucket URL.
    ///
    /// If no eager credentials are found, returns AnonymousAWSCredentials
    /// for unsigned public bucket access.
    ///
    /// For EC2/ECS/Lambda environments where instance credentials are needed,
    /// callers should use the IAmazonS3 constructor overload with a pre-
    /// configured client that has the correct credential provider.
    /// </summary>
    private static AWSCredentials ResolveCredentialsOrAnonymous()
    {
        // 1. Environment variables (AWS_ACCESS_KEY_ID + AWS_SECRET_ACCESS_KEY)
        //    This is the most explicit and common CI/scripting path.
        var accessKey = Environment.GetEnvironmentVariable("AWS_ACCESS_KEY_ID");
        var secretKey = Environment.GetEnvironmentVariable("AWS_SECRET_ACCESS_KEY");

        if (!string.IsNullOrWhiteSpace(accessKey) && !string.IsNullOrWhiteSpace(secretKey))
        {
            var sessionToken = Environment.GetEnvironmentVariable("AWS_SESSION_TOKEN");

            return string.IsNullOrWhiteSpace(sessionToken)
                ? new BasicAWSCredentials(accessKey, secretKey)
                : new SessionAWSCredentials(accessKey, secretKey, sessionToken);
        }

        // 2. Shared credentials file (~/.aws/credentials, "default" profile)
        //    This is the standard developer workstation path.
        try
        {
            var chain = new CredentialProfileStoreChain();
            if (chain.TryGetAWSCredentials("default", out var profileCredentials))
                return profileCredentials;
        }
        catch
        {
            // Malformed credentials file — fall through to anonymous.
        }

        // 3. No credentials found — anonymous access for public buckets.
        //    This is the expected path for most OME-Zarr consumers who just
        //    want to read publicly shared imaging data.
        return new AnonymousAWSCredentials();
    }

    /// <summary>
    /// Determines the correct region for the given S3 bucket.
    ///
    /// Strategy:
    ///   1. If the user has AWS_REGION / AWS_DEFAULT_REGION set, use that.
    ///   2. If ~/.aws/config has a region, use that.
    ///   3. Send an anonymous HEAD to https://{bucket}.s3.amazonaws.com and
    ///      read the x-amz-bucket-region response header. This works for any
    ///      public bucket regardless of its region — S3 always includes the
    ///      header even on 301/403 responses.
    ///   4. Fall back to us-east-1 if all else fails.
    /// </summary>
    private static RegionEndpoint ResolveBucketRegion(string? bucketName)
    {
        // Check environment variables first — explicit user intent
        var envRegion = Environment.GetEnvironmentVariable("AWS_REGION")
                     ?? Environment.GetEnvironmentVariable("AWS_DEFAULT_REGION");

        if (!string.IsNullOrWhiteSpace(envRegion))
        {
            var endpoint = RegionEndpoint.GetBySystemName(envRegion);
            if (endpoint != null)
                return endpoint;
        }

        // Check ~/.aws/config
        try
        {
            var fallback = FallbackRegionFactory.GetRegionEndpoint();
            if (fallback != null)
                return fallback;
        }
        catch
        {
            // Malformed config — continue to bucket probe.
        }

        // Probe the bucket's actual region via a HEAD request to the global
        // S3 endpoint. S3 always returns the x-amz-bucket-region header,
        // even for 301 (wrong region) and 403 (access denied) responses.
        if (!string.IsNullOrWhiteSpace(bucketName))
        {
            var detectedRegion = ProbeBucketRegion(bucketName);
            if (detectedRegion != null)
                return detectedRegion;
        }

        return RegionEndpoint.USEast1;
    }

    /// <summary>
    /// Sends an anonymous HEAD request to the S3 global endpoint for the
    /// given bucket and extracts the x-amz-bucket-region header.
    ///
    /// This works without credentials — S3 returns the header on every
    /// response including 301, 400, and 403. The request is fast because
    /// HEAD returns no body.
    /// </summary>
    private static RegionEndpoint? ProbeBucketRegion(string bucketName)
    {
        try
        {
            using var httpClient = new HttpClient { Timeout = TimeSpan.FromSeconds(10) };

            var probeUrl = $"https://{bucketName}.s3.amazonaws.com";
            using var request  = new HttpRequestMessage(HttpMethod.Head, probeUrl);

            // Use SendAsync with HttpCompletionOption.ResponseHeadersRead to
            // avoid reading any body (there shouldn't be one for HEAD anyway).
            var response = httpClient.Send(request, HttpCompletionOption.ResponseHeadersRead);

            if (response.Headers.TryGetValues("x-amz-bucket-region", out var values))
            {
                var regionName = values.FirstOrDefault();
                if (!string.IsNullOrWhiteSpace(regionName))
                    return RegionEndpoint.GetBySystemName(regionName);
            }
        }
        catch
        {
            // Network failure, DNS failure, timeout — fall through.
            // This is a best-effort optimisation, not a hard requirement.
        }

        return null;
    }

    // =========================================================================
    // IZarrStore — Read
    // =========================================================================

    public async Task<byte[]?> ReadAsync(string key, CancellationToken ct = default)
    {
        ThrowIfDisposed();

        // Check metadata cache first
        if (IsMetadataKey(key) && _metadataCache.TryGetValue(key, out var cached))
            return cached;

        var s3Key = BuildS3Key(key);

        try
        {
            var request = new GetObjectRequest
            {
                BucketName = _bucketName,
                Key        = s3Key
            };

            System.Diagnostics.Debug.WriteLine(
                $"[S3ZarrStore] GET s3://{_bucketName}/{s3Key}");

            using var response = await _s3Client.GetObjectAsync(request, ct).ConfigureAwait(false);
            using var ms       = new MemoryStream();

            await response.ResponseStream.CopyToAsync(ms, ct).ConfigureAwait(false);
            var data = ms.ToArray();

            System.Diagnostics.Debug.WriteLine(
                $"[S3ZarrStore] GET s3://{_bucketName}/{s3Key} → {data.Length} bytes");

            if (IsMetadataKey(key))
                _metadataCache[key] = data;

            return data;
        }
        catch (AmazonS3Exception ex) when (IsNotFound(ex))
        {
            System.Diagnostics.Debug.WriteLine(
                $"[S3ZarrStore] GET s3://{_bucketName}/{s3Key} → NOT FOUND ({ex.ErrorCode})");

            if (IsMetadataKey(key))
                _metadataCache[key] = null;

            return null;
        }
        catch (AmazonS3Exception ex) when (ex.StatusCode == System.Net.HttpStatusCode.Forbidden)
        {
            // S3 and Ceph return 403 (not 404) for nonexistent keys when the
            // caller lacks s3:ListBucket permission. With anonymous access this
            // is indistinguishable from "key doesn't exist" vs "access denied".
            // Treat as not-found — if the bucket itself were inaccessible, every
            // request would fail and the caller will see the pattern.
            System.Diagnostics.Debug.WriteLine(
                $"[S3ZarrStore] GET s3://{_bucketName}/{s3Key} → 403 FORBIDDEN (treating as not-found for anonymous access)");

            if (IsMetadataKey(key))
                _metadataCache[key] = null;

            return null;
        }
        catch (AmazonS3Exception ex)
        {
            System.Diagnostics.Debug.WriteLine(
                $"[S3ZarrStore] GET s3://{_bucketName}/{s3Key} → ERROR {ex.StatusCode} {ex.ErrorCode}: {ex.Message}");

            throw;
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

        // Invalidate cached metadata if we just overwrote it
        if (IsMetadataKey(key))
            _metadataCache[key] = data;
    }

    // =========================================================================
    // IZarrStore — Exists
    // =========================================================================

    public async Task<bool> ExistsAsync(string key, CancellationToken ct = default)
    {
        ThrowIfDisposed();

        // Check metadata cache — a non-null entry means the key exists
        if (IsMetadataKey(key) && _metadataCache.TryGetValue(key, out var cached))
            return cached is not null;

        // Skip HEAD (GetObjectMetadata) entirely. HEAD is unreliable across
        // S3-compatible endpoints:
        //   - Public AWS buckets often return 403 for HEAD but 200 for GET
        //   - Ceph RadosGW may reject anonymous HEAD with malformed auth errors
        //   - MinIO has similar edge cases
        //
        // Instead, use GET directly. For metadata keys (the primary caller of
        // ExistsAsync during the open sequence), we need the data anyway so
        // ReadAsync caches it — zero wasted bandwidth. For non-metadata keys,
        // use a range GET for just one byte to avoid downloading large chunks.

        System.Diagnostics.Debug.WriteLine(
            $"[S3ZarrStore] EXISTS (via GET): bucket={_bucketName} key={BuildS3Key(key)}");

        if (IsMetadataKey(key))
        {
            var data = await ReadAsync(key, ct).ConfigureAwait(false);
            return data is not null;
        }

        return await ProbeExistenceViaRangeGet(BuildS3Key(key), ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Checks whether an S3 object exists by requesting just the first byte.
    /// Used as a fallback when HEAD returns 403 on public buckets.
    /// </summary>
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
        catch (AmazonS3Exception ex) when (IsNotFound(ex))
        {
            return false;
        }
        catch (AmazonS3Exception)
        {
            // Any other S3 error — treat as not found rather than crashing.
            return false;
        }
    }

    // =========================================================================
    // IZarrStore — List  (the big win over HttpZarrStore)
    // =========================================================================

    /// <summary>
    /// Lists all keys under the given prefix using S3 ListObjectsV2.
    /// Handles pagination automatically (S3 returns max 1000 keys per page).
    /// Returns store-relative keys (prefix stripped).
    /// </summary>
    public async Task<IReadOnlyList<string>> ListAsync(string prefix = "", CancellationToken ct = default)
    {
        ThrowIfDisposed();

        var s3Prefix = BuildS3Key(prefix);

        // Ensure the prefix ends with '/' so we don't match partial key names.
        // e.g. prefix "0/1" should not match "0/10/zarr.json"
        if (s3Prefix.Length > 0 && !s3Prefix.EndsWith('/'))
            s3Prefix += '/';

        var results           = new List<string>();
        string? continuation  = null;

        do
        {
            var request = new ListObjectsV2Request
            {
                BucketName            = _bucketName,
                Prefix                = s3Prefix,
                ContinuationToken     = continuation
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

        // Remove from metadata cache if present
        if (IsMetadataKey(key))
            _metadataCache.TryRemove(key, out _);
    }

    // =========================================================================
    // S3 URI parsing
    // =========================================================================

    /// <summary>
    /// Parses an s3:// URI into bucket name and key prefix.
    ///
    /// Accepted formats:
    ///   s3://bucket-name
    ///   s3://bucket-name/
    ///   s3://bucket-name/some/prefix/path
    /// </summary>
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

    /// <summary>
    /// Returns true if the s3Uri looks like an S3 URI (starts with "s3://").
    /// Used by external code (e.g. OmeZarrReader.CreateStore) to decide
    /// which store implementation to instantiate.
    /// </summary>
    public static bool IsS3Uri(string uri)
        => uri.StartsWith("s3://", StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// Extracts just the bucket name from an s3:// URI without constructing
    /// the full store. Used by OmeZarrReader.CreateStore to check whether
    /// the bucket maps to a known S3-compatible endpoint before deciding
    /// which constructor to call.
    /// </summary>
    public static string ParseBucketName(string s3Uri)
        => ParseS3Uri(s3Uri).Bucket;

    // =========================================================================
    // Key / path helpers
    // =========================================================================

    /// <summary>
    /// Converts a store-relative key (e.g. "0/zarr.json") into a full
    /// S3 object key by prepending the root prefix.
    /// </summary>
    private string BuildS3Key(string storeRelativeKey)
    {
        if (string.IsNullOrEmpty(_keyPrefix))
            return storeRelativeKey;

        if (string.IsNullOrEmpty(storeRelativeKey))
            return _keyPrefix;

        return $"{_keyPrefix}/{storeRelativeKey}";
    }

    /// <summary>
    /// Strips the root prefix from an absolute S3 key to produce a
    /// store-relative key for the caller.
    /// </summary>
    private string StripPrefix(string absoluteS3Key)
    {
        var expectedPrefix = string.IsNullOrEmpty(_keyPrefix)
            ? string.Empty
            : _keyPrefix + "/";

        if (expectedPrefix.Length == 0)
            return absoluteS3Key;

        if (absoluteS3Key.StartsWith(expectedPrefix, StringComparison.Ordinal))
            return absoluteS3Key.Substring(expectedPrefix.Length);

        // Shouldn't happen if the ListObjects prefix filter is correct,
        // but guard against it rather than returning garbage keys.
        return absoluteS3Key;
    }

    private static bool IsMetadataKey(string key)
    {
        return key.EndsWith("zarr.json", StringComparison.OrdinalIgnoreCase)
            || key.EndsWith(".zarray",   StringComparison.OrdinalIgnoreCase)
            || key.EndsWith(".zgroup",   StringComparison.OrdinalIgnoreCase)
            || key.EndsWith(".zattrs",   StringComparison.OrdinalIgnoreCase)
            || key.EndsWith(".zmetadata", StringComparison.OrdinalIgnoreCase);
    }

    private static bool IsNotFound(AmazonS3Exception ex)
    {
        return ex.StatusCode == System.Net.HttpStatusCode.NotFound
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
