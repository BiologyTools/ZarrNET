namespace ZarrNET.Core.Zarr.Store;

/// <summary>
/// IZarrStore backed by the local filesystem. Keys map directly to
/// relative paths under the root directory, using the OS path separator.
/// </summary>
public sealed class LocalFileSystemStore : IZarrStore, IDisposable
{
    private readonly string _rootPath;
    private bool _disposed;

    public string RootPath => _rootPath;
    public void Dispose()
    {
        DisposeAsync();
    }
    public LocalFileSystemStore(string rootPath)
    {
        var expanded = Path.GetFullPath(rootPath);

        if (!Directory.Exists(expanded))
            throw new DirectoryNotFoundException($"Zarr store root not found: {expanded}");

        _rootPath = expanded;
    }

    // -------------------------------------------------------------------------
    // IZarrStore
    // -------------------------------------------------------------------------

    public async Task<byte[]?> ReadAsync(string key, CancellationToken ct = default)
    {
        ThrowIfDisposed();

        var fullPath = ResolveKey(key);

        if (!File.Exists(fullPath))
            return null;

        return await File.ReadAllBytesAsync(fullPath, ct).ConfigureAwait(false);
    }

    public async Task WriteAsync(string key, byte[] data, CancellationToken ct = default)
    {
        ThrowIfDisposed();

        var fullPath = ResolveKey(key);
        var directory = Path.GetDirectoryName(fullPath)!;

        Directory.CreateDirectory(directory);

        await File.WriteAllBytesAsync(fullPath, data, ct).ConfigureAwait(false);
    }

    public Task<bool> ExistsAsync(string key, CancellationToken ct = default)
    {
        ThrowIfDisposed();
        ct.ThrowIfCancellationRequested();

        var fullPath = ResolveKey(key);
        var exists    = File.Exists(fullPath) || Directory.Exists(fullPath);

        return Task.FromResult(exists);
    }

    public Task<IReadOnlyList<string>> ListAsync(string prefix = "", CancellationToken ct = default)
    {
        ThrowIfDisposed();
        ct.ThrowIfCancellationRequested();

        var searchRoot = string.IsNullOrEmpty(prefix)
            ? _rootPath
            : ResolveKey(prefix);

        if (!Directory.Exists(searchRoot))
            return Task.FromResult<IReadOnlyList<string>>(Array.Empty<string>());

        var allFiles = Directory
            .EnumerateFiles(searchRoot, "*", SearchOption.AllDirectories)
            .Select(absPath => ToStoreKey(absPath))
            .OrderBy(k => k)
            .ToList();

        return Task.FromResult<IReadOnlyList<string>>(allFiles);
    }

    public Task DeleteAsync(string key, CancellationToken ct = default)
    {
        ThrowIfDisposed();
        ct.ThrowIfCancellationRequested();

        var fullPath = ResolveKey(key);

        if (File.Exists(fullPath))
            File.Delete(fullPath);

        return Task.CompletedTask;
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /// <summary>
    /// Converts a store key (forward-slash separated) to an absolute filesystem path.
    /// Guards against path traversal attacks by ensuring the resolved path stays
    /// within the store root.
    /// </summary>
    private string ResolveKey(string key)
    {
        // Normalise separator and remove any leading slash
        var relativePath = key
            .Replace('/', Path.DirectorySeparatorChar)
            .TrimStart(Path.DirectorySeparatorChar);

        var fullPath = Path.GetFullPath(Path.Combine(_rootPath, relativePath));

        if (!fullPath.StartsWith(_rootPath, StringComparison.OrdinalIgnoreCase))
            throw new InvalidOperationException(
                $"Key '{key}' resolves outside the store root. Possible path traversal.");

        return fullPath;
    }

    /// <summary>Converts an absolute filesystem path back to a forward-slash store key.</summary>
    private string ToStoreKey(string absolutePath)
    {
        var relative = Path.GetRelativePath(_rootPath, absolutePath);
        return relative.Replace(Path.DirectorySeparatorChar, '/');
    }

    // -------------------------------------------------------------------------
    // Disposal
    // -------------------------------------------------------------------------

    public ValueTask DisposeAsync()
    {
        _disposed = true;
        return ValueTask.CompletedTask;
    }

    private void ThrowIfDisposed()
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(LocalFileSystemStore));
    }
}
