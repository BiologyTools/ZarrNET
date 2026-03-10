namespace ZarrNET.Core.Zarr.Store;

/// <summary>
/// Pure key/value store abstraction. Keys are forward-slash-separated paths
/// relative to the store root (e.g. "0/0.0.0", "labels/nuclei/zarr.json").
/// No Zarr semantics live here — this is infrastructure only.
/// </summary>
public interface IZarrStore : IAsyncDisposable
{
    /// <summary>Reads raw bytes for a key. Returns null if the key does not exist.</summary>
    Task<byte[]?> ReadAsync(string key, CancellationToken ct = default);

    /// <summary>Writes raw bytes to a key, creating it if it does not exist.</summary>
    Task WriteAsync(string key, byte[] data, CancellationToken ct = default);

    /// <summary>Returns true if the key exists in the store.</summary>
    Task<bool> ExistsAsync(string key, CancellationToken ct = default);

    /// <summary>
    /// Lists all keys that begin with the given prefix.
    /// Pass an empty string to list from the root.
    /// </summary>
    Task<IReadOnlyList<string>> ListAsync(string prefix = "", CancellationToken ct = default);

    /// <summary>Deletes a key. No-ops silently if the key does not exist.</summary>
    Task DeleteAsync(string key, CancellationToken ct = default);
}
