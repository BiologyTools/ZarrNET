using System.Collections.Concurrent;

namespace ZarrNET.Core.Zarr.Store;

/// <summary>
/// Thread-safe LRU cache for decoded chunk data, keyed by chunk path.
///
/// Designed to sit inside HttpZarrStore so that panning back over
/// previously-fetched tiles hits memory instead of the network.
///
/// The eviction policy is approximate-LRU: on each access the entry is
/// moved to the tail of a linked list, and when the byte budget is
/// exceeded the head (oldest) entries are evicted.  This avoids a full
/// sort on every access while still giving good cache behaviour for
/// spatial locality patterns (panning, zooming).
///
/// Concurrency: all public methods lock on a single object.  This is
/// fine because the lock is held only for O(1) pointer manipulation;
/// the expensive work (HTTP fetch, decompression) happens outside.
/// </summary>
public sealed class ChunkLruCache
{
    // -----------------------------------------------------------------
    // Configuration
    // -----------------------------------------------------------------

    /// <summary>
    /// Default budget: 256 MB.  Enough for ~60 uncompressed 2048×2048
    /// uint16 chunks, which covers a comfortable panning window.
    /// </summary>
    public const long DefaultMaxBytes = 256L * 1024 * 1024;

    private readonly long _maxBytes;

    // -----------------------------------------------------------------
    // Internal state
    // -----------------------------------------------------------------

    private readonly object _lock = new();
    private readonly Dictionary<string, LinkedListNode<CacheEntry>> _map = new();
    private readonly LinkedList<CacheEntry> _order = new();   // tail = most recent
    private long _currentBytes;

    // -----------------------------------------------------------------
    // Construction
    // -----------------------------------------------------------------

    public ChunkLruCache(long maxBytes = DefaultMaxBytes)
    {
        _maxBytes = maxBytes;
    }

    // -----------------------------------------------------------------
    // Public API
    // -----------------------------------------------------------------

    /// <summary>
    /// Returns the cached bytes for <paramref name="key"/>, or null if
    /// not present.  A hit promotes the entry to most-recently-used.
    /// </summary>
    public byte[]? TryGet(string key)
    {
        lock (_lock)
        {
            if (!_map.TryGetValue(key, out var node))
                return null;

            // Promote to tail (most recent)
            _order.Remove(node);
            _order.AddLast(node);

            return node.Value.Data;
        }
    }

    /// <summary>
    /// Inserts or replaces an entry.  If the cache exceeds its byte
    /// budget after insertion, the least-recently-used entries are
    /// evicted until the budget is met.
    /// </summary>
    public void Set(string key, byte[] data)
    {
        lock (_lock)
        {
            // If already present, remove the old entry first.
            if (_map.TryGetValue(key, out var existing))
            {
                _currentBytes -= existing.Value.Data.Length;
                _order.Remove(existing);
                _map.Remove(key);
            }

            var entry = new CacheEntry(key, data);
            var node = _order.AddLast(entry);
            _map[key] = node;
            _currentBytes += data.Length;

            EvictUntilUnderBudget();
        }
    }

    /// <summary>
    /// Removes all entries immediately.
    /// </summary>
    public void Clear()
    {
        lock (_lock)
        {
            _map.Clear();
            _order.Clear();
            _currentBytes = 0;
        }
    }

    public long CurrentBytes
    {
        get { lock (_lock) return _currentBytes; }
    }

    public int Count
    {
        get { lock (_lock) return _map.Count; }
    }

    // -----------------------------------------------------------------
    // Internals
    // -----------------------------------------------------------------

    private void EvictUntilUnderBudget()
    {
        while (_currentBytes > _maxBytes && _order.First is not null)
        {
            var victim = _order.First!.Value;
            _order.RemoveFirst();
            _map.Remove(victim.Key);
            _currentBytes -= victim.Data.Length;
        }
    }

    private sealed record CacheEntry(string Key, byte[] Data);
}
