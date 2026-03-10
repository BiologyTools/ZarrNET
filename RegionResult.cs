using ZarrNET.Core.OmeZarr.Metadata;

namespace ZarrNET.Core.OmeZarr.Nodes;

/// <summary>
/// The result of reading a data region from a resolution level.
/// Carries raw bytes alongside the metadata the caller needs to interpret them:
/// the actual pixel shape, the data type for casting, and the axis descriptors.
///
/// Data layout: C-order (row-major), matching the Zarr array layout.
/// Element size in bytes = DataType size (e.g. uint16 → 2 bytes per element).
/// Total bytes = Product(Shape) * ElementSizeBytes.
/// </summary>
public sealed record RegionResult(
    byte[]         Data,
    long[]          Shape,
    string         DataType,
    AxisMetadata[] Axes)
{
    /// <summary>Number of dimensions.</summary>
    public int Rank => Shape.Length;

    /// <summary>Total number of elements in the region.</summary>
    public long ElementCount => Shape.Aggregate(1L, (acc, s) => acc * s);

    /// <summary>
    /// Returns a human-readable summary of the region.
    /// E.g. "uint16 [2, 512, 512] (t, y, x) — 524288 bytes"
    /// </summary>
    public override string ToString()
    {
        var shapeStr = string.Join(", ", Shape);
        var axisStr  = Axes.Length > 0
            ? " (" + string.Join(", ", Axes.Select(a => a.Name)) + ")"
            : string.Empty;

        return $"{DataType} [{shapeStr}]{axisStr} — {Data.Length:N0} bytes";
    }
}
