namespace ZarrNET.Core.OmeZarr.Coordinates;

/// <summary>
/// A region of interest expressed in physical units (e.g. micrometers).
/// Axis order matches the multiscale axes declaration.
/// All values are axis-aligned (no rotation).
/// </summary>
public sealed class PhysicalROI
{
    /// <summary>
    /// Per-axis physical origin of the ROI, in the unit declared for each axis.
    /// </summary>
    public double[] Origin { get; }

    /// <summary>
    /// Per-axis physical size of the ROI (not the end coordinate — the size).
    /// </summary>
    public double[] Size { get; }

    public int Rank => Origin.Length;

    public PhysicalROI(double[] origin, double[] size)
    {
        if (origin.Length != size.Length)
            throw new ArgumentException(
                $"Origin and Size must have the same rank. " +
                $"Got origin.Length={origin.Length}, size.Length={size.Length}.");

        if (size.Any(s => s <= 0))
            throw new ArgumentException("All size components must be positive.");

        Origin = origin;
        Size   = size;
    }

    /// <summary>Per-axis physical end coordinate (origin + size).</summary>
    public double[] End => Origin.Zip(Size, (o, s) => o + s).ToArray();

    public override string ToString()
    {
        var originStr = string.Join(", ", Origin.Select(v => v.ToString("G4")));
        var sizeStr   = string.Join(", ", Size.Select(v => v.ToString("G4")));
        return $"PhysicalROI(origin=[{originStr}], size=[{sizeStr}])";
    }
}

/// <summary>
/// A region expressed in pixel (array index) coordinates.
/// Maps directly to ZarrArray.ReadRegionAsync parameters.
/// </summary>
public sealed class PixelRegion
{
    /// <summary>Per-axis inclusive start index (zero-based).</summary>
    public long[] Start { get; }

    /// <summary>Per-axis exclusive end index.</summary>
    public long[] End { get; }

    public int  Rank  => Start.Length;
    public long[] Shape => Start.Zip(End, (s, e) => e - s).ToArray();

    public PixelRegion(long[] start, long[] end)
    {
        if (start.Length != end.Length)
            throw new ArgumentException(
                $"Start and End must have the same rank. " +
                $"Got start.Length={start.Length}, end.Length={end.Length}.");

        for (int i = 0; i < start.Length; i++)
        {
            if (end[i] <= start[i])
                throw new ArgumentException(
                    $"end[{i}] ({end[i]}) must be greater than start[{i}] ({start[i]}).");
        }

        Start = start;
        End   = end;
    }

    public override string ToString()
    {
        var startStr = string.Join(", ", Start);
        var endStr   = string.Join(", ", End);
        return $"PixelRegion([{startStr}] → [{endStr}])";
    }
}

/// <summary>
/// Known physical axis units as declared in OME-Zarr axes metadata.
/// Used for unit-aware coordinate conversion when needed.
/// </summary>
public enum AxisUnit
{
    Unknown,

    // Spatial
    Nanometer,
    Micrometer,
    Millimeter,
    Centimeter,
    Meter,
    Inch,
    Foot,
    Angstrom,

    // Temporal
    Nanosecond,
    Microsecond,
    Millisecond,
    Second,
    Minute,
    Hour,
    Day,

    // Dimensionless (channels, z-planes without physical unit)
    Dimensionless
}

public static class AxisUnitParser
{
    public static AxisUnit Parse(string? unit) => unit?.ToLowerInvariant() switch
    {
        "nanometer"   or "nm"  => AxisUnit.Nanometer,
        "micrometer"  or "µm"
                      or "um"  => AxisUnit.Micrometer,
        "millimeter"  or "mm"  => AxisUnit.Millimeter,
        "centimeter"  or "cm"  => AxisUnit.Centimeter,
        "meter"       or "m"   => AxisUnit.Meter,
        "inch"        or "in"  => AxisUnit.Inch,
        "foot"        or "ft"  => AxisUnit.Foot,
        "angstrom"    or "å"   => AxisUnit.Angstrom,
        "nanosecond"  or "ns"  => AxisUnit.Nanosecond,
        "microsecond" or "µs"
                      or "us"  => AxisUnit.Microsecond,
        "millisecond" or "ms"  => AxisUnit.Millisecond,
        "second"      or "s"   => AxisUnit.Second,
        "minute"      or "min" => AxisUnit.Minute,
        "hour"        or "hr"  => AxisUnit.Hour,
        "day"                  => AxisUnit.Day,
        null or ""             => AxisUnit.Dimensionless,
        _                      => AxisUnit.Unknown
    };
}
