using ZarrNET.Core.OmeZarr.Metadata;

namespace ZarrNET.Core.OmeZarr.Coordinates;

/// <summary>
/// Converts between physical coordinate space (ROI in microns, etc.) and
/// array index space (PixelRegion) using the coordinate transformations
/// declared in OME-Zarr metadata.
///
/// OME-Zarr transform semantics:
///   Physical = Scale * Index + Translation   (index → physical)
///   Index    = (Physical - Translation) / Scale   (physical → index)
///
/// Two levels of transforms must be composed:
///   1. Per-dataset transforms (on each DatasetMetadata)
///   2. Top-level multiscale transforms (on MultiscaleMetadata itself)
///
/// Per-dataset transforms are applied first, then top-level transforms.
/// </summary>
public sealed class CoordinateTransformService
{
    // -------------------------------------------------------------------------
    // Public API
    // -------------------------------------------------------------------------

    /// <summary>
    /// Converts a physical ROI to a pixel region for a specific resolution level.
    /// The pixel region is clamped to the array bounds and snapped to whole pixels.
    /// </summary>
    public PixelRegion PhysicalToPixel(
        PhysicalROI         roi,
        DatasetMetadata     dataset,
        MultiscaleMetadata  multiscale,
        long[]              arrayShape)
    {
        var composedTransforms = ComposeTransforms(dataset, multiscale);
        var (scale, translation) = FlattenToScaleTranslation(composedTransforms, roi.Rank);

        var pixelStart = PhysicalToIndex(roi.Origin, scale, translation);
        var physicalEnd = roi.End;
        var pixelEnd   = PhysicalToIndex(physicalEnd, scale, translation);

        var clampedStart = ClampAndFloor(pixelStart, arrayShape);
        var clampedEnd   = ClampAndCeil(pixelEnd,   arrayShape);

        EnsureMinimumRegion(clampedStart, clampedEnd, arrayShape);

        return new PixelRegion(clampedStart, clampedEnd);
    }

    /// <summary>
    /// Converts a pixel region back to a physical ROI for a specific resolution level.
    /// Useful for reporting the actual physical extent of a read region.
    /// </summary>
    public PhysicalROI PixelToPhysical(
        PixelRegion        region,
        DatasetMetadata    dataset,
        MultiscaleMetadata multiscale)
    {
        var composedTransforms   = ComposeTransforms(dataset, multiscale);
        var (scale, translation) = FlattenToScaleTranslation(composedTransforms, region.Rank);

        var physicalOrigin = IndexToPhysical(region.Start.Select(v => (double)v).ToArray(), scale, translation);
        var physicalEnd    = IndexToPhysical(region.End.Select(v => (double)v).ToArray(), scale, translation);

        var physicalSize = physicalOrigin.Zip(physicalEnd, (o, e) => Math.Abs(e - o)).ToArray();

        return new PhysicalROI(physicalOrigin, physicalSize);
    }

    /// <summary>
    /// Returns the physical pixel size (scale) for a given dataset.
    /// Useful for resolution level selection — pick the level closest to
    /// the desired physical resolution.
    /// </summary>
    public double[] GetPixelSize(DatasetMetadata dataset, MultiscaleMetadata multiscale)
    {
        var composedTransforms   = ComposeTransforms(dataset, multiscale);
        var (scale, _)           = FlattenToScaleTranslation(composedTransforms, multiscale.Axes.Length);
        return scale;
    }

    // -------------------------------------------------------------------------
    // Transform composition
    // -------------------------------------------------------------------------

    /// <summary>
    /// Composes per-dataset transforms with top-level multiscale transforms
    /// into an ordered list to apply sequentially.
    /// Dataset transforms come first per the OME-Zarr spec.
    /// </summary>
    private static IReadOnlyList<CoordinateTransformation> ComposeTransforms(
        DatasetMetadata    dataset,
        MultiscaleMetadata multiscale)
    {
        var transforms = new List<CoordinateTransformation>();

        transforms.AddRange(dataset.CoordinateTransformations);

        if (multiscale.CoordinateTransformations is not null)
            transforms.AddRange(multiscale.CoordinateTransformations);

        return transforms;
    }

    /// <summary>
    /// Reduces an ordered list of transforms to a single (scale, translation) pair
    /// by composing them in order. Supports "scale", "translation", and "identity".
    /// </summary>
    private static (double[] Scale, double[] Translation) FlattenToScaleTranslation(
        IReadOnlyList<CoordinateTransformation> transforms,
        int rank)
    {
        var composedScale       = Enumerable.Repeat(1.0, rank).ToArray();
        var composedTranslation = Enumerable.Repeat(0.0, rank).ToArray();

        foreach (var transform in transforms)
        {
            switch (transform.Type)
            {
                case "identity":
                    break;

                case "scale":
                    ApplyScale(transform, composedScale, composedTranslation, rank);
                    break;

                case "translation":
                    ApplyTranslation(transform, composedTranslation, rank);
                    break;

                default:
                    throw new NotSupportedException(
                        $"Coordinate transform type '{transform.Type}' is not supported. " +
                        $"Supported types: scale, translation, identity.");
            }
        }

        return (composedScale, composedTranslation);
    }

    private static void ApplyScale(
        CoordinateTransformation transform,
        double[]                 composedScale,
        double[]                 composedTranslation,
        int                      rank)
    {
        if (transform.Scale is null)
            throw new InvalidOperationException("Scale transform is missing 'scale' values.");

        if (transform.Scale.Length != rank)
            throw new InvalidOperationException(
                $"Scale transform has {transform.Scale.Length} components, expected {rank}.");

        for (int d = 0; d < rank; d++)
        {
            // Composing: new_physical = new_scale * (old_scale * index + old_translation)
            // So overall scale multiplies, translation scales too
            composedTranslation[d] *= transform.Scale[d];
            composedScale[d]       *= transform.Scale[d];
        }
    }

    private static void ApplyTranslation(
        CoordinateTransformation transform,
        double[]                 composedTranslation,
        int                      rank)
    {
        if (transform.Translation is null)
            throw new InvalidOperationException("Translation transform is missing 'translation' values.");

        if (transform.Translation.Length != rank)
            throw new InvalidOperationException(
                $"Translation transform has {transform.Translation.Length} components, expected {rank}.");

        for (int d = 0; d < rank; d++)
            composedTranslation[d] += transform.Translation[d];
    }

    // -------------------------------------------------------------------------
    // Index ↔ physical conversion
    // -------------------------------------------------------------------------

    private static double[] PhysicalToIndex(double[] physical, double[] scale, double[] translation)
    {
        var index = new double[physical.Length];
        for (int d = 0; d < physical.Length; d++)
            index[d] = (physical[d] - translation[d]) / scale[d];
        return index;
    }

    private static double[] IndexToPhysical(double[] index, double[] scale, double[] translation)
    {
        var physical = new double[index.Length];
        for (int d = 0; d < index.Length; d++)
            physical[d] = scale[d] * index[d] + translation[d];
        return physical;
    }

    // -------------------------------------------------------------------------
    // Clamping and snapping helpers
    // -------------------------------------------------------------------------

    private static long[] ClampAndFloor(double[] values, long[] shape)
    {
        var result = new long[values.Length];
        for (int d = 0; d < values.Length; d++)
            result[d] = (long)Math.Max(0, Math.Floor(values[d]));
        return result;
    }

    private static long[] ClampAndCeil(double[] values, long[] shape)
    {
        var result = new long[values.Length];
        for (int d = 0; d < values.Length; d++)
            result[d] = Math.Min(shape[d], (long)Math.Ceiling(values[d]));
        return result;
    }

    private static void EnsureMinimumRegion(long[] start, long[] end, long[] shape)
    {
        for (int d = 0; d < start.Length; d++)
        {
            if (end[d] <= start[d])
            {
                // Expand by one pixel in this axis, clamped to array bounds
                end[d] = Math.Min(start[d] + 1, shape[d]);

                if (end[d] <= start[d])
                    start[d] = Math.Max(0, end[d] - 1);
            }
        }
    }
}
