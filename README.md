# Zarr.NET

A high-performance, fully async C# library for reading and writing OME-Zarr datasets with comprehensive support for multiscale images, labels, and High-Content Screening (HCS) plate data.

## Features

✅ **Zarr v2 & v3 Support** - Automatic version detection and handling  
✅ **OME-Zarr 0.4 & 0.5** - Full spec compliance for multiscale images, labels, and HCS plates  
✅ **Remote Access** - Read from HTTP/HTTPS, S3, Azure Blob, Google Cloud Storage  
✅ **Physical Coordinates** - ROI reading in real-world units (micrometers, seconds, etc.)  
✅ **Compression** - Gzip, Zstandard (zstd) codec support  
✅ **Memory Efficient** - Chunk-level reading with streaming support  
✅ **Type Safe** - Strongly typed metadata models and coordinate transformations  
✅ **Cross-Platform** - .NET 10.0, works on Windows, Linux, macOS  
✅ **Well-Architected** - Clean separation of concerns, testable, extensible  

## Installation

```bash
# Via NuGet (when published)
dotnet add package ZarrNET

# Or clone and build locally
git clone https://github.com/yBiologyTools/ZarrNET.git
cd ZarrNET
dotnet build
```

### Dependencies

- .NET 10.0 or higher
- ZstdSharp.Port (managed zstd implementation)
- System.Text.Json (included in .NET 8.0)
- AForgeBio

## Quick Start

### Read a single 2D plane from a multiscale image

```csharp
using ZarrNET;
using ZarrNET.Helpers;

await using var reader = await OmeZarrReader.OpenAsync("/path/to/dataset.zarr");
var image = reader.AsMultiscaleImage();
var level = await image.OpenResolutionLevelAsync(datasetIndex: 0);

// Read timepoint 0, channel 1, z-slice 5
var plane = await level.ReadPlaneAsync(t: 0, c: 1, z: 5);

// Get as typed 2D array
ushort[,] pixels = plane.As2DArray<ushort>();  // for uint16 data
Console.WriteLine($"Plane: {plane.Width}x{plane.Height}, max value: {pixels.Cast<ushort>().Max()}");

// Or get as byte array for interop
byte[] bytes = plane.ToBytes<ushort>(PixelFormat.Gray8);
```

### Read using physical coordinates (ROI in microns)

```csharp
using ZarrNET.Coordinates;

// Define ROI: 100µm x 100µm region at specific location
var roi = new PhysicalROI(
    origin: [0, 0, 5.0, 50.0, 50.0],    // t, c, z, y, x in physical units
    size:   [1, 1, 1.0, 100.0, 100.0]   // timepoint, channel, z-slice, 100µm x 100µm
);

var result = await level.ReadRegionAsync(roi);
```

### Read from remote HTTP/S3 URLs

```csharp
// Public HTTP server
var httpUrl = "https://example.com/datasets/image.zarr";
await using var reader = await OmeZarrReader.OpenAsync(httpUrl);

// S3 public bucket
var s3Url = "https://s3.amazonaws.com/bucket/image.zarr";
await using var s3Reader = await OmeZarrReader.OpenAsync(s3Url);

// Azure Blob Storage
var azureUrl = "https://account.blob.core.windows.net/container/image.zarr";
await using var azureReader = await OmeZarrReader.OpenAsync(azureUrl);

// Works exactly the same as local files
var image = reader.AsMultiscaleImage();
var plane = await level.ReadPlaneAsync(t: 0, c: 0, z: 0);
```

### Custom HTTP configuration (auth, timeouts)

```csharp
using OmeZarr.Core.Zarr.Store;

var httpClient = new HttpClient();
httpClient.DefaultRequestHeaders.Add("Authorization", "Bearer YOUR_TOKEN");
httpClient.Timeout = TimeSpan.FromMinutes(10);

var store = new HttpZarrStore("https://example.com/data.zarr", httpClient);
await using var reader = await OmeZarrReader.OpenAsync(store);
```

### Navigate HCS Plate structure

```csharp
var plate = reader.AsPlate();
Console.WriteLine($"Plate: {plate.PlateMetadata.Name}");
Console.WriteLine($"Wells: {plate.Wells.Count}");

// Open well B3, field 0
var well = await plate.OpenWellAsync("B", "3");
var field = await well.OpenFieldAsync(0);
var level = await field.OpenResolutionLevelAsync(0);

// Read a plane from the field
var plane = await level.ReadPlaneAsync(c: 0, z: 0);
```

### Work with labels (segmentation masks)

```csharp
var image = reader.AsMultiscaleImage();

if (await image.HasLabelsAsync())
{
    var labelGroup = await image.OpenLabelsAsync();
    Console.WriteLine($"Available labels: {string.Join(", ", labelGroup.LabelNames)}");
    
    var cellLabel = await labelGroup.OpenLabelAsync("cells");
    var labelLevel = await cellLabel.OpenResolutionLevelAsync(0);
    
    var labelPlane = await labelLevel.ReadPlaneAsync(t: 0, c: 0, z: 0);
    var labelIds = labelPlane.As1DArray<uint>();  // typically uint32
    
    var uniqueCells = labelIds.Distinct().Count(id => id != 0);
    Console.WriteLine($"Detected {uniqueCells} cells");
}
```

## Architecture

The library is structured in clean, composable layers:

```
┌─────────────────────────────────────────────────────┐
│ OmeZarrReader (Public API)                          │
├─────────────────────────────────────────────────────┤
│ Node Tree Layer                                      │
│  - MultiscaleNode, PlateNode, WellNode, FieldNode   │
│  - ResolutionLevelNode, LabelNode                   │
├─────────────────────────────────────────────────────┤
│ Coordinate Transform Layer                          │
│  - PhysicalROI ↔ PixelRegion                        │
│  - CoordinateTransformService                       │
├─────────────────────────────────────────────────────┤
│ OME-Zarr Metadata Layer                             │
│  - MultiscaleMetadata, PlateMetadata, etc.          │
│  - OmeAttributesParser                              │
├─────────────────────────────────────────────────────┤
│ Zarr Array/Group Layer                              │
│  - ZarrArray (chunk reading, region extraction)     │
│  - ZarrGroup (tree navigation)                      │
├─────────────────────────────────────────────────────┤
│ Codec Pipeline                                       │
│  - BytesCodec, GzipCodec, ZstdCodec                 │
│  - CodecPipeline (ordered application)              │
├─────────────────────────────────────────────────────┤
│ Store Layer (I/O)                                    │
│  - IZarrStore, LocalFileSystemStore                 │
└─────────────────────────────────────────────────────┘
```

### Design Principles

- **Separation of Concerns** - Each layer has a single, well-defined responsibility
- **No Leaky Abstractions** - Zarr layer knows nothing about OME, OME layer knows nothing about I/O
- **Async Throughout** - All I/O operations are async from the start
- **Variable Staging Pattern** - Readable code with clear intermediate values
- **Testable** - Interfaces at key boundaries enable unit testing

## Supported Features

### Data Types
- `uint8`, `uint16`

### Compression
- **Gzip** - Standard compression (via System.IO.Compression)
- **Zstandard** - High-performance compression (via ZstdSharp.Port)
- **Uncompressed** - Raw data
- **Blosc**

### OME-Zarr Node Types
- **Multiscale Images** - Pyramidal resolution levels with coordinate transforms
- **Labels** - Segmentation masks with color/property metadata
- **HCS Plates** - Wells, fields, acquisitions
- **Label Groups** - Multiple label arrays per image

### Zarr Versions
- **Zarr v2** - `.zarray`, `.zattrs`, `.zgroup` files (widely used by Fiji, napari, OMERO)
- **Zarr v3** - Single `zarr.json` per node (newer spec)
- Automatic version detection and handling

## API Overview

### Core Classes

#### `OmeZarrReader`
Entry point for opening datasets. Auto-detects node type.

```csharp
await using var reader = await OmeZarrReader.OpenAsync(path);
var root = reader.OpenRoot();  // Auto-dispatch to correct type

// Or strongly-typed access
var image = reader.AsMultiscaleImage();
var plate = reader.AsPlate();
var well = reader.AsWell();
```

#### `ResolutionLevelNode`
Represents a single resolution level in a multiscale pyramid.

```csharp
var level = await image.OpenResolutionLevelAsync(datasetIndex: 0);

// Properties
long[] shape = level.Shape;          // [t, c, z, y, x]
string dtype = level.DataType;       // "uint16", "float32", etc.
double[] pixelSize = level.GetPixelSize();  // Physical size per pixel

// Reading
var plane = await level.ReadPlaneAsync(t: 0, c: 0, z: 5);
var roi = await level.ReadRegionAsync(physicalROI);
var region = await level.ReadPixelRegionAsync(pixelRegion);

// Writing
await level.WriteRegionAsync(pixelRegion, data);
```

#### `PlaneResult`
Result of reading a 2D plane with convenience extraction methods.

```csharp
var plane = await level.ReadPlaneAsync(t: 0, c: 0, z: 0);

int width = plane.Width;
int height = plane.Height;

// Extract as typed arrays
ushort[,] pixels2D = plane.As2DArray<ushort>();
ushort[] pixels1D = plane.As1DArray<ushort>();

// Convert to specific formats
byte[] gray8 = plane.ToBytes<ushort>(PixelFormat.Gray8);
byte[] bgra32 = plane.ToBytes<ushort>(PixelFormat.Bgra32);
```

#### `PhysicalROI` and `PixelRegion`
Coordinate representations for ROI specification.

```csharp
// Physical coordinates (microns, seconds, etc.)
var physicalROI = new PhysicalROI(
    origin: [0, 0, 5.0, 100.0, 200.0],
    size:   [1, 1, 2.0, 50.0, 50.0]
);

// Pixel coordinates (array indices)
var pixelRegion = new PixelRegion(
    start: [0, 0, 10, 100, 200],
    end:   [1, 1, 12, 150, 250]
);
```

## Examples

### Iterate through a Z-stack

```csharp
var axes = level.Multiscale.Axes;
var zIndex = Array.FindIndex(axes, a => a.Name.Equals("z", StringComparison.OrdinalIgnoreCase));
var numSlices = (int)level.Shape[zIndex];

for (int z = 0; z < numSlices; z++)
{
    var plane = await level.ReadPlaneAsync(t: 0, c: 0, z: z);
    var pixels = plane.As1DArray<ushort>();
    
    double meanIntensity = pixels.Average(p => (double)p);
    Console.WriteLine($"Z={z}: mean intensity = {meanIntensity:F1}");
}
```

### Read all channels as separate planes

```csharp
var cIndex = Array.FindIndex(axes, a => a.Name.Equals("c", StringComparison.OrdinalIgnoreCase));
var numChannels = (int)level.Shape[cIndex];

var channels = new List<ushort[,]>();
for (int c = 0; c < numChannels; c++)
{
    var plane = await level.ReadPlaneAsync(t: 0, c: c, z: 0);
    channels.Add(plane.As2DArray<ushort>());
}

// Create RGB composite, max projection, etc.
```

### Choose optimal resolution level for display

```csharp
var levels = await image.OpenAllResolutionLevelsAsync();
double targetMicronsPerPixel = 0.5;

var spatialAxisIndex = 3;  // y axis in t,c,z,y,x
var bestLevel = levels
    .Select((l, i) => (level: l, index: i, pixelSize: l.GetPixelSize()[spatialAxisIndex]))
    .OrderBy(l => Math.Abs(l.pixelSize - targetMicronsPerPixel))
    .First();

Console.WriteLine($"Using level {bestLevel.index} ({bestLevel.pixelSize:G4} µm/px)");
```

### Process an entire HCS plate

```csharp
var plate = reader.AsPlate();

foreach (var wellRef in plate.Wells)
{
    var well = await plate.OpenWellAsync(wellRef.Path);
    
    foreach (var fieldRef in well.Fields)
    {
        var field = await well.OpenFieldAsync(fieldRef.Path);
        var level = await field.OpenResolutionLevelAsync(0);
        
        // Process each field
        var plane = await level.ReadPlaneAsync(c: 0, z: 0);
        // ... analyze, segment, etc.
    }
}
```

### Save plane as image file (using ImageSharp)

```csharp
using SixLabors.ImageSharp;
using SixLabors.ImageSharp.PixelFormats;

var plane = await level.ReadPlaneAsync(t: 0, c: 0, z: 5);
byte[] pixels = plane.ToBytes<ushort>(PixelFormat.Gray8);

var img = Image.LoadPixelData<L8>(pixels, plane.Width, plane.Height);
img.SaveAsPng("output.png");
```

## Performance Considerations

### Chunk-Aligned Reads
For best performance, align ROI boundaries to chunk boundaries when possible:

```csharp
var chunkShape = level.ZarrArray.Metadata.ChunkShape;  // e.g., [1, 1, 1, 96, 96]

// Good: aligned to chunk boundaries (multiples of 96)
var alignedROI = new PixelRegion(
    start: [0, 0, 0, 0, 0],
    end:   [1, 1, 1, 96, 192]  // 1x2 chunks
);

// Less efficient: crosses chunk boundaries
var unalignedROI = new PixelRegion(
    start: [0, 0, 0, 50, 50],
    end:   [1, 1, 1, 150, 150]  // touches 4 chunks
);
```

### Resolution Level Selection
Use lower resolution levels for overview/navigation, full resolution for analysis:

```csharp
// Level 0: Full resolution (slow, detailed)
// Level 3: 1/8 resolution (fast, overview)
var overview = await image.OpenResolutionLevelAsync(datasetIndex: 3);
var fullRes = await image.OpenResolutionLevelAsync(datasetIndex: 0);
```

### Memory Management
For large datasets, process in tiles rather than loading entire planes:

```csharp
int tileSize = 512;
for (int y = 0; y < height; y += tileSize)
{
    for (int x = 0; x < width; x += tileSize)
    {
        var tileRegion = new PixelRegion(
            start: [0, 0, 0, y, x],
            end:   [1, 1, 1, Math.Min(y + tileSize, height), Math.Min(x + tileSize, width)]
        );
        
        var tile = await level.ReadPixelRegionAsync(tileRegion);
        // Process tile...
    }
}
```

## Limitations
- **Sharded Zarr v3** - Not yet supported (planned).
- **HTTP store listing** - ListChildNamesAsync() not supported for HTTP stores. Use explicit path navigation.
- **Write support to HTTP** - Read-only for remote stores. Use LocalFileSystemStore for writing.
- **Full dataset creation** - Basic region writing works, but no complete writer API yet.

## Roadmap

- [x] HTTP/S3 remote store support
- [x] Blosc codec support
- [ ] AWS S3 native SDK integration (ListObjects, credentials)
- [x] Sharded Zarr v3 support
- [ ] OME-Zarr writer API for creating new datasets
- [x] Parallel chunk reading for improved performance
- [ ] Zarr v2 → v3 conversion utilities
- [ ] NGFF transformations support (rotation, affine)
- [ ] Consolidated metadata (.zmetadata) support for HTTP stores

## Contributing

Contributions welcome! Please:

1. Follow the existing code style (variable staging pattern, clear separation of concerns)
2. Add tests for new features
3. Update documentation
4. Open an issue first for major changes

## License
- GNU GPL 3.0 only.

## Acknowledgments

- **OME-Zarr Specification** - https://ngff.openmicroscopy.org/
- **Zarr Specification** - https://zarr-specs.readthedocs.io/
- **ZstdSharp** - Managed Zstandard compression
- Inspired by zarr-python, ome-zarr-py, and the broader scientific imaging community

## Support
Please use image.sc forum for discussion.
---

**Built with ❤️ for the scientific imaging community**
