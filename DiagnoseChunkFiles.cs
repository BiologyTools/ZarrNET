using ZarrNET;
using ZarrNET.Core.Zarr.Store;
using ZarrNET.Core;
namespace ZarrNET
{
    public static class DiagnoseChunks
    {
        // Check if chunk files actually exist on disk
        public static async Task DiagnoseChunkFiles(string zarrPath)
        {
            await using var reader = await OmeZarrReader.OpenAsync(zarrPath);
            var image = reader.AsMultiscaleImage();

            // Get the resolution level 0 path
            var multiscale = image.Multiscales[0];
            var dataset0 = multiscale.Datasets[0];

            Console.WriteLine($"Dataset 0 path: {dataset0.Path}");

            // Open the store directly to list files
            using var store = new LocalFileSystemStore(zarrPath);

            // List all files in the dataset 0 directory
            var datasetPath = dataset0.Path;
            var allFiles = await store.ListAsync(datasetPath);

            Console.WriteLine($"\nFiles in {datasetPath}/ ({allFiles.Count} total):");

            // Show first 20 files
            foreach (var file in allFiles.Take(20))
            {
                Console.WriteLine($"  {file}");
            }

            if (allFiles.Count > 20)
                Console.WriteLine($"  ... and {allFiles.Count - 20} more files");

            // Check for .zarray metadata
            var zarrayPath = $"{datasetPath}/.zarray";
            var zarrayExists = await store.ExistsAsync(zarrayPath);
            Console.WriteLine($"\n.zarray exists at '{zarrayPath}': {zarrayExists}");

            if (zarrayExists)
            {
                var zarrayBytes = await store.ReadAsync(zarrayPath);
                var zarrayJson = System.Text.Encoding.UTF8.GetString(zarrayBytes);
                Console.WriteLine($"\n.zarray contents:\n{zarrayJson}");
            }

            // Try to find some actual chunk files
            Console.WriteLine("\nLooking for chunk files with pattern '0.0.0.*':");
            var chunkPattern = allFiles.Where(f => f.Contains("0.0.0")).Take(10).ToList();
            foreach (var chunk in chunkPattern)
            {
                var chunkBytes = await store.ReadAsync(chunk);
                Console.WriteLine($"  {chunk} - {chunkBytes?.Length ?? 0} bytes");
            }

            // Check what the expected chunk key format would be
            Console.WriteLine("\nExpected chunk key for chunk [0,0,0,0,0]:");
            Console.WriteLine($"  With '.' separator: {datasetPath}/0.0.0.0.0");
            Console.WriteLine($"  With '/' separator: {datasetPath}/c/0/0/0/0/0");

            // Test both
            var dotKey = $"{datasetPath}/0.0.0.0.0";
            var slashKey = $"{datasetPath}/c/0/0/0/0/0";

            var dotExists = await store.ExistsAsync(dotKey);
            var slashExists = await store.ExistsAsync(slashKey);

            Console.WriteLine($"\nChunk [0,0,0,0,0] exists:");
            Console.WriteLine($"  Dot format: {dotExists}");
            Console.WriteLine($"  Slash format: {slashExists}");
        }
    }
}