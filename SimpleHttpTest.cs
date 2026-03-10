using OmeZarr.Core.OmeZarr.Helpers;
using ZarrNET;
using ZarrNET.Core;
using ZarrNET.Core.OmeZarr;

namespace ZarrNET
{
    public static class SimpleHttpTest
    {
        public async static void Start()
        {
            // Simple test with public IDR dataset
            var url = "https://uk1s3.embassy.ebi.ac.uk/idr/zarr/v0.4/idr0062A/6001240.zarr";
            await using var reader = await OmeZarrReader.OpenAsync(url);
            var image = reader.AsMultiscaleImage();
            var level = await image.OpenResolutionLevelAsync(0);

            Console.WriteLine($"Array shape: [{string.Join(", ", level.Shape)}]");

            var plane = await level.ReadPlaneAsync(t: 0, c: 0, z: 0);

            Console.WriteLine($"Downloaded plane: {plane.Width}x{plane.Height}, {plane.Data.Length:N0} bytes");
        }
    }
}