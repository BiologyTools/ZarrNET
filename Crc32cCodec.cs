namespace ZarrNET;

/// <summary>
/// The "crc32c" bytes-to-bytes codec (Zarr v3 spec).
///
/// On encode: computes CRC32C over the input and appends 4 bytes (little-endian).
/// On decode: reads the trailing 4-byte checksum, validates it against the
///            payload, and returns the payload without the checksum bytes.
///
/// This codec is commonly used as an index codec in sharding configurations
/// to protect the shard index integrity.
///
/// Uses a self-contained CRC32C implementation (Castagnoli polynomial 0x1EDC6F41)
/// with no external NuGet dependencies.
/// </summary>
public sealed class Crc32cCodec : IZarrCodec
{
    public string Name => "crc32c";

    // -------------------------------------------------------------------------
    // IZarrCodec
    // -------------------------------------------------------------------------

    public Task<byte[]> DecodeAsync(byte[] input, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();

        if (input.Length < 4)
            throw new InvalidOperationException(
                $"crc32c decode: input is {input.Length} bytes, need at least 4 for the checksum.");

        var payloadLength = input.Length - 4;
        var storedHash    = (uint)(input[payloadLength]
                          | (input[payloadLength + 1] << 8)
                          | (input[payloadLength + 2] << 16)
                          | (input[payloadLength + 3] << 24));

        var computedHash = Crc32CHash.Compute(input, 0, payloadLength);

        if (computedHash != storedHash)
            throw new InvalidOperationException(
                $"crc32c checksum mismatch: stored 0x{storedHash:X8}, " +
                $"computed 0x{computedHash:X8}. The data may be corrupted.");

        var result = new byte[payloadLength];
        Array.Copy(input, 0, result, 0, payloadLength);

        return Task.FromResult(result);
    }

    public Task<byte[]> EncodeAsync(byte[] input, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();

        var hash   = Crc32CHash.Compute(input, 0, input.Length);
        var result = new byte[input.Length + 4];

        Array.Copy(input, 0, result, 0, input.Length);

        // Append little-endian uint32
        result[input.Length]     = (byte)(hash);
        result[input.Length + 1] = (byte)(hash >> 8);
        result[input.Length + 2] = (byte)(hash >> 16);
        result[input.Length + 3] = (byte)(hash >> 24);

        return Task.FromResult(result);
    }

    // =========================================================================
    // Self-contained CRC32C (Castagnoli) implementation
    // Polynomial: 0x1EDC6F41 (reflected: 0x82F63B78)
    // =========================================================================

    private static class Crc32CHash
    {
        private static readonly uint[] Table = BuildTable();

        public static uint Compute(byte[] data, int offset, int length)
        {
            uint crc = 0xFFFFFFFF;

            var end = offset + length;
            for (int i = offset; i < end; i++)
            {
                crc = (crc >> 8) ^ Table[(crc ^ data[i]) & 0xFF];
            }

            return crc ^ 0xFFFFFFFF;
        }

        private static uint[] BuildTable()
        {
            const uint polynomial = 0x82F63B78;  // reflected Castagnoli
            var table = new uint[256];

            for (uint i = 0; i < 256; i++)
            {
                uint crc = i;
                for (int j = 0; j < 8; j++)
                {
                    crc = (crc & 1) != 0
                        ? (crc >> 1) ^ polynomial
                        : crc >> 1;
                }
                table[i] = crc;
            }

            return table;
        }
    }
}
