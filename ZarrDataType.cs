namespace ZarrNET;

/// <summary>
/// Typed representation of a Zarr v3 data type string (e.g. "uint8", "float32").
/// Resolves element size in bytes and provides type classification used
/// by the codec pipeline for byte-order handling.
/// </summary>
public sealed class ZarrDataType
{
    public string TypeString { get; }
    public int    ElementSize { get; }
    public bool   IsFloat     { get; }
    public bool   IsInteger   { get; }
    public bool   IsSigned    { get; }

    private ZarrDataType(string typeString, int elementSize, bool isFloat, bool isSigned)
    {
        TypeString  = typeString;
        ElementSize = elementSize;
        IsFloat     = isFloat;
        IsInteger   = !isFloat;
        IsSigned    = isSigned;
    }

    // -------------------------------------------------------------------------
    // Parsing
    // -------------------------------------------------------------------------

    public static ZarrDataType Parse(string typeString)
    {
        return typeString switch
        {
            "bool"    => new ZarrDataType(typeString, 1, isFloat: false, isSigned: false),
            "int8"    => new ZarrDataType(typeString, 1, isFloat: false, isSigned: true),
            "uint8"   => new ZarrDataType(typeString, 1, isFloat: false, isSigned: false),
            "int16"   => new ZarrDataType(typeString, 2, isFloat: false, isSigned: true),
            "uint16"  => new ZarrDataType(typeString, 2, isFloat: false, isSigned: false),
            "int32"   => new ZarrDataType(typeString, 4, isFloat: false, isSigned: true),
            "uint32"  => new ZarrDataType(typeString, 4, isFloat: false, isSigned: false),
            "int64"   => new ZarrDataType(typeString, 8, isFloat: false, isSigned: true),
            "uint64"  => new ZarrDataType(typeString, 8, isFloat: false, isSigned: false),
            "float32" => new ZarrDataType(typeString, 4, isFloat: true,  isSigned: true),
            "float64" => new ZarrDataType(typeString, 8, isFloat: true,  isSigned: true),
            _ => throw new NotSupportedException($"Unsupported Zarr data type: '{typeString}'")
        };
    }

    public override string ToString() => TypeString;
}
