__all__: list[str] = [
    "BOOL",
    "INT8",
    "INT16",
    "INT32",
    "INT64",
    "INT128",
    "INT256",
    "UINT8",
    "UINT16",
    "UINT32",
    "UINT64",
    "UINT128",
    "UINT256",
    "FLOAT32",
    "FLOAT64",
    "STRING",
    "DATE",
    "DATE32",
    "DATETIME",
    "DATETIME64",
    "ChdbType",
]

class ChdbType: ...

BOOL: ChdbType        # value = Bool
INT8: ChdbType        # value = Int8
INT16: ChdbType       # value = Int16
INT32: ChdbType       # value = Int32
INT64: ChdbType       # value = Int64
INT128: ChdbType      # value = Int128
INT256: ChdbType      # value = Int256
UINT8: ChdbType       # value = UInt8
UINT16: ChdbType      # value = UInt16
UINT32: ChdbType      # value = UInt32
UINT64: ChdbType      # value = UInt64
UINT128: ChdbType     # value = UInt128
UINT256: ChdbType     # value = UInt256
FLOAT32: ChdbType     # value = Float32
FLOAT64: ChdbType     # value = Float64
STRING: ChdbType      # value = String
DATE: ChdbType        # value = Date
DATE32: ChdbType      # value = Date32
DATETIME: ChdbType    # value = DateTime
DATETIME64: ChdbType  # value = DateTime64(3)
