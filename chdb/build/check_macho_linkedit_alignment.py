#!/usr/bin/env python3
"""Reject Mach-O files whose LC_SYMTAB string pool is not 8-byte aligned."""

from __future__ import annotations

import struct
import sys
from pathlib import Path


LC_SYMTAB = 0x2
MH_MAGIC_64 = 0xFEEDFACF
MH_CIGAM_64 = 0xCFFAEDFE
FAT_MAGIC = 0xCAFEBABE
FAT_CIGAM = 0xBEBAFECA


def _u32(data: bytes, offset: int, endian: str) -> int:
    return struct.unpack_from(endian + "I", data, offset)[0]


def _thin_stroff(data: bytes, base: int) -> int:
    magic = _u32(data, base, "<")
    if magic == MH_MAGIC_64:
        endian = "<"
    elif magic == MH_CIGAM_64:
        endian = ">"
    else:
        raise ValueError(f"unsupported Mach-O slice magic 0x{magic:08x}")

    ncmds = _u32(data, base + 16, endian)
    command = base + 32
    for _ in range(ncmds):
        cmd = _u32(data, command, endian)
        size = _u32(data, command + 4, endian)
        if size < 8 or command + size > len(data):
            raise ValueError("malformed Mach-O load command")
        if cmd == LC_SYMTAB:
            return _u32(data, command + 16, endian)
        command += size
    raise ValueError("Mach-O has no LC_SYMTAB")


def stroffs(path: Path) -> list[int]:
    data = path.read_bytes()
    magic = _u32(data, 0, "<")
    if magic in (MH_MAGIC_64, MH_CIGAM_64):
        return [_thin_stroff(data, 0)]
    if magic not in (FAT_MAGIC, FAT_CIGAM):
        raise ValueError("not a 64-bit Mach-O or fat Mach-O file")

    # `magic` was read little-endian above.  FAT_MAGIC therefore denotes a
    # little-endian fat header, while FAT_CIGAM denotes the usual big-endian
    # fat header.
    endian = "<" if magic == FAT_MAGIC else ">"
    count = _u32(data, 4, endian)
    offsets = []
    for index in range(count):
        entry = 8 + index * 20
        offsets.append(_u32(data, entry + 8, endian))
    return [_thin_stroff(data, offset) for offset in offsets]


def main(argv: list[str]) -> int:
    if len(argv) < 2:
        print(f"usage: {argv[0]} MACH-O ...", file=sys.stderr)
        return 2
    failed = False
    for name in argv[1:]:
        path = Path(name)
        try:
            values = stroffs(path)
        except (OSError, ValueError) as error:
            print(f"{path}: {error}", file=sys.stderr)
            failed = True
            continue
        for index, stroff in enumerate(values):
            label = f"slice {index}" if len(values) > 1 else "slice"
            if stroff % 8:
                print(f"{path}: {label} string pool offset 0x{stroff:x} is not 8-byte aligned", file=sys.stderr)
                failed = True
            else:
                print(f"{path}: {label} string pool offset 0x{stroff:x} is aligned")
    return int(failed)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
