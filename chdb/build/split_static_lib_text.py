#!/usr/bin/env python3
"""Spread the code of the prelinked aarch64 libchdb.o over output sections that the AArch64
branch range can span (prelink_static_lib.sh).

A BL reaches +-128 MB. A consumer's linker routes a call it cannot reach directly through a
stub, and GNU ld's aarch64 backend decides where stubs go before it knows how many there will
be: it cuts each output section into groups of input sections at most 127 MB long and puts the
stubs of a group right after it, which leaves them 1 MB. libchdb.o has over 300 MB of code in
more than half a million sections, and a consumer's default linker script puts all of it into
its .text. Once one group's stubs outgrew that megabyte, the calls at the far ends of the
group could no longer reach them, and every C, C++ and Rust consumer linked with GNU ld
(binutils 2.42, Ubuntu 24.04) failed with "relocation truncated to fit: R_AARCH64_CALL26".

So this prints llvm-objcopy options that rename the code sections, in section order, into
equal runs of at most 64 MB: .chdb.text.0, .chdb.text.1, ... No default linker script names
them, so every linker gives each run an output section of its own, and GNU ld makes each one
a single group with its stubs at the end, at least 64 MB within reach of every call in it.
Every function keeps its own section, so --gc-sections still drops what a consumer does not
use.

Merging each run into one section would do for GNU ld too, but mold 2.30 (Ubuntu 24.04) then
links binaries that crash: its range-extension thunks are per symbol, and a call that the
merge turned into "section + offset" lands at "thunk + offset". --gc-sections could not drop
anything either.

Prints nothing when the code fits into one run.
"""

import argparse
import math
import struct
import sys

SHT_PROGBITS = 1
SHF_ALLOC = 0x2
SHF_EXECINSTR = 0x4
SECTION_HEADER = struct.Struct("<IIQQQQIIQQ")
MB = 1 << 20


def code_sections(path):
    """The .text and .text.* sections of an ELF64 little-endian relocatable object, in
    section order, as (name, offset) with the offset each gets when they are laid out back
    to back, and the end of that layout."""
    with open(path, "rb") as f:
        header = f.read(64)
        if header[:4] != b"\x7fELF" or header[4] != 2 or header[5] != 1:
            raise SystemExit(f"{path}: not an ELF64 little-endian object")
        (shoff,) = struct.unpack_from("<Q", header, 0x28)
        shentsize, shnum, shstrndx = struct.unpack_from("<HHH", header, 0x3A)
        if shentsize != SECTION_HEADER.size:
            raise SystemExit(f"{path}: unexpected section header size {shentsize}")
        f.seek(shoff)
        first = SECTION_HEADER.unpack(f.read(shentsize))
        # Past 0xff00 sections the count and the name table's index move into section 0.
        if shnum == 0:
            shnum = first[5]
        if shstrndx == 0xFFFF:
            shstrndx = first[6]
        f.seek(shoff)
        table = f.read(shnum * shentsize)
        names_header = SECTION_HEADER.unpack_from(table, shstrndx * shentsize)
        f.seek(names_header[4])
        names = f.read(names_header[5])

    sections, end = [], 0
    for name_offset, sh_type, flags, _, _, size, _, _, align, _ in SECTION_HEADER.iter_unpack(table):
        if sh_type != SHT_PROGBITS or flags & (SHF_ALLOC | SHF_EXECINSTR) != SHF_ALLOC | SHF_EXECINSTR:
            continue
        name = names[name_offset:names.index(b"\0", name_offset)].decode()
        if name != ".text" and not name.startswith(".text."):
            continue
        # An llvm-objcopy response file splits on whitespace and --rename-section on '='.
        if any(c.isspace() or c in "=\"'\\" for c in name):
            raise SystemExit(f"{path}: cannot rename section {name!r}")
        align = max(align, 1)
        start = -(-end // align) * align
        sections.append((name, start))
        end = start + size
    return sections, end


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("object", help="the prelinked object")
    parser.add_argument("--max-mb", type=float, default=64, help="the longest run, in MB (default: 64)")
    parser.add_argument("--prefix", default=".chdb.text.", help="the name of the runs, numbered from 0")
    args = parser.parse_args()

    sections, end = code_sections(args.object)
    runs = math.ceil(end / (args.max_mb * MB))
    if runs <= 1:
        print(f"split_static_lib_text: {len(sections)} code sections, {end / MB:.1f} MB: nothing to split",
              file=sys.stderr)
        return
    length = end / runs
    sizes = [0] * runs
    for i, (name, start) in enumerate(sections):
        run = min(int(start // length), runs - 1)
        print(f"--rename-section={name}={args.prefix}{run}")
        sizes[run] += (sections[i + 1][1] if i + 1 < len(sections) else end) - start
    print(f"split_static_lib_text: {len(sections)} code sections, {end / MB:.1f} MB, into {runs} output sections "
          f"{args.prefix}0-{runs - 1} of " + ", ".join(f"{size / MB:.1f}" for size in sizes) + " MB",
          file=sys.stderr)


if __name__ == "__main__":
    main()
