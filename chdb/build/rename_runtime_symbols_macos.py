#!/usr/bin/env python3
"""Give the macOS libchdb.a's bundled C++ runtime names no consumer can bind to (chdb-io/chdb-rust#53).

The archive carries chDB's LLVM libc++, libc++abi and libunwind, compiled hidden
(cmake/bundled_runtime_visibility.cmake). On Mach-O, as on ELF, hidden only keeps a symbol out
of the linked image's exports: a private-extern definition in an archive member still satisfies
a reference from any other object in the same static link. So a consumer's own references went
to chDB's copies:

  Rust   std's _Unwind_GetIP bound to the bundled libunwind while _Unwind_Backtrace came from
         libSystem. On arm64 the bundled accessor returns the return address still signed, so
         every frame of a backtrace symbolized as <unknown>.
  C++    a host's __cxa_throw, personality routine, std::exception typeinfo, std::string
         members and operator new/delete bound to chDB's copies. The host's own allocations
         went through chDB's MemoryTracker and its exceptions aborted.

Linux prelinks the archive and localizes everything but the C API (prelink_static_lib.sh).
That does not carry over to ld64: `ld -r` drops MH_SUBSECTIONS_VIA_SYMBOLS as soon as one input
lacks it, after which arm64 links can no longer place branch islands in the merged __text, and
ld64 finds personality routines by name, so a localized __gxx_personality_v0 cannot be used.

So the symbols are renamed instead, in every member at once, which keeps all of chDB's own
references consistent and the archive layout unchanged. A suffix keeps the names demangleable:
`__ZTISt9exception.chdb` reads as "typeinfo for std::exception (.chdb)". Renamed are

  - every external or private-external definition in the bundled runtime's members
    (lib{cxx,cxxabi,unwind}__*, as create_static_libchdb.py names them), and
  - every other definition whose name the system C++ runtime or unwinder exports, as listed by
    the SDK's libc++, libc++abi and libunwind stubs: ClickHouse's replaceable operator
    new/delete, and std:: template members some objects instantiate.

check_static_lib_hermetic.sh checks the result against the exports of the machine it runs on.

Usage: rename_runtime_symbols_macos.py <libchdb.a> --sdk <MacOSX.sdk> [--objcopy X] [--nm X]
"""

import argparse
import os
import re
import subprocess
import sys
import tempfile

SUFFIX = ".chdb"
RUNTIME_MEMBER = re.compile(r"^lib(cxx|cxxabi|unwind)__")
SDK_STUBS = ("usr/lib/libc++.1.tbd", "usr/lib/libc++abi.tbd", "usr/lib/system/libunwind.tbd")


def tbd_symbols(path):
    """Every symbol a text-based dylib stub exports, for any architecture."""
    text = open(path).read()
    names = set()
    for block in re.finditer(r"(?:symbols|weak-symbols|thread-local-symbols)\s*:\s*\[(.*?)\]", text, re.S):
        for token in block.group(1).split(","):
            token = token.strip().strip("'\"")
            if token.startswith("_"):
                names.add(token)
    return names


def definitions(nm, archive):
    """(member, name) for every external or private-external definition in the archive."""
    out = subprocess.run([nm, "-m", "-A", "--defined-only", archive], capture_output=True, text=True, check=True).stdout
    defs = []
    for line in out.splitlines():
        if " external " not in line:
            continue
        member = line.split(":")[1] if line.count(":") >= 2 else ""
        defs.append((member, line.split()[-1]))
    if not defs:
        sys.exit(f"{nm} read no definitions out of {archive}")
    return defs


def symbol_names(nm, archive):
    """Every symbol name in the archive, defined or not."""
    out = subprocess.run([nm, "-A", archive], capture_output=True, text=True, check=True).stdout
    return {line.split()[-1] for line in out.splitlines() if line.strip() and not line.endswith(":")}


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("archive")
    parser.add_argument("--sdk", required=True, help="macOS SDK whose runtime stubs list the system exports")
    parser.add_argument("--objcopy", default=os.environ.get("OBJCOPY", "llvm-objcopy"))
    parser.add_argument("--nm", default=os.environ.get("LLVM_NM", "llvm-nm"))
    args = parser.parse_args()

    system_exports = set()
    for stub in SDK_STUBS:
        path = os.path.join(args.sdk, stub)
        if not os.path.exists(path):
            sys.exit(f"missing SDK stub {path}; the system export set would be incomplete")
        system_exports |= tbd_symbols(path)

    defs = definitions(args.nm, args.archive)
    runtime = {name for member, name in defs if RUNTIME_MEMBER.match(member)}
    if not runtime:
        sys.exit(f"no lib{{cxx,cxxabi,unwind}}__ members in {args.archive} - has the naming in create_static_libchdb.py changed?")
    if any(name.endswith(SUFFIX) for _, name in defs):
        sys.exit(f"{args.archive} already has {SUFFIX} symbols; it was renamed before")
    colliding = {name for _, name in defs if name in system_exports}
    rename = runtime | colliding

    with tempfile.TemporaryDirectory(dir=os.path.dirname(os.path.abspath(args.archive))) as tmp:
        mapping = os.path.join(tmp, "rename.map")
        with open(mapping, "w") as f:
            for name in sorted(rename):
                f.write(f"{name} {name}{SUFFIX}\n")
        renamed = os.path.join(tmp, "libchdb.a")
        subprocess.run([args.objcopy, f"--redefine-syms={mapping}", args.archive, renamed], check=True)

        # Defined or not: an old name left anywhere is a reference a consumer could satisfy.
        left = rename & symbol_names(args.nm, renamed)
        if left:
            sys.exit(f"{len(left)} renamed symbols still occur under their old name, e.g. {sorted(left)[:5]}")
        os.replace(renamed, args.archive)

    print(f"Renamed {len(rename)} symbols in {args.archive}: {len(runtime)} defined by the bundled runtime, "
          f"{len(colliding - runtime)} more exported by the system runtime")


if __name__ == "__main__":
    main()
