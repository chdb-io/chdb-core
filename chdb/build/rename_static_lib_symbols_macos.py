#!/usr/bin/env python3
"""Give every symbol of the macOS libchdb.a outside the C API a name no consumer can bind to
(chdb-io/chdb-rust#53).

On Mach-O, as on ELF, hidden visibility only keeps a symbol out of the linked image's exports:
a private-extern definition in an archive member still satisfies a reference from any other
object in the same static link. So a consumer's own references went to chDB's copies:

  Rust   std's _Unwind_GetIP bound to the bundled libunwind while _Unwind_Backtrace came from
         libSystem. On arm64 the bundled accessor returns the return address still signed, so
         every frame of a backtrace symbolized as <unknown>.
  C++    a host's __cxa_throw, personality routine, std::exception typeinfo, std::string
         members and operator new/delete bound to chDB's copies. The host's own allocations
         went through chDB's MemoryTracker and its exceptions aborted.

The same held for everything else the archive bundles. A host that linked its own zstd, zlib
or OpenSSL failed with duplicate symbols or bound to chDB's copy, of whatever version, and an
inline function or template instantiation the host shares with chDB was coalesced into one
copy for both.

Linux prelinks the archive and localizes everything but the C API (prelink_static_lib.sh).
That does not carry over to ld64: `ld -r` drops MH_SUBSECTIONS_VIA_SYMBOLS as soon as one input
lacks it, after which arm64 links can no longer place branch islands in the merged __text, and
ld64 finds personality routines by name, so a localized __gxx_personality_v0 cannot be used.

So the symbols are renamed instead, in every member at once, which keeps all of chDB's own
references consistent and the archive layout unchanged. Every external or private-external
definition outside the C API contract (check_export_contract.py) gets a suffix, which keeps
the names demangleable: `__ZTISt9exception.chdb` reads as "typeinfo for std::exception (.chdb)".

References to names the archive does not define keep theirs: they are what chDB imports from
the system. Unlike on Linux, no optional hook hides among them. ld64 rejects a weak reference
nothing in the link defines, so each one the archive makes goes either to a definition inside
it, renamed along with it, or to a system export (__darwin_check_fd_set_overflow).

check_static_lib_hermetic.sh checks that only the C API kept its name.

Usage: rename_static_lib_symbols_macos.py <libchdb.a> [--objcopy X] [--nm X]
"""

import argparse
import os
import subprocess
import sys
import tempfile

SUFFIX = ".chdb"
MY_DIR = os.path.dirname(os.path.abspath(__file__))


def contract():
    """The C API, in Mach-O names."""
    out = subprocess.run([sys.executable, os.path.join(MY_DIR, "check_export_contract.py")],
                         capture_output=True, text=True, check=True).stdout
    names = {"_" + name for name in out.split()}
    if not names:
        sys.exit("the C API contract is empty")
    return names


def symbols(nm, archive):
    """(defined, all) external and private-external symbol names in the archive."""
    out = subprocess.run([nm, "-m", archive], capture_output=True, text=True, check=True).stdout
    defined, every = set(), set()
    for line in out.splitlines():
        # "<value> (<section>) [weak] [private] external <name>", without the value when
        # undefined. A member's header line ends with a colon.
        fields = line.split()
        if len(fields) < 3 or line.endswith(":") or "external" not in fields[:-1]:
            continue
        every.add(fields[-1])
        if not fields[0].startswith("("):
            defined.add(fields[-1])
    return defined, every


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("archive")
    parser.add_argument("--objcopy", default=os.environ.get("OBJCOPY", "llvm-objcopy"))
    parser.add_argument("--nm", default=os.environ.get("LLVM_NM", "llvm-nm"))
    args = parser.parse_args()

    keep = contract()
    defined, _ = symbols(args.nm, args.archive)
    if not defined:
        sys.exit(f"{args.nm} read no definitions out of {args.archive}")
    if any(name.endswith(SUFFIX) for name in defined):
        sys.exit(f"{args.archive} already has {SUFFIX} symbols; it was renamed before")
    missing = keep - defined
    if missing:
        sys.exit(f"{len(missing)} C API symbols are not defined in {args.archive}, e.g. {sorted(missing)[:5]}")
    rename = defined - keep

    with tempfile.TemporaryDirectory(dir=os.path.dirname(os.path.abspath(args.archive))) as tmp:
        mapping = os.path.join(tmp, "rename.map")
        with open(mapping, "w") as f:
            for name in sorted(rename):
                f.write(f"{name} {name}{SUFFIX}\n")
        renamed = os.path.join(tmp, "libchdb.a")
        subprocess.run([args.objcopy, f"--redefine-syms={mapping}", args.archive, renamed], check=True)

        # Defined or not: an old name left anywhere is a reference a consumer could satisfy.
        _, every = symbols(args.nm, renamed)
        left = rename & every
        if left:
            sys.exit(f"{len(left)} renamed symbols still occur under their old name, e.g. {sorted(left)[:5]}")
        os.replace(renamed, args.archive)

    print(f"Renamed {len(rename)} symbols in {args.archive}; the {len(keep)} C API symbols keep their names")


if __name__ == "__main__":
    main()
