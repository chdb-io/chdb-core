#!/usr/bin/env python3
"""Gate 3a: the two checked-in export allow-lists must describe the same C API contract.

`chdb/libchdb_export.map` (Linux version script) and `chdb/libchdb_export_macos.txt`
(macOS -exported_symbols_list) are maintained by hand and drift apart easily. They are the
only checked-in statement of what the public C ABI is, so everything downstream - including
the static-library reachability gate - reads the contract from here rather than hard-coding
a symbol count.

Prints the contract, one unprefixed symbol per line, to stdout. Diagnostics go to stderr,
so `check_export_contract.py > contract.txt` is safe.
"""

import argparse
import os
import re
import sys

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DEFAULT_MAP = os.path.join(REPO_ROOT, "chdb", "libchdb_export.map")
DEFAULT_MACOS = os.path.join(REPO_ROOT, "chdb", "libchdb_export_macos.txt")


def parse_version_script(path):
    """Symbols listed in the `global:` section of a linker version script."""
    text = re.sub(r"/\*.*?\*/", "", open(path).read(), flags=re.DOTALL)
    match = re.search(r"\bglobal:(.*?)\blocal:", text, flags=re.DOTALL)
    if not match:
        raise SystemExit(f"{path}: no 'global: ... local:' section found")
    return set(re.findall(r"([A-Za-z_][A-Za-z0-9_]*)\s*;", match.group(1)))


def parse_exported_symbols_list(path):
    """Symbols in a Mach-O -exported_symbols_list, with the leading underscore removed."""
    symbols = set()
    for lineno, line in enumerate(open(path), 1):
        line = line.split("#", 1)[0].strip()
        if not line:
            continue
        if not line.startswith("_"):
            raise SystemExit(f"{path}:{lineno}: Mach-O symbol must start with '_': {line}")
        symbols.add(line[1:])
    return symbols


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--map", default=DEFAULT_MAP)
    parser.add_argument("--macos", default=DEFAULT_MACOS)
    args = parser.parse_args()

    linux = parse_version_script(args.map)
    macos = parse_exported_symbols_list(args.macos)

    if linux != macos:
        for symbol in sorted(linux - macos):
            print(f"  only in {args.map}: {symbol}", file=sys.stderr)
        for symbol in sorted(macos - linux):
            print(f"  only in {args.macos}: {symbol}", file=sys.stderr)
        raise SystemExit(
            "Export allow-lists disagree. Every public chdb_* symbol declared CHDB_EXPORT in\n"
            "programs/local/chdb.h must be listed in both files.")

    print(f"C API contract: {len(linux)} symbols, both allow-lists agree", file=sys.stderr)
    for symbol in sorted(linux):
        print(symbol)


if __name__ == "__main__":
    main()
