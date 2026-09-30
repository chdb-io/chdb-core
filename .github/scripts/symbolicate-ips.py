#!/usr/bin/env python3
"""Print macOS .ips crash reports with every frame resolved to file:line.

    symbolicate-ips.py <directory-of-ips-files>

A .ips carries each frame as an image index plus an offset, and for a stripped
library that is all it carries: the reader gets `_chdb.abi3.so +0x56c2800` and
nothing else.  The dSYM sitting in the workspace turns that into a function and
a line, and it is the only copy whose UUID matches the build that crashed, so
this has to happen on the runner and not afterwards.

Every thread is resolved, not only the one that took the signal.  A crash in a
thread pool says more about the pool than about the thread that noticed.

atos is invoked once per image rather than once per frame.  The cost is loading
the DWARF -- around ten seconds for a multi-gigabyte one -- and does not grow
with the number of addresses, so per-frame calls would turn a forty-frame stack
into ten minutes and a hundred threads into hours.
"""

import functools
import glob
import json
import os
import re
import subprocess
import sys

MAX_RAW = 200_000


def dwarf_files():
    """dSYM payloads in the workspace, keyed by the image name they describe."""
    return {
        os.path.basename(p): p
        for p in glob.glob("*.dSYM/Contents/Resources/DWARF/*")
    }


@functools.lru_cache(maxsize=None)
def dwarf_uuid(path):
    """Build id of a dSYM payload, or None if dwarfdump cannot say."""
    try:
        out = subprocess.run(["dwarfdump", "--uuid", path],
                             capture_output=True, text=True, timeout=300).stdout
    except Exception:
        return None
    m = re.search(r"UUID:\s*([0-9A-Fa-f-]{36})", out)
    return m.group(1).lower() if m else None


def resolve(dwarfs, images, threads):
    """Offsets to resolve, per image, skipping any dSYM that is not this build.

    A wheel job replaces the extension between the test that crashed and this
    step -- the lite variant is swapped in over the full one -- while the dSYM
    left in the workspace still describes the build that was there before.
    Resolving one against the other yields function names and line numbers that
    are entirely plausible and entirely wrong, which is worse than the raw
    offset it replaced. So the crash report's own build id has to agree with the
    dSYM's before a single address is sent to atos.
    """
    wanted = {}
    for t in threads:
        for f in t.get("frames", []):
            idx = f.get("imageIndex")
            if not isinstance(idx, int) or idx >= len(images):
                continue
            image = images[idx]
            name = image.get("name") or ""
            if name not in dwarfs:
                continue
            want, have = (image.get("uuid") or "").lower(), dwarf_uuid(dwarfs[name])
            if want and have and want != have:
                continue
            wanted.setdefault(name, set()).add(f.get("imageOffset", 0))

    resolved = {}
    for name, offsets in wanted.items():
        offsets = sorted(offsets)
        print("  atos: %d addresses in %s" % (len(offsets), name))
        try:
            out = subprocess.run(
                ["atos", "-o", dwarfs[name], "-offset"] + [hex(o) for o in offsets],
                capture_output=True,
                text=True,
                timeout=1800,
            ).stdout.strip().split("\n")
        except Exception as exc:  # atos missing, dSYM unreadable, timeout
            print("  atos failed for %s: %s" % (name, exc))
            continue
        for offset, line in zip(offsets, out):
            resolved[(name, offset)] = line
    return resolved


def report(path):
    raw = open(path, errors="replace").read()
    print("=" * 72)
    print("===", path)
    print("=" * 72)

    # A .ips is a one-line JSON header followed by a JSON body.
    try:
        body = json.loads(raw.split("\n", 1)[1])
    except Exception as exc:
        print("  body not parseable (%s); raw report follows" % exc)
        print(raw[:MAX_RAW])
        return

    for key in ("procName", "pid", "parentProc", "exception", "termination",
                "asi", "asiBacktraces", "vmRegionInfo"):
        if key in body:
            print("  %s: %s" % (key, body[key]))

    images = body.get("usedImages", [])
    threads = body.get("threads", [])
    dwarfs = dwarf_files()
    for image in images:
        name = image.get("name") or ""
        if name not in dwarfs:
            continue
        want, have = (image.get("uuid") or "").lower(), dwarf_uuid(dwarfs[name])
        if want and have and want != have:
            print("  %s: crash report is build %s but the dSYM in the workspace "
                  "is %s, so its frames stay as offsets" % (name, want, have))
    resolved = resolve(dwarfs, images, threads)

    for t in threads:
        mark = "   <<< TRIGGERED" if t.get("triggered") else ""
        print("--- thread %s %s%s" % (t.get("id"), t.get("name", ""), mark))
        for i, f in enumerate(t.get("frames", [])):
            idx = f.get("imageIndex")
            img = images[idx] if isinstance(idx, int) and idx < len(images) else {}
            name = img.get("name") or "?"
            offset = f.get("imageOffset", 0)
            line = "  %3d  %s +0x%x" % (i, name, offset)
            if f.get("symbol"):
                line += "  " + f["symbol"]
            hit = resolved.get((name, offset))
            if hit:
                line += "  ->  " + hit
            print(line)


def main():
    directory = sys.argv[1] if len(sys.argv) > 1 else "crash-diagnostics"
    reports = sorted(glob.glob(os.path.join(directory, "*.ips")))
    if not reports:
        print("no .ips files in", directory)
        return
    print("dSYM payloads found:", dwarf_files() or "(none -- frames stay as offsets)")
    for path in reports:
        report(path)


if __name__ == "__main__":
    main()
