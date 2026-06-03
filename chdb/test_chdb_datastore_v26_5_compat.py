#!/usr/bin/env python3
"""
chdb-io v4.1.8 vs chdb-core (post-v26.5 sync) compatibility shim.

Apply in-place edits to a freshly-cloned chdb-io tree so its datastore tests
can run against the current chdb-core baseline.  Invoked by
chdb/test_chdb_datastore.sh after `git clone`; the resulting tests live in
the temporary workdir and are discarded with it.

This shim is a **temporary** unblocker.  The real fix belongs in chdb-io
itself — once chdb-io ships a release containing the marker change, the
shim's needles will no longer match and it will warn-skip into a no-op.
Designed to be **idempotent and tolerant**: every replace is guarded by an
`old in text` check, missing needles log `skip: ...` rather than failing.

Phase ladder:
  - phase 1 (now)         : shim finds needle → patches → CI passes
  - phase 2 (chdb-io PR
    merged + released)    : shim finds nothing → skips → CI passes via real fix
  - phase 3 (cleanup)     : remove this file + its invocation in the .sh script

Single patch currently encoded (after the v26.5 main rebase that brought
in chdb-core fix `25c9b73` for the BLOB roundtrip behaviour, the three
BLOB-test xfail decorators previously patched here are no longer needed
and have been removed from this shim):

  - xfail_markers.py — the `chdb_array_nullable` marker uses strict=True
    to assert that Array(T) inside Nullable is broken.  ClickHouse fixed
    that limitation in v26.5, so two tests using `splitByWhitespace`,
    `str.findall` etc. on nullable columns now unexpectedly pass.  We
    flip strict to False so XPASS is treated as the upstream fix landing
    rather than a test-framework regression.
"""

import sys
from pathlib import Path

CHDB_SRC = Path(sys.argv[1])


def maybe_replace(path: Path, old: str, new: str, label: str) -> None:
    """Replace `old` with `new` in `path`, but skip-with-warning if missing."""
    text = path.read_text()
    if old in text:
        path.write_text(text.replace(old, new))
        print(f"patched: {path} [{label}]")
    else:
        print(f"skip:    {path} [{label}] — needle not present (already fixed upstream?)")


# --- xfail_markers.py — drop strict on chdb_array_nullable ---
markers = CHDB_SRC / "datastore/tests/xfail_markers.py"
maybe_replace(
    markers,
    old=(
        'chdb_array_nullable = pytest.mark.xfail(\n'
        '    reason="chDB: Array type cannot be inside Nullable type",\n'
        '    strict=True,\n'
        ')'
    ),
    new=(
        'chdb_array_nullable = pytest.mark.xfail(\n'
        '    # Further fixed by ClickHouse upstream in v26.5: Array(T) inside Nullable is now\n'
        '    # permitted natively, so tests using splitByWhitespace, str.findall etc. on\n'
        '    # nullable columns succeed.  Switching strict=False so XPASS does not fail.\n'
        '    reason="chDB: Array type cannot be inside Nullable type (fixed in chdb-core v26.5)",\n'
        '    strict=False,\n'
        ')'
    ),
    label="chdb_array_nullable strict=False",
)
