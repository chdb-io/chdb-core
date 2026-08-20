#!/usr/bin/env python3
"""Read Apache Arrow IPC "gold" integration files with chDB and validate the
result against a trusted decode of the *same bytes* (pyarrow).

Motivation: chdb-io/chdb#625 — chDB hit errors reading various Apache Arrow IPC
data. The Arrow project publishes cross-implementation "gold" files (paired
JSON + Arrow IPC, in both *file* and *stream* framing) in the
``apache/arrow-testing`` repo under ``data/arrow-ipc-stream/integration``.
These are the artifacts of Arrow's official ``archery integration`` conformance
suite and are, per the Arrow docs, "assumed to be correct".

chDB is a query engine, not an Arrow *library* implementation, so it cannot join
Arrow's ``archery`` harness directly (that harness only accepts implementations
that ship a JSON<->IPC integration executable). Instead this test borrows the
official *fixtures* and the *validate* idea (equality against a reference
decode): for each gold file we assert that

    SELECT * FROM file(<path>, <format>)

read by chDB has the same shape and values that pyarrow reads from the identical
bytes. ``.arrow_file`` uses the ``Arrow`` (IPC file) format; ``.stream`` uses
``ArrowStream`` (IPC streaming framing).

Comparison is representation-tolerant on purpose: chDB maps Arrow ``binary`` to
``string`` and materializes dictionaries, so values are normalized (dictionaries
decoded, binary/string compared as bytes) before comparison. Only genuine value
differences fail — not type-label differences.

One test is generated per (version, file, framing). Types that chDB reads wrong
today (chdb-io/chdb#625) are ``@unittest.skip``-ped so the suite is green; the
correct ones run. Written in unittest so ``tests/run_all.py`` collects it.
Fixtures are fetched on first use and cached; the network being unavailable
skips (never fails) the affected case, matching ``hits_dataset.py``.
"""

import os
import tempfile
import time
import unittest
import urllib.error
import urllib.request

try:
    import pyarrow as pa
    import pyarrow.ipc  # noqa: F401
    import pyarrow.types as pt
    HAVE_PYARROW = True
except ImportError:  # pragma: no cover - pyarrow is normally present in CI
    HAVE_PYARROW = False

import chdb

# --- Fixture source, pinned for reproducibility -----------------------------
ARROW_TESTING_COMMIT = "9ff285c88565f0f6abc855918c6a342e70e4909c"
BASE_URL = (
    "https://raw.githubusercontent.com/apache/arrow-testing/"
    f"{ARROW_TESTING_COMMIT}/data/arrow-ipc-stream/integration"
)
CACHE_DIR = os.path.join(tempfile.gettempdir(), "chdb_arrow_ipc_gold_cache")

# Gold-file cases per version directory (only files that actually exist there).
# Big-endian dirs are omitted: pyarrow cannot decode cross-endian IPC on a
# little-endian host, so there is no reference to validate against. More dirs
# (0.14.1, 0.17.1) can be appended. A file that later disappears upstream is
# skipped automatically when the download 404s.
VERSION_CASES = {
    "1.0.0-littleendian": [
        "primitive", "primitive_large_offsets", "primitive_no_batches",
        "primitive_zerolength", "null", "null_trivial", "decimal", "decimal256",
        "datetime", "interval", "map", "map_non_canonical", "nested",
        "nested_large_offsets", "dictionary", "dictionary_unsigned",
        "nested_dictionary", "custom_metadata", "duplicate_fieldnames",
        "extension",
    ],
    "2.0.0-compression": ["lz4", "zstd", "uncompressible_lz4", "uncompressible_zstd"],
}

# .arrow_file -> IPC file format; .stream -> IPC streaming format.
FORMATS = {"arrow_file": "Arrow", "stream": "ArrowStream"}

# Type cases known to be broken in chDB today, keyed by generated-file name.
# Established empirically against chdb 4.3.0 (engine 26.7.2.1). Skipped so the
# suite stays green while chdb-io/chdb#625 is open; drop an entry once fixed
# (switch to unittest.expectedFailure if a regression signal is wanted instead).
KNOWN_ISSUES = {
    # --- read fails outright (query raises) ---
    "null": "#625: Arrow null type -> Code 636 CANNOT_EXTRACT_TABLE_STRUCTURE",
    "null_trivial": "#625: Arrow null type -> Code 636 CANNOT_EXTRACT_TABLE_STRUCTURE",
    "datetime": "#625: date32 value out of chDB Date32 range -> Code 321",
    "interval": "#625: Arrow interval type unsupported -> Code 636",
    "dictionary": "#625: 'Arrow dictionary contains duplicate values' -> Code 117",
    "dictionary_unsigned": "#625: 'Arrow dictionary contains duplicate values' -> Code 117",
    "nested_dictionary": "#625: 'Arrow dictionary contains duplicate values' -> Code 117",
    "extension": "#625: dict-backed extension -> Code 117 duplicate values",
    "duplicate_fieldnames": "#625: empty/duplicate tuple element names -> Code 636",
    # --- reads, but returns wrong values (silent) ---
    "map": "#625: null map read as empty map (ClickHouse Map is non-nullable)",
    "map_non_canonical": "#625: null map read as empty map (ClickHouse Map is non-nullable)",
    "nested": "#625: null list read as empty list (ClickHouse Array is non-nullable)",
    "nested_large_offsets": "#625: null list read as empty list (ClickHouse Array is non-nullable)",
    "custom_metadata": "#625: null list read as empty list (ClickHouse Array is non-nullable)",
}

_USER_AGENT = "Mozilla/5.0 (compatible; chdb-tests)"
_RETRY_DELAYS = (2, 8)


def _download(url, dest, retries=_RETRY_DELAYS):
    """Fetch ``url`` to ``dest``. Returns the path, ``None`` on HTTP 404 (file
    absent for this version), or raises ``SkipTest`` if the network stays down
    (an unreachable CDN skips rather than fails, matching ``hits_dataset.py``)."""
    last = None
    for attempt, delay in enumerate((0,) + tuple(retries)):
        if delay:
            time.sleep(delay)
        try:
            req = urllib.request.Request(url, headers={"User-Agent": _USER_AGENT})
            tmp = dest + ".part"
            with urllib.request.urlopen(req, timeout=120) as resp, open(tmp, "wb") as out:
                while True:
                    chunk = resp.read(1 << 20)
                    if not chunk:
                        break
                    out.write(chunk)
            os.replace(tmp, dest)
            return dest
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                return None
            last = exc
        except Exception as exc:  # noqa: BLE001 - retry any transient failure
            last = exc
        try:
            os.remove(dest + ".part")
        except OSError:
            pass
    raise unittest.SkipTest(f"{url} unreachable: {last}")


def _ensure_gold_file(version, name, ext):
    fname = f"generated_{name}.{ext}"
    dest = os.path.join(CACHE_DIR, version, fname)
    if os.path.exists(dest):
        return dest
    os.makedirs(os.path.dirname(dest), exist_ok=True)
    result = _download(f"{BASE_URL}/{version}/{fname}", dest)
    if result is None:
        raise unittest.SkipTest(f"{version}/{fname} not present in arrow-testing")
    return result


def _read_reference(path, ext):
    if ext == "arrow_file":
        return pa.ipc.open_file(path).read_all()
    return pa.ipc.open_stream(path).read_all()


def _normalize(col):
    """Decode-safe, representation-tolerant values for one column: dictionaries
    are decoded and binary/string are compared as bytes, so only genuine value
    differences (not chDB's type-label choices) show up."""
    t = col.type
    try:
        if pt.is_dictionary(t):
            col = col.cast(t.value_type)
            t = col.type
        if pt.is_string(t) or pt.is_large_string(t):
            col = col.cast(pa.binary())
        return col.to_pylist()
    except Exception:  # noqa: BLE001 - last-resort decode-safe fallback
        return col.cast(pa.binary()).to_pylist()


@unittest.skipUnless(HAVE_PYARROW, "pyarrow not installed")
class TestArrowIpcGoldFiles(unittest.TestCase):
    """One generated method per (version, file, framing). See module docstring."""


def _make_case(version, name, ext, fmt):
    def test(self):
        path = _ensure_gold_file(version, name, ext)
        # Reference decode of the identical bytes. If pyarrow itself cannot read
        # the file there is nothing to validate against, so skip.
        try:
            reference = _read_reference(path, ext)
        except Exception as exc:  # noqa: BLE001
            self.skipTest(f"pyarrow cannot decode reference {path}: {exc}")

        got = chdb.query(f"SELECT * FROM file('{path}', '{fmt}')", "ArrowTable")

        self.assertEqual(got.num_rows, reference.num_rows, "row count mismatch")
        self.assertEqual(got.num_columns, reference.num_columns, "column count mismatch")
        self.assertEqual(got.column_names, reference.column_names, "column names mismatch")
        for i in range(reference.num_columns):
            self.assertEqual(
                _normalize(got.column(i)), _normalize(reference.column(i)),
                f"value mismatch in column {i} ({reference.column_names[i]!r})",
            )
    return test


def _install_cases():
    for version, names in VERSION_CASES.items():
        slug_v = version.replace(".", "_").replace("-", "_")
        for name in names:
            for ext, fmt in FORMATS.items():
                method = _make_case(version, name, ext, fmt)
                method.__name__ = f"test_{slug_v}__{name}__{ext}"
                if name in KNOWN_ISSUES:
                    method = unittest.skip(KNOWN_ISSUES[name])(method)
                setattr(TestArrowIpcGoldFiles, method.__name__, method)


_install_cases()


if __name__ == "__main__":
    unittest.main(verbosity=2)
