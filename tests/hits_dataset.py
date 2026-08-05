"""Shared access to the `hits_0.parquet` fixture several tests read.

The file is fetched on first use and reused afterwards. Two details matter for
CI: the CDN answers urllib's default User-Agent with 403, so a real one is
sent, and a transient failure is retried before the test is skipped rather than
reported as an error.
"""

import os
import time
import unittest
import urllib.error
import urllib.request

HITS_URL = (
    "https://datasets.clickhouse.com/hits_compatible/athena_partitioned/"
    "hits_0.parquet"
)
HITS_FILE = "hits_0.parquet"

_USER_AGENT = "Mozilla/5.0 (compatible; chdb-tests)"
_RETRY_DELAYS = (2, 8, 20)


def download(url, dest, retries=_RETRY_DELAYS):
    """Fetch `url` to `dest`, retrying transient failures. Raises SkipTest if
    the dataset stays unreachable so an unavailable CDN doesn't fail the run."""
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
        except Exception as exc:  # noqa: BLE001 - retry any fetch failure
            last = exc
            print(f"[hits_dataset] fetch failed ({exc}); attempt {attempt + 1}")
            try:
                os.remove(dest + ".part")
            except OSError:
                pass
    raise unittest.SkipTest(f"{url} unreachable: {last}")


def ensure_hits_parquet(path=HITS_FILE):
    """Return the path to hits_0.parquet, downloading it if needed."""
    if os.path.exists(path):
        return path
    print(f"Downloading {path}...")
    download(HITS_URL, path)
    print("Download complete!")
    return path
