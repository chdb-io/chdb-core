#!/usr/bin/env bash
# Build chdb-core and run its Python tests on Python 3.14, as the Linux x86_64
# wheel workflow does, so the daily sync can check itself before it opens a pull
# request; chdb/build.sh itself builds with Python 3.9, as it requires. Run it from
# anywhere in the checkout: the first run builds everything, later runs rebuild
# what changed. Each step's full log goes to .sync/build/; a failing step prints the
# end of its log. The last line is PASS or FAIL, and .sync/build.json records the
# result, and the state it was for, for the pull request.

set -uo pipefail

cd "$(git rev-parse --show-toplevel)"
logs=.sync/build
mkdir -p "$logs"

export PATH="$HOME/.pyenv/bin:$PATH"
eval "$(pyenv init -)"
pyenv shell 3.9
if [ -f "$HOME/.cargo/env" ]; then
  . "$HOME/.cargo/env"
fi
export CC=/usr/bin/clang CXX=/usr/bin/clang++
# The pybind11 shims for the two installed versions only; the default wants all six.
export CHDB_PYBIND11_PYTHON_VERSIONS="3.9 3.14"
# vars.sh writes CHDB_VERSION into chdb.h; hand it the value the sync left there,
# or the build would rewrite the header.
CHDB_VERSION=$(sed -n 's/^#define CHDB_VERSION "\(.*\)"$/\1/p' programs/local/chdb.h)
export CHDB_VERSION

record() {  # record pass|fail [step]
  python3 - "$1" "${2:-}" <<'PY'
import hashlib, json, subprocess, sys
from pathlib import Path

result, step = sys.argv[1], sys.argv[2]
# The same hash daily_sync.py stage takes, to tell whether the sync changed later.
diff = subprocess.run(["git", "diff", "HEAD", "--binary", "--ignore-submodules=dirty"],
                      capture_output=True, check=True).stdout
tail = ""
if step:
    tail = "\n".join(Path(f".sync/build/{step}.log").read_text(errors="replace").splitlines()[-60:])
record = {"result": result, "step": step, "tree": hashlib.sha256(diff).hexdigest(), "tail": tail}
Path(".sync/build.json").write_text(json.dumps(record, indent=2) + "\n")
PY
}

step() {  # step NAME COMMAND...
  local name=$1
  shift
  echo "== $name"
  if ! "$@" > "$logs/$name.log" 2>&1; then
    echo "-- $name failed; the end of $logs/$name.log:"
    tail -n 150 "$logs/$name.log"
    record fail "$name"
    echo "FAIL: $name"
    exit 1
  fi
}

step build bash ./chdb/build.sh
step smoke bash ./chdb/test_smoke.sh
rm -rf dist
step wheel make wheel
pyenv shell 3.14
step install sh -c 'python -m pip install --force-reinstall dist/*.whl "pandas>=3.0" adbc-driver-manager==1.8.0'
step test make test
record pass
echo "PASS"
