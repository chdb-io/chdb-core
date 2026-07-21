#!/bin/bash
set -e

# Build, audit, and test free-threading wheels on Linux (native).
# Driven by the build_ft_wheels.yml workflow. Caller is responsible for installing
# system packages, Rust, clang, pyenv, ccache, and patchelf before invoking.
#
# Required env vars:
#   AUDITWHEEL_PLAT  — e.g. manylinux2014_x86_64, manylinux_2_17_aarch64
#   PATCHELF_ARCH    — e.g. x86_64, aarch64
#
# Optional env vars:
#   FT_VERSIONS      — space-separated list (default: "3.13t 3.14t")

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJ_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"

: "${AUDITWHEEL_PLAT:?AUDITWHEEL_PLAT is required}"
: "${PATCHELF_ARCH:?PATCHELF_ARCH is required}"
: "${FT_VERSIONS:=3.13t 3.14t}"

export PATH="$HOME/.pyenv/bin:$PATH"
eval "$(pyenv init -)"
source ~/.cargo/env 2>/dev/null || true

cd "$PROJ_DIR"

# ── 1. Install free-threading Python versions ────────────────────
# pyenv's `:latest` syntax doesn't support the "t" suffix, so we resolve manually
for ft_version in $FT_VERSIONS; do
    base_ver=${ft_version%t}  # 3.13t → 3.13
    ft_latest=$(pyenv install --list | grep -E "^\s*${base_ver}\.[0-9].*t$" | tail -1 | tr -d ' ')
    if [ -z "$ft_latest" ]; then
        echo "ERROR: no free-threading Python matching ${ft_version} found in pyenv" >&2
        exit 1
    fi
    echo "Resolved ${ft_version} → ${ft_latest}"
    # Retry pyenv install — python.org occasionally times out / 403s.
    for attempt in 1 2 3; do
        if pyenv install "$ft_latest" -s; then
            break
        fi
        if [ "$attempt" = "3" ]; then
            echo "ERROR: pyenv install $ft_latest failed after 3 attempts" >&2
            exit 1
        fi
        echo "pyenv install $ft_latest failed (attempt $attempt/3); retrying in $((attempt * 10))s..."
        sleep $((attempt * 10))
    done
done

for ft_version in $FT_VERSIONS; do
    ft_full=$(pyenv versions --bare | grep "^${ft_version%t}\." | grep 't$' | head -1)
    if [ -n "$ft_full" ]; then
        echo "Installing deps for Python $ft_full"
        PYENV_VERSION=$ft_full python -m pip install --upgrade pip
        PYENV_VERSION=$ft_full python -m pip install setuptools wheel tox pandas pyarrow psutil adbc-driver-manager==1.8.0
    fi
done

# ── 2. Build ft wheels ───────────────────────────────────────────
mkdir -p ft_dist

for ft_version in $FT_VERSIONS; do
    ft_full=$(pyenv versions --bare | grep "^${ft_version%t}\." | grep 't$' | head -1)
    if [ -z "$ft_full" ]; then
        echo "ERROR: Python $ft_version not available" >&2
        exit 1
    fi

    echo "=============================================="
    echo "Building FT wheel: Python $ft_full ($ft_version)"
    echo "=============================================="

    export CHDB_FREE_THREADING=1
    export CHDB_FREE_THREADING_PYTHON_VERSION=$ft_version
    export PYENV_VERSION=$ft_full
    export CC=/usr/bin/clang
    export CXX=/usr/bin/clang++

    bash chdb/build.sh Release

    rm -rf dist/
    python -m pip install "build[virtualenv]" -q
    python -m build --wheel

    mv dist/*.whl ft_dist/ 2>/dev/null || true

    unset PYENV_VERSION CHDB_FREE_THREADING CHDB_FREE_THREADING_PYTHON_VERSION
done

echo "Built FT wheels:"
ls -lh ft_dist/

# ── 3. Audit wheels (patchelf + auditwheel) ──────────────────────
if ! command -v patchelf &>/dev/null; then
    # Extract into a fresh dir; /tmp's sticky bit makes tar fail when restoring
    # metadata on the extraction root.
    mkdir -p /tmp/patchelf-install
    wget -q "https://github.com/NixOS/patchelf/releases/download/0.18.0/patchelf-0.18.0-${PATCHELF_ARCH}.tar.gz" -O /tmp/patchelf-install/patchelf.tar.gz
    tar -xf /tmp/patchelf-install/patchelf.tar.gz -C /tmp/patchelf-install
    sudo cp /tmp/patchelf-install/bin/patchelf /usr/bin/
    sudo chmod +x /usr/bin/patchelf
fi
patchelf --version

ft_full=$(pyenv versions --bare | grep 't$' | head -1)
if [ -n "$ft_full" ]; then
    PYENV_VERSION=$ft_full python -m pip install auditwheel
fi

mkdir -p ft_dist/audited
for whl in ft_dist/*.whl; do
    [ -f "$whl" ] || continue
    PYENV_VERSION=$ft_full auditwheel -v repair "$whl" -w ft_dist/audited/ --plat "$AUDITWHEEL_PLAT"
done
if ls ft_dist/audited/*.whl &>/dev/null; then
    mv ft_dist/audited/*.whl ft_dist/
fi
rm -rf ft_dist/audited
rm -f ft_dist/*-linux_*.whl

echo "Final FT wheels:"
ls -lh ft_dist/

# ── 4. Test ft wheels ────────────────────────────────────────────
for ft_version in $FT_VERSIONS; do
    ft_full=$(pyenv versions --bare | grep "^${ft_version%t}\." | grep 't$' | head -1)
    if [ -z "$ft_full" ]; then
        echo "ERROR: Python $ft_version not available" >&2
        exit 1
    fi

    py_tag="cp${ft_version//./}"
    whl=$(ls ft_dist/*"${py_tag}"*.whl 2>/dev/null | head -1)
    if [ -z "$whl" ]; then
        echo "ERROR: No wheel for $ft_version (tag $py_tag)" >&2
        exit 1
    fi

    echo "=============================================="
    echo "Testing FT wheel on Python $ft_full"
    echo "  Wheel: $whl"
    echo "=============================================="

    export PYENV_VERSION=$ft_full
    python -m pip install "$whl" --force-reinstall
    # Tests skipped on FT only:
    #   - test_arrow_record_reader_deltalake: deltalake wheel has no FT build
    #   - test_on_df: uses urllib.request.URLopener, removed in Python 3.14.6t
    #   - test_arrow_table_queries / test_dataframe_large_scale_2 / test_state2_dataframe:
    #     all download the same 122MB hits_0.parquet from datasets.clickhouse.com,
    #     which returns HTTP 403 from this runner's egress IP.
    skip_ft_tests=(
        test_arrow_record_reader_deltalake
        test_on_df
        test_arrow_table_queries
        test_dataframe_large_scale_2
        test_state2_dataframe
    )
    for t in "${skip_ft_tests[@]}"; do
        mv "tests/${t}.py" "tests/${t}.py.skip" 2>/dev/null || true
    done
    set +e
    make test
    test_rc=$?
    set -e
    for t in "${skip_ft_tests[@]}"; do
        mv "tests/${t}.py.skip" "tests/${t}.py" 2>/dev/null || true
    done
    if [ "$test_rc" -ne 0 ]; then
        echo "ERROR: make test failed (rc=$test_rc) on free-threading Python $ft_version" >&2
        exit "$test_rc"
    fi
    echo "✓ Tests PASSED on free-threading Python $ft_version"
    unset PYENV_VERSION
done

echo "=============================================="
echo "All FT builds complete. Wheels in ft_dist/:"
ls -lh ft_dist/
