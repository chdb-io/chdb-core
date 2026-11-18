#!/bin/bash
# Usage: check_static_lib_parity.sh <baseline libchdb.a> <candidate libchdb.a> [path/to/chdb.h]
#
# Checks that two static libraries of the same build register and do the same: the prelinked
# libchdb.a as the baseline and the split one (chdb-io/chdb-core#342) as the candidate.
# check_static_lib_hermetic.sh checks each archive's isolation, and a probe that links and
# answers SELECT 1; neither would notice a shard - or any member - whose only content is
# static registrations being left out of the link. Here static-probe/chdb_parity_probe.c is
# linked against each archive the way a consumer links it, with a plain -lchdb: no
# --whole-archive and no -u for the C API, so only what the archive itself pulls in is
# there. Its reports - every registry's names, and the results of plain, failing, streaming
# and Arrow queries - must be identical.
#
# Both archives must come from one build: the report has nothing version-specific written
# down, so a baseline of another build would show every change between the two as a failure.
set -euo pipefail

MY_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
PROJ_DIR="$( cd "${MY_DIR}/../.." >/dev/null 2>&1 && pwd )"

if [ $# -lt 2 ]; then
    echo "Usage: $0 <baseline libchdb.a> <candidate libchdb.a> [path/to/chdb.h]"
    exit 1
fi
BASELINE=$1
CANDIDATE=$2
CHDB_H=${3:-${PROJ_DIR}/programs/local/chdb.h}
PROBE_TIMEOUT=${PROBE_TIMEOUT:-300}
HOST_CC=${HOST_CC:-cc}

for f in "${BASELINE}" "${CANDIDATE}" "${CHDB_H}"; do
    [ -f "${f}" ] || { echo "Error: not found: ${f}"; exit 1; }
done
abs () { echo "$(cd "$(dirname "$1")" && pwd)/$(basename "$1")"; }
BASELINE=$(abs "${BASELINE}")
CANDIDATE=$(abs "${CANDIDATE}")
CHDB_H=$(abs "${CHDB_H}")

# The split archive is Linux only; the macOS archive has one layout.
if [ "$(uname)" != Linux ]; then
    echo "Error: Linux only"
    exit 1
fi
system_libs=(-lpthread -ldl -lm -lrt)

WORK_DIR=$(mktemp -d)
trap 'rm -rf "${WORK_DIR}"' EXIT

echo "Static library parity"
echo "  baseline:  ${BASELINE}"
echo "  candidate: ${CANDIDATE}"

# One directory per archive, each with its own libchdb.a, so -lchdb finds the right one.
# Built concurrently: on aarch64 a GNU ld link of either archive takes minutes.
pids=()
for side in baseline candidate; do
    dir="${WORK_DIR}/${side}"
    mkdir -p "${dir}"
    if [ "${side}" = baseline ]; then ln -s "${BASELINE}" "${dir}/libchdb.a"; else ln -s "${CANDIDATE}" "${dir}/libchdb.a"; fi
    cp "${CHDB_H}" "${dir}/chdb.h"
    cp "${MY_DIR}/static-probe/chdb_parity_probe.c" "${dir}/"
    (cd "${dir}" && "${HOST_CC}" -I. chdb_parity_probe.c -o chdb_parity_probe -L. -lchdb "${system_libs[@]}" \
        > link.log 2>&1) &
    pids+=($!)
done

failures=0
fail () { echo "FAIL: $*"; failures=$((failures + 1)); }

i=0
for side in baseline candidate; do
    dir="${WORK_DIR}/${side}"
    if ! wait "${pids[${i}]}"; then
        sed 's/^/    /' "${dir}/link.log" | tail -40
        fail "${side}: could not link chdb_parity_probe with a plain -lchdb"
    fi
    i=$((i + 1))
done
if [ "${failures}" -ne 0 ]; then
    exit 1
fi

for side in baseline candidate; do
    dir="${WORK_DIR}/${side}"
    rc=0
    (cd "${dir}" && "./chdb_parity_probe" > report.txt 2> stderr.txt) &
    probe=$!
    ( sleep "${PROBE_TIMEOUT}"; kill -9 "${probe}" 2>/dev/null ) &
    watchdog=$!
    disown "${watchdog}" 2>/dev/null || true
    wait "${probe}" || rc=$?
    kill "${watchdog}" 2>/dev/null || true
    sections=$(grep -c '^## ' "${dir}/report.txt" || true)
    lines=$(wc -l < "${dir}/report.txt" | tr -d ' ')
    if [ "${rc}" -ne 0 ]; then
        sed 's/^/    /' "${dir}/stderr.txt" | tail -20
        fail "${side}: chdb_parity_probe exited with ${rc} (killed after ${PROBE_TIMEOUT}s if it hung)"
    else
        echo "PASS (${side}: chdb_parity_probe ran its own checks; ${sections} sections, ${lines} lines)"
    fi
done

# Per registry, the sizes on both sides, so the log shows what was compared.
summarize () {
    awk '/^## / { if (name != "") printf "%s=%d ", name, n; name = substr($0, 4); gsub(/ /, "_", name); n = 0; next }
         { n++ }
         END { if (name != "") printf "%s=%d", name, n; print "" }' "$1"
}
echo "  baseline:  $(summarize "${WORK_DIR}/baseline/report.txt" | cut -c1-400)"
echo "  candidate: $(summarize "${WORK_DIR}/candidate/report.txt" | cut -c1-400)"

if [ ! -s "${WORK_DIR}/baseline/report.txt" ]; then
    fail "the baseline's report is empty - nothing would be compared"
elif diff -u "${WORK_DIR}/baseline/report.txt" "${WORK_DIR}/candidate/report.txt" > "${WORK_DIR}/report.diff"; then
    echo "PASS (the candidate's report is identical to the baseline's)"
else
    head -80 "${WORK_DIR}/report.diff" | sed 's/^/    /'
    fail "the candidate registers or answers differently from the baseline ($(grep -c '^[-+][^-+]' "${WORK_DIR}/report.diff") lines differ)"
fi

if [ "${failures}" -ne 0 ]; then
    echo "${failures} parity check(s) failed"
    exit 1
fi
echo "Static library parity passed"
