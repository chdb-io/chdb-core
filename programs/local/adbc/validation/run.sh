#!/usr/bin/env bash
# Copyright 2026 ClickHouse, Inc. and the chDB authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Run the conformance suite with one test case per process.
#
# chDB keeps a single engine per process and does not support shutting that
# engine down and starting another one in the same process. The suite opens and
# releases a database around every test case, so running the binary directly
# puts ~100 engine lifecycles through one process. One process per case avoids
# that entirely; the engine starts in ~30 ms and the library is warm in the page
# cache after the first case, so the whole run still takes about a minute.
#
#   ./run.sh <build-dir> [extra gtest args...]
#
# CHDB_ADBC_DRIVER selects the libchdb to validate (see README.md).

set -uo pipefail

BUILD_DIR="${1:?usage: run.sh <build-dir> [gtest args...]}"
shift
readonly BIN="${BUILD_DIR}/chdb_adbc_validation"

[[ -x "${BIN}" ]] || { echo "not built: ${BIN}" >&2; exit 1; }

# Turn gtest's listing into fully qualified "Suite.Case" names.
tests="$("${BIN}" --gtest_list_tests 2>/dev/null | awk '
    /^[^ ]/  { suite = $1; next }
    /^  [^ ]/ { print suite $1 }
')"
[[ -n "${tests}" ]] || { echo "no tests found" >&2; exit 1; }

total=0 passed=0 skipped=0
failed=()
while IFS= read -r test; do
    [[ -n "${test}" ]] || continue
    total=$((total + 1))
    # </dev/null matters: without it the test binary consumes the loop's stdin
    # and the remaining test names are lost.
    output="$("${BIN}" --gtest_filter="${test}" "$@" </dev/null 2>&1)"
    if [[ $? -ne 0 ]]; then
        failed+=("${test}")
        printf 'FAIL %s\n' "${test}"
        printf '%s\n' "${output}" | sed 's/^/     | /'
    elif printf '%s' "${output}" | grep -q '\[  SKIPPED \]'; then
        skipped=$((skipped + 1))
        printf 'SKIP %s\n' "${test}"
    else
        passed=$((passed + 1))
        printf 'ok   %s\n' "${test}"
    fi
done <<< "${tests}"

printf '\n%d tests: %d passed, %d skipped, %d failed\n' \
    "${total}" "${passed}" "${skipped}" "${#failed[@]}"
if ((${#failed[@]})); then
    printf 'failed:\n'
    printf '  %s\n' "${failed[@]}"
    exit 1
fi
