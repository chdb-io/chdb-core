#!/bin/bash
set -euo pipefail

cd "$(dirname "$0")"
test_dir=$(mktemp -d)
trap 'rm -f "$test_dir/host-test" "$test_dir/abi-test" "$test_dir/v1-client.o"; rmdir "$test_dir"' EXIT

test_flags=(-Wall -Wextra -Werror)
if [[ -n "${CHDB_TEST_SANITIZE:-}" ]]; then
    test_flags+=(-fsanitize="${CHDB_TEST_SANITIZE}" -fno-sanitize-recover=all)
fi

${CC:-clang} -std=c11 "${test_flags[@]}" \
    -c abi/objectStorageV1Fixture.c -o "$test_dir/v1-client.o"
${CXX:-clang++} -std=c++20 "${test_flags[@]}" \
    objectStorageAbiTest.cpp "$test_dir/v1-client.o" \
    -I../programs/local -o "$test_dir/abi-test"
"$test_dir/abi-test"

${CC:-clang} -std=c11 -D_GNU_SOURCE "${test_flags[@]}" \
    objectStorageMemStoreTest.c objectStorageMemStore.c -I../programs/local -lpthread \
    -o "$test_dir/host-test"
"$test_dir/host-test"
CHDB_TEST_SERVICE_THREAD=1 "$test_dir/host-test"
