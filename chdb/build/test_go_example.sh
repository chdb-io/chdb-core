#!/bin/bash
set -e

# Get script directory
MY_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJ_DIR="$(cd "${MY_DIR}/../.." && pwd)"

# Allow custom library path
LIBCHDB_PATH="${1:-${MY_DIR}/libchdb_minimal.a}"

echo "Testing with Go example..."
echo "Using library: ${LIBCHDB_PATH}"

# Prepare go-example directory
echo "Preparing go-example directory..."
cd ${MY_DIR}/go-example

# Copy library and header
if [ -f "${LIBCHDB_PATH}" ]; then
    cp "${LIBCHDB_PATH}" ./libchdb.a
else
    echo "Error: Library not found: ${LIBCHDB_PATH}"
    exit 1
fi

cp ${PROJ_DIR}/programs/local/chdb.h .
echo "Copied library as libchdb.a and chdb.h to go-example directory"

# Allow the macOS -Wl,-force_load,<path> (single dash) and Linux
# -Wl,--whole-archive / -Wl,--no-whole-archive (double dash) cgo LDFLAGS
# patterns through Go's default cgo security filter.  These are needed by
# cgo_{darwin,linux}.go so SQL functions registered via file-scope static
# initializers (e.g. FunctionMD5.cpp's REGISTER_FUNCTION(MD5)) survive the final
# cgo->ld dead-strip on static libchdb.a consumers.
export CGO_LDFLAGS_ALLOW='-Wl,(-force_load,.*|--whole-archive|--no-whole-archive)'

# Build with core dumps off, then run. Apple's ld (Xcode 15.4) segfaults in the trial
# link cgo makes against this multi-gigabyte archive. go expects that link to fail at times
# and carries on, but under the CI step's unlimited core limit the crash leaves a 4.5 GB
# core behind, enough to fill the macOS runner's disk later in the job. The example itself
# runs under the caller's core limit, so a crash in chDB still leaves its core.
BUILD_DIR=$(mktemp -d)
trap 'rm -rf "${BUILD_DIR}"' EXIT
echo "Building Go example..."
# GO_BUILD_TAGS=chdb_plain_link links with a plain -lchdb (go-example/cgo_linux_plain.go).
echo "Build tags: ${GO_BUILD_TAGS:-none}"
(ulimit -c 0 && go build ${GO_BUILD_TAGS:+-tags "${GO_BUILD_TAGS}"} -o "${BUILD_DIR}/chdb_go_example" .)

echo "Running Go test..."
# From a directory of its own: the example's --path=:memory: is a directory there, and a
# second run in the same place (the split archive's, after the prelinked one's) would find
# the first one's tables.
if ! (cd "${BUILD_DIR}" && ./chdb_go_example); then
    echo "Error: Go test failed"
    exit 1
fi

echo "Go test completed successfully!"
