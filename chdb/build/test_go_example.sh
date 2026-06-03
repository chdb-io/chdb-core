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

# Run Go test
echo "Running Go test..."
go run .
if [ $? -ne 0 ]; then
    echo "Error: Go test failed"
    exit 1
fi

echo "Go test completed successfully!"
