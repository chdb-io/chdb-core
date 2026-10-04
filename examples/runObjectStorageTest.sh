#!/bin/bash

set -e

# check current os type, and make ldd command
if [ "$(uname)" == "Darwin" ]; then
    LDD="otool -L"
    LIB_PATH="DYLD_LIBRARY_PATH"
elif [ "$(uname)" == "Linux" ]; then
    LDD="ldd"
    LIB_PATH="LD_LIBRARY_PATH"
else
    echo "OS not supported"
    exit 1
fi

# cd to the directory of this script
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd "$DIR"

echo "Compile and link"
# CHDB_TEST_UBSAN=1 builds the host store with UBSan, which catches contract slips such as a
# memcpy from NULL. CHDB_TEST_NO_COPY=1 (read by the store) exercises the engine's copy fallback.
SANITIZE=${CHDB_TEST_UBSAN:+-fsanitize=undefined -fno-sanitize-recover=all}
${CC:-clang} ${SANITIZE} chdbObjectStorageTest.c objectStorageMemStore.c objectStorageDiskTests.c objectStorageFaultTests.c \
    -o chdbObjectStorageTest -I../programs/local/ -L../ -lchdb -lpthread

export ${LIB_PATH}=..
${LDD} chdbObjectStorageTest

echo "Run it:"
# The test makes its own temporary --path and prints PASS or FAIL.
./chdbObjectStorageTest
