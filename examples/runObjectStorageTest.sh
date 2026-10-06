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
# memcpy from NULL.
SANITIZE=${CHDB_TEST_UBSAN:+-fsanitize=undefined -fno-sanitize-recover=all}
${CC:-clang} ${SANITIZE} chdbObjectStorageTest.c objectStorageMemStore.c objectStorageDiskTests.c objectStorageFaultTests.c \
    objectStorageLifecycleTests.c -o chdbObjectStorageTest -I../programs/local/ -L../ -lchdb -lpthread

export ${LIB_PATH}=..
${LDD} chdbObjectStorageTest

# The test makes its own temporary --path and prints PASS or FAIL. Each mode is a separate process,
# since the engine cannot be restarted after chdb_shutdown.
echo "Run it: callbacks on the engine threads"
./chdbObjectStorageTest
echo "Run it: without the copy callback (the engine streams copies through read and write)"
CHDB_TEST_NO_COPY=1 ./chdbObjectStorageTest
echo "Run it: every callback on one service thread, as a host bound to one thread serves them"
CHDB_TEST_SERVICE_THREAD=1 ./chdbObjectStorageTest
