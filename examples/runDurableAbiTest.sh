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

# The statement table only means what it claims if the engine's parser list is
# checked against it; a hand-written table cannot notice a new statement type.
echo "Check statement coverage against the engine's parser list"
python3 ../chdb/build/check_statement_coverage.py

echo "Compile and link"
${CC:-clang} chdbDurableAbiTest.c -o chdbDurableAbiTest -I../programs/local/ -L../ -lchdb

export ${LIB_PATH}=..
${LDD} chdbDurableAbiTest

echo "Run it:"
# The test resolves both directories against its working directory and creates
# the data one itself; backups.allowed_path has to exist before the engine will
# write into it.
rm -rf chdb_durable_abi_test_db chdb_durable_abi_test_db2 chdb_durable_abi_test_backups
mkdir chdb_durable_abi_test_backups
./chdbDurableAbiTest
rm -rf chdb_durable_abi_test_db chdb_durable_abi_test_db2 chdb_durable_abi_test_backups
