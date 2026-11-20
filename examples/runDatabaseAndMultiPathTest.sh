#!/bin/bash
set -euo pipefail

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd "$DIR"

clang chdbDatabaseAndMultiPathTest.c -I../programs/local -L.. -lchdb -lpthread -o chdbDatabaseAndMultiPathTest
if [ "$(uname)" = "Darwin" ]; then
    DYLD_LIBRARY_PATH=.. ./chdbDatabaseAndMultiPathTest
else
    LD_LIBRARY_PATH=.. ./chdbDatabaseAndMultiPathTest
fi
