#!/bin/bash

set -e

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

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd $DIR

echo "Compile and link"
${CC:-clang} chdbAsanRegressionTest.c -o chdbAsanRegressionTest -I../programs/local/ -L../ -lchdb

export ${LIB_PATH}=..
${LDD} chdbAsanRegressionTest

echo "Run it:"
./chdbAsanRegressionTest
