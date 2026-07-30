#!/bin/bash

set -e

# cd to the directory of this script
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd "$DIR"

echo "Compile (header only, no libchdb link)"
${CC:-clang} chdbVersionHeaderTest.c -o chdbVersionHeaderTest -I../programs/local/

echo "Run it:"
./chdbVersionHeaderTest
