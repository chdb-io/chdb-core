#!/usr/bin/env bash
# Assemble the Cloudflare deploy bundle (pages/ + r2/) from a chdb-wasm dist dir and
# the web/ source. Used by .github/workflows/deploy-wasm-shell.yml.
#
#   build-cf-bundle.sh <dist-dir> <web-dir> <out-dir>
#
# <dist-dir> must contain: index.js worker.js async.js bindings.js protocol.js
# status.js platform.js chdb.mjs chdb.wasm st/chdb.mjs st/chdb.wasm
#
# Output:
#   <out-dir>/pages/  -> deploy to Cloudflare Pages (HTML, glue JS, *.mjs, _headers,
#                        _routes.json, the Function) — no large wasm
#   <out-dir>/r2/     -> upload to R2 (the two large chdb.wasm only; keys mirror paths)
set -euo pipefail

DIST="${1:?usage: build-cf-bundle.sh <dist-dir> <web-dir> <out-dir>}"
WEB="${2:?missing web dir}"
OUT="${3:?missing out dir}"
JS="index.js worker.js async.js bindings.js protocol.js status.js platform.js"

# Content-hashed dir name: changes only when the engine or glue changes, so the
# browser caches it immutably and re-pulls only on a new build.
VER="dist-$(cat "$DIST/chdb.wasm" "$DIST/st/chdb.wasm" "$DIST/index.js" | md5sum | cut -c1-10)"

rm -rf "$OUT"
mkdir -p "$OUT/pages/$VER/st" "$OUT/pages/functions" "$OUT/r2/$VER/st"

# Pages: small files only (glue JS + the small .mjs; NOT the big wasm)
for f in $JS; do cp "$DIST/$f" "$OUT/pages/$VER/$f"; done
cp "$DIST/chdb.mjs"    "$OUT/pages/$VER/chdb.mjs"
cp "$DIST/st/chdb.mjs" "$OUT/pages/$VER/st/chdb.mjs"
cp "$WEB/_headers" "$WEB/_routes.json" "$OUT/pages/"
cp "$WEB/functions/[[path]].js" "$OUT/pages/functions/"
# Point the page at the hashed engine dir (source keeps ./dist).
sed "s#\./dist#./$VER#g" "$WEB/index.html" > "$OUT/pages/index.html"

# R2: the two large wasm (object keys mirror the URL paths)
cp "$DIST/chdb.wasm"    "$OUT/r2/$VER/chdb.wasm"
cp "$DIST/st/chdb.wasm" "$OUT/r2/$VER/st/chdb.wasm"

echo "$VER"
