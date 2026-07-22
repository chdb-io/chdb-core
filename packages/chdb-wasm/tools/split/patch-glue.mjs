#!/usr/bin/env node
// Post-link patches for the -sSPLIT_MODULE Emscripten glue (chdb.mjs).
//
// Two independent patches, selected by flags; each asserts its anchor text so
// an emsdk upgrade that changes the glue fails LOUDLY here instead of hanging
// at runtime:
//
// --lazy-load (shipped split bundle AND instrumented bundle):
//   The placeholder-import proxy derives the deferred file name from
//   `wasmBinaryFile`, but pthread workers instantiate the module from the
//   postMessage'd WebAssembly.Module without ever assigning wasmBinaryFile
//   (preamble sets it only on the main-instance path), so the first cold call
//   on a worker — or plain instantiation of the split primary, whose
//   placeholder imports are read during `new WebAssembly.Instance` — throws.
//   Fix: fall back to findWasmBinary() at the use site.
//
// --profile-collect (instrumented mt bundle only):
//   wasm-split's instrumentation counters live in module GLOBALS, which are
//   per-instance — and every pthread worker is its own instance. The main
//   instance's __write_profile therefore misses everything that only ran on
//   workers (thread entry points, futex/proxying, IO-pool reads, parallel
//   parsing...). This patch adds a `chdbWriteProfile` command to the worker
//   message dispatch: the worker calls ITS OWN instance's __write_profile,
//   writing into a caller-provided buffer in the SHARED linear memory, and
//   replies through the existing `callHandler` main-thread dispatch
//   (Module.__chdbProfileWritten(tag, len)). The profiling runner collects one
//   profile per worker plus the main instance and wasm-split --merge-profiles
//   unions them.
//
// --lite (chdb-wasm-lite, primary-only bundle):
//   The lite package ships NO deferred module (Cloudflare Workers forbids
//   runtime wasm compilation, so lazy loading cannot exist there). A cold
//   call must fail immediately with a message that explains itself instead
//   of an ENOENT/fetch error for a file that is not supposed to exist.
//
// Usage: patch-glue.mjs <chdb.mjs> [--lazy-load] [--profile-collect] [--lite]

import { readFileSync, writeFileSync } from 'node:fs';

const file = process.argv[2];
const doLazy = process.argv.includes('--lazy-load');
const doProfile = process.argv.includes('--profile-collect');
const doLite = process.argv.includes('--lite');
if (!file || (!doLazy && !doProfile && !doLite)) {
  console.error('usage: patch-glue.mjs <chdb.mjs> [--lazy-load] [--profile-collect] [--lite]');
  process.exit(2);
}
if (doLite && doLazy) {
  console.error('--lite replaces the lazy loader; do not combine it with --lazy-load');
  process.exit(2);
}

let src = readFileSync(file, 'utf8');
const srcBefore = src;

function replaceCounted(what, anchor, replacement, expected) {
  const n = src.split(anchor).length - 1;
  if (n !== expected)
    throw new Error(`${what}: expected ${expected} occurrence(s) of anchor, found ${n} — emscripten glue changed, re-verify the patch`);
  src = src.split(anchor).join(replacement);
}

if (doLazy) {
  if (src.includes('wasmBinaryFile??=findWasmBinary()).slice(0,-5)')) {
    console.log('lazy-load: already patched');
  } else {
    // Both branches of the proxy (old `placeholder` and new `placeholder.N`
    // module-name formats) compute the secondary file name this way.
    replaceCounted('lazy-load', 'wasmBinaryFile.slice(0,-5)', '(wasmBinaryFile??=findWasmBinary()).slice(0,-5)', 2);

    // binaryen's wasm-split puts placeholders in a SECOND table (the import
    // name is a sequential ordinal into it), and the secondary module's elem
    // repairs THAT table — but the glue trampoline re-dispatches through the
    // MAIN function table (wasmTable), reaching an unrelated function. Pick
    // the placeholder table out of the raw exports (the split step exports it
    // via --export-prefix); it is 32-bit even when the main table is table64,
    // so index with whichever type its length reports.
    replaceCounted(
      'lazy-load(redispatch)',
      'return wasmTable.get(BigInt(base))(...args)}',
      'var pts__=Object.values(wasmRawExports).filter((v)=>v instanceof WebAssembly.Table&&v!==wasmTable);'
        + 'if(pts__.length!==1)throw new Error("chdb split: expected exactly one placeholder table, found "+pts__.length);'
        + 'var pt__=pts__[0];'
        + 'return pt__.get(typeof pt__.length==="bigint"?BigInt(base):Number(base))(...args)}',
      1);
    console.log('lazy-load: patched (2 sites + placeholder-table redispatch)');
  }
}

if (doLite) {
  if (src.includes('chdb-wasm-lite')) {
    console.log('lite: already patched');
  } else {
    // The placeholder-import proxy still computes the (never fetched)
    // secondary file name from wasmBinaryFile at instantiation time; on paths
    // that never assign it (pthread workers, Module.instantiateWasm hosts like
    // Cloudflare Workers) that would throw before our stub even runs.
    replaceCounted('lite(wasmBinaryFile)', 'wasmBinaryFile.slice(0,-5)', '(wasmBinaryFile??=findWasmBinary()).slice(0,-5)', 2);
    // Replace the whole lazy-load trampoline: no download, no re-dispatch —
    // just a self-explanatory error. A WASM_JSPI link emits an async
    // trampoline (lazy loading itself suspends via JSPI); the surrounding
    // `new WebAssembly.Suspending(ret)` wrapper is kept — it accepts a plain
    // function, and a synchronous throw from a suspending import propagates
    // to the wasm call site the same way a rejection would.
    const stub =
      'let ret=(...args)=>{throw new Error("chdb-wasm-lite: this SQL feature is not included in the size-optimized lite build; use the full chdb-wasm package")}';
    const trampolines = [
      'let ret=(...args)=>{var imports={primary:wasmRawExports};loadSplitModule(secondaryFile,imports,base);return wasmTable.get(BigInt(base))(...args)}',
      'let ret=async(...args)=>{var imports={primary:wasmRawExports};await loadSplitModule(secondaryFile,imports,base);return wasmTable.get(BigInt(base))(...args)}',
    ];
    const present = trampolines.filter((t) => src.includes(t));
    if (present.length !== 1)
      throw new Error(`lite(trampoline): expected exactly one trampoline shape, found ${present.length} — emscripten glue changed, re-verify the patch`);
    replaceCounted('lite(trampoline)', present[0], stub, 1);
    console.log('lite: patched (cold calls throw a clear error)');
  }
}

if (doProfile) {
  if (src.includes('chdbWriteProfile')) {
    console.log('profile-collect: already patched');
  } else if (!src.includes('unusedWorkers')) {
    // Single-threaded glue: no pthread workers, nothing to collect from — the
    // main instance's __write_profile already sees everything.
    console.log('profile-collect: single-threaded glue, skipped');
  } else {
    // Insert before the worker-side setimmediate no-op branch. In scope here:
    // msgData (the message), wasmExports (THIS worker instance's raw exports,
    // where binaryen's __write_profile lives), postMessage (bridged to
    // parentPort in Node). Memory64: the pointer parameter is a raw i64 ->
    // BigInt; the byte-count parameter is i32 -> Number.
    const anchor = 'else if(msgData.target==="setimmediate"){}';
    // try/catch so ANY failure still posts a reply — otherwise the runner
    // burns its per-worker timeout with no indication of the real cause.
    const branch =
      'else if(cmd==="chdbWriteProfile"){' +
      'var n__=-1;' +
      'try{var wp__=wasmExports["__write_profile"];' +
      'if(wp__)n__=Number(wp__(BigInt(msgData.ptr),msgData.cap))}' +
      'catch(e__){n__=-1}' +
      'postMessage({cmd:"callHandler",handler:"__chdbProfileWritten",args:[msgData.tag,n__]})}';
    replaceCounted('profile-collect', anchor, branch + anchor, 1);
    console.log('profile-collect: patched');
  }
}

// Don't dirty the mtime when every branch was a no-op (already patched /
// single-threaded skip) — that can trigger spurious downstream rebuilds.
if (src !== srcBefore) writeFileSync(file, src);
