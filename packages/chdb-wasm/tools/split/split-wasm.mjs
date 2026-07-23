#!/usr/bin/env node
// End-to-end wasm-split pipeline for a chdb bundle built with
// -DWASM_SPLIT_MODULE=ON (which emits chdb.mjs + chdb.wasm [instrumented] +
// chdb.wasm.orig [the real artifact]):
//
//   1. stage the instrumented bundle, patch its glue for lazy-load correctness
//      and per-worker profile collection
//   2. run the profiling corpus against it (run-profile.mjs, separate process)
//   3. union the per-instance profiles (wasm-split --merge-profiles)
//   4. add a safety keep-list: cold thread-runtime primitives whose only
//      executions happen on workers that were parked in Atomics.wait during
//      collection (they can't answer, so their coverage can be lost)
//   5. split chdb.wasm.orig into primary + deferred guided by the profile
//   6. install primary as chdb.wasm, the deferred module as
//      chdb.deferred.wasm, and the lazy-load-patched glue as chdb.mjs
//
// Usage: split-wasm.mjs --build <dir> --out <dir> [--wasm-split <bin>] [--skip-profile-run]
//   --build           dir containing chdb.mjs / chdb.wasm / chdb.wasm.orig
//   --out             output dir (also holds staging/ and profiles/)
//   --wasm-split      wasm-split binary (default: $WASM_SPLIT or emsdk's)
//   --skip-profile-run reuse existing profiles in <out>/profiles
//   --emit-hot-names F write the profile's hot function names to F (one per
//                     line). Run the SINGLE-THREADED bundle with this: its one
//                     instance sees the whole pipeline execute inline, so its
//                     hot set is the complete corpus coverage.
//   --corpus F        statement file for the profiling runs (default:
//                     profile-corpus.sql; the lite build passes
//                     profile-corpus-lite.sql)
//   --lite-glue       install the glue with the --lite patch (cold calls throw
//                     a clear "not in lite" error) instead of --lazy-load
//   --extra-keep F    force-keep these exact function names (escaped form)
//                     when they appear in the cold set — the lite pipeline
//                     passes keep-lite.txt
//   --extra-hot F     force-keep these function names too (F from a prior
//                     --emit-hot-names run). Needed for the THREADED bundle:
//                     its pool workers park in Atomics.wait and cannot answer
//                     the profile-collection message, so everything that only
//                     ran on worker threads (IO pool reads, parallel parsing,
//                     format output threads...) is missing from its own
//                     profile. C++ mangled names are identical across the two
//                     links, so the st hot set fills exactly that gap.

import { readFileSync, writeFileSync, mkdirSync, copyFileSync, readdirSync, statSync, existsSync, rmSync, createWriteStream } from 'node:fs';
import { spawnSync, spawn } from 'node:child_process';
import { createInterface } from 'node:readline';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const here = dirname(fileURLToPath(import.meta.url));
const argv = process.argv.slice(2);
const opt = (name, dflt) => {
  const i = argv.indexOf(`--${name}`);
  return i >= 0 ? argv[i + 1] : dflt;
};
const buildDir = opt('build');
const outDir = opt('out');
const wasmSplit = opt('wasm-split', process.env.WASM_SPLIT
  || (process.env.EMSDK && join(process.env.EMSDK, 'upstream/bin/wasm-split')));
if (!buildDir || !outDir) {
  console.error('usage: split-wasm.mjs --build <dir> --out <dir> [--wasm-split <bin>] [--skip-profile-run]');
  process.exit(2);
}
if (!wasmSplit || !existsSync(wasmSplit)) {
  console.error(`wasm-split not found${wasmSplit ? ` at ${wasmSplit}` : ''} — pass --wasm-split, or set WASM_SPLIT / EMSDK (source emsdk_env.sh)`);
  process.exit(2);
}
const orig = join(buildDir, 'chdb.wasm.orig');
if (!existsSync(orig)) {
  console.error(`${orig} not found — build with -DWASM_SPLIT_MODULE=ON first`);
  process.exit(2);
}

// The .orig carries no usable target_features section, so binaryen needs the
// build's feature set spelled out. NOT --all-features: that lets the writer
// use experimental encodings (e.g. shared-everything) that V8 rejects with
// "unknown import kind".
const FEATURES = [
  '--enable-mutable-globals', '--enable-sign-ext', '--enable-nontrapping-float-to-int',
  '--enable-bulk-memory', '--enable-memory64', '--enable-exception-handling',
  '--enable-reference-types', '--enable-multivalue', '--enable-threads',
];

const run = (cmd, args) => {
  console.log(`$ ${cmd} ${args.join(' ')}`);
  const r = spawnSync(cmd, args, { stdio: 'inherit' });
  // spawnSync doesn't throw on spawn failure: status stays null, the cause is in r.error.
  if (r.error) throw new Error(`failed to run ${cmd}: ${r.error.message}`);
  if (r.status !== 0) throw new Error(`${cmd} exited with ${r.status}`);
};
const mb = (p) => `${(statSync(p).size / 1048576).toFixed(1)}MB`;

// --- 1. stage instrumented bundle ---------------------------------------------
const staging = join(outDir, 'staging');
const profilesDir = join(outDir, 'profiles');
mkdirSync(staging, { recursive: true });
copyFileSync(join(buildDir, 'chdb.wasm'), join(staging, 'chdb.wasm'));
copyFileSync(join(buildDir, 'chdb.mjs'), join(staging, 'chdb.mjs'));
run(process.execPath, [join(here, 'patch-glue.mjs'), join(staging, 'chdb.mjs'), '--lazy-load', '--profile-collect']);

// --- 2. profiling runs ------------------------------------------------------------
// Two passes, because a process has exactly ONE engine cold start and the SDK
// reaches it two ways: the main corpus pass boots via chdb_wasm_connect, the
// light init-probe pass boots via connectionless chdb_wasm_query.
if (!argv.includes('--skip-profile-run')) {
  // A fresh profiling run must not inherit .data files from a previous run in
  // a reused --out dir: run-profile only overwrites what it produces, and
  // leftovers (possibly from a DIFFERENT build) would be merged in below.
  if (existsSync(profilesDir))
    for (const f of readdirSync(profilesDir)) if (f.endsWith('.data')) rmSync(join(profilesDir, f));
  const corpusEnv = opt('corpus') ? { CHDB_CORPUS: opt('corpus') } : {};
  // A WASM_JSPI bundle's glue wraps imports in WebAssembly.Suspending and
  // exports in WebAssembly.promising — Node (V8 without the Chrome default-on)
  // needs the flag to even instantiate it. Probe for either wrapper.
  const glueSrc = readFileSync(join(staging, 'chdb.mjs'), 'utf8');
  const jspiFlags = glueSrc.includes('WebAssembly.promising') || glueSrc.includes('WebAssembly.Suspending')
    ? ['--experimental-wasm-jspi'] : [];
  for (const extraEnv of [{}, { CHDB_INIT_PROBE: 'global', CHDB_PROFILE_PREFIX: 'initprobe' }]) {
    const r = spawnSync(process.execPath, [...jspiFlags, join(here, 'run-profile.mjs')], {
      stdio: 'inherit',
      env: { ...process.env, CHDB_BUNDLE_DIR: staging, CHDB_PROFILE_OUT: profilesDir, ...corpusEnv, ...extraEnv },
    });
    if (r.status !== 0) throw new Error(`run-profile.mjs exited with ${r.status}`);
  }
}
const profiles = existsSync(profilesDir)
  ? readdirSync(profilesDir).filter((f) => f.endsWith('.data')).map((f) => join(profilesDir, f))
  : [];
if (!profiles.length) throw new Error(`no profiles in ${profilesDir} — run without --skip-profile-run first`);
console.log(`${profiles.length} instance profiles`);

// --- 3. merge -------------------------------------------------------------------
const merged = join(outDir, 'merged.profile');
run(wasmSplit, ['--merge-profiles', ...profiles, '-o', merged]);

// --- 4. keep-list -----------------------------------------------------------------
// (a) Thread/proxying runtime primitives must never be placeholder-stubbed: some
//     run during worker instantiation (before the lazy loader could even fire)
//     and their only profile source is a worker that may have been unresponsive
//     at collection time. Match by name against the cold set and force-keep.
// (b) --extra-hot names (typically the st bundle's hot set) that are cold in
//     THIS profile get force-kept too; taking them from the cold list doubles
//     as the existence check (wasm-split ignores names, it can't match).
// print-profile output exceeds V8's string limit (~140k functions with huge
// mangled names), so it is streamed line by line.
// emscripten_stack_* runs at every pthread's entry (stack-bounds setup) — it
// executes ONLY on worker instances (invisible to the main profile) and does
// not exist in the st build, so no profile can ever see it. Same class of
// blind spot for the rest of the thread-entry runtime here.
const SAFETY = /^(_*pthread_|__futex|futex_|emscripten_futex_|_*emscripten_stack_|emscripten_wasm_worker_|_emscripten_thread_|_emscripten_tls_|__wasm_init_tls|_emscripten_check_mailbox|emscripten_proxy_|em_proxying_|do_proxy|_emscripten_yield|emscripten_exit_with_live_runtime|__cxa_thread_|thrd_|call_once|__wait|__timedwait|__private_cond_signal|init_mparams|abort$)/;
// Same blind spot one layer up, in C++: thread ENTRY code — std::thread /
// Poco / ThreadFromGlobalPool trampolines, the per-task lambda wrapper
// template instances they dispatch to, thread-local init routines, the
// terminate/abort chain, and the logging-channel threads. All of it runs only
// on worker instances of the mt build, so no profile pass can record it
// (found empirically by probing a deferred-less bundle; matching by substring
// catches the endless per-task template variants).
// The last five entries are mt-only IMPLEMENTATIONS of the query result path
// (the st build takes the synchronous variants, so its hot set can't supply
// them): LazyOutputFormat + PullingAsyncPipelineExecutor feed results across
// threads, ParallelFormattingOutputFormat renders them.
const SAFETY_SUBSTR = /ThreadFromGlobalPool|ThreadPoolImpl<|__thread_proxy|__thread_struct|__thread_local_data|JobWithPriority|thread-local\\20initialization|runnableEntry|OwnRunnableForChannel|OwnAsyncSplitChannel|AsyncLogMessageQueue|demangling_terminate|std::terminate|std::__terminate|GrantedAllocation|TracingContextHolder|arrow::Unreachable|DiskEncryptedTransaction::undo|LazyOutputFormat|ParallelFormattingOutputFormat|PullingAsyncPipelineExecutor|ThreadFramePointers|BufferWithOutsideMemory/;
// Template families specialized PER INPUT SHAPE, force-kept whole on the LITE
// split only: the corpus inevitably exercises some instantiations of each and
// a cold sibling there is a hard error, not a lazy load. findExtreme* are the
// SIMD min/max kernels (per element type), WriteBufferFromVector is the C-API
// result buffer (per output container), FunctionBinaryArithmetic /
// FunctionComparison are the arithmetic/comparison executors (per type pair
// and const/vector shape), and FunctionFactory carries one registration/
// creation thunk per function — the corpus only constructs the functions it
// happens to call. The FULL bundles keep a working lazy loader, so these stay
// deferred there and the shipped primary stays lean.
const LITE_SUBSTR = argv.includes('--lite-glue')
  ? /findExtreme|WriteBufferFromVector|FunctionBinaryArithmetic|FunctionComparison|FunctionFactory/
  : null;
const extraHotFile = opt('extra-hot');
const extraHot = extraHotFile ? new Set(readFileSync(extraHotFile, 'utf8').split('\n').filter(Boolean)) : null;
// Checked-in list of single worker-path functions (see keep-worker-path.txt).
const workerPath = new Set(
  readFileSync(join(here, 'keep-worker-path.txt'), 'utf8').split('\n').filter((l) => l && !l.startsWith('#')));
// Optional additional exact-name keeps (e.g. keep-lite.txt for the lite build).
const extraKeepFile = opt('extra-keep');
if (extraKeepFile)
  for (const l of readFileSync(extraKeepFile, 'utf8').split('\n'))
    if (l && !l.startsWith('#')) workerPath.add(l);
const emitHotFile = opt('emit-hot-names');
const emitHot = emitHotFile ? createWriteStream(emitHotFile) : null;

let hot = 0, cold = 0, keptSafety = 0, keptExtra = 0, keptNameless = 0, numericNames = 0;
const keepFile = join(outDir, 'keep-funcs.txt');
const keepStream = createWriteStream(keepFile);
{
  console.log(`$ ${wasmSplit} --print-profile=${merged} ${orig}  (streamed)`);
  const p = spawn(wasmSplit, [`--print-profile=${merged}`, orig, ...FEATURES], { stdio: ['ignore', 'pipe', 'inherit'] });
  // Attach BEFORE draining stdout: 'exit' can fire while buffered output is
  // still being consumed, and a one-shot listener added afterwards never
  // settles. 'close' fires after the stdio streams end; 'error' covers spawn
  // failure (e.g. missing wasm-split binary).
  const exited = new Promise((res, rej) => {
    p.on('error', rej);
    p.on('close', (c) => (c === 0 ? res() : rej(new Error(`print-profile exited with ${c}`))));
  });
  const rl = createInterface({ input: p.stdout, crlfDelay: Infinity });
  for await (const line of rl) {
    if (line.startsWith('+ ')) {
      hot++;
      if (/^\d+$/.test(line.slice(2))) numericNames++;
      emitHot?.write(line.slice(2) + '\n');
    } else if (line.startsWith('- ')) {
      const name = line.slice(2);
      cold++;
      if (/^\d+$/.test(name) || /^trampoline_/.test(name) || /_\d{4,}$/.test(name)) {
        // Linker/compiler-SYNTHESIZED names never correspond across links, so
        // the st->mt hot-set transfer is blind to them even though hot code
        // calls into them: bare-index names on nameless std::function/lambda
        // thunks, and wasm-ld's signature-mismatch bridges named
        // trampoline_X / X_<link-specific-number> (found the hard way under
        // CountingTransform and ~DeduplicationInfo in the INSERT pipeline).
        // All tiny; keep every cold one (~10k functions, low-MB cost).
        if (/^\d+$/.test(name)) numericNames++;
        keepStream.write(name + '\n');
        keptNameless++;
      } else if (SAFETY.test(name) || SAFETY_SUBSTR.test(name) || LITE_SUBSTR?.test(name) || workerPath.has(name)) { keepStream.write(name + '\n'); keptSafety++; }
      else if (extraHot?.has(name)) { keepStream.write(name + '\n'); keptExtra++; }
    }
  }
  await exited;
  await Promise.all([keepStream, emitHot].filter(Boolean).map((s) => new Promise((r) => s.end(r))));
}
// Without a name section print-profile emits bare function INDICES; the
// keep-lists and the st->mt hot-set transfer silently degrade to noise
// (indices from different links never correspond). Guard against it.
if (numericNames > (hot + cold) * 0.9)
  throw new Error('profile names are bare indices — the build lost its name section; link with WASM_SPLIT_MODULE=ON (adds --profiling-funcs)');
console.log(`profile: ${hot} hot / ${cold} cold; force-keeping ${keptSafety} thread-runtime + ${keptExtra} extra-hot + ${keptNameless} nameless-thunk functions`);

// --- 5. split ---------------------------------------------------------------------
const primary = join(outDir, 'chdb.wasm');
const deferred = join(outDir, 'chdb.deferred.wasm');
run(wasmSplit, [
  '--export-prefix=%', orig,
  '-o1', primary, '-o2', deferred,
  `--profile=${merged}`, `--keep-funcs=@${keepFile}`,
  ...FEATURES,
]);

// --- 6. install glue ----------------------------------------------------------------
const glue = join(outDir, 'chdb.mjs');
copyFileSync(join(buildDir, 'chdb.mjs'), glue);
run(process.execPath, [join(here, 'patch-glue.mjs'), glue, argv.includes('--lite-glue') ? '--lite' : '--lazy-load']);

// Sanity: the primary must import its stubs from `placeholder*` modules — that's
// what the glue's proxy intercepts.
const mod = new WebAssembly.Module(readFileSync(primary));
const placeholders = WebAssembly.Module.imports(mod).filter((i) => i.module.startsWith('placeholder')).length;
if (!placeholders) throw new Error('primary has no placeholder imports — split produced a non-lazy module?');

console.log(`
done:
  primary   ${primary}  ${mb(primary)}  (${placeholders} placeholder stubs)
  deferred  ${deferred}  ${mb(deferred)}
  glue      ${glue}
original    ${orig}  ${mb(orig)}`);
