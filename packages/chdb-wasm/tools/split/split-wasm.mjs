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
//   --extra-hot F     force-keep these function names too (F from a prior
//                     --emit-hot-names run). Needed for the THREADED bundle:
//                     its pool workers park in Atomics.wait and cannot answer
//                     the profile-collection message, so everything that only
//                     ran on worker threads (IO pool reads, parallel parsing,
//                     format output threads...) is missing from its own
//                     profile. C++ mangled names are identical across the two
//                     links, so the st hot set fills exactly that gap.

import { readFileSync, writeFileSync, mkdirSync, copyFileSync, readdirSync, statSync, existsSync, createWriteStream } from 'node:fs';
import { spawnSync, spawn } from 'node:child_process';
import { createInterface } from 'node:readline';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { homedir } from 'node:os';

const here = dirname(fileURLToPath(import.meta.url));
const argv = process.argv.slice(2);
const opt = (name, dflt) => {
  const i = argv.indexOf(`--${name}`);
  return i >= 0 ? argv[i + 1] : dflt;
};
const buildDir = opt('build');
const outDir = opt('out');
const wasmSplit = opt('wasm-split', process.env.WASM_SPLIT || join(homedir(), 'code/emsdk/upstream/bin/wasm-split'));
if (!buildDir || !outDir) {
  console.error('usage: split-wasm.mjs --build <dir> --out <dir> [--wasm-split <bin>] [--skip-profile-run]');
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

// --- 2. profiling run -----------------------------------------------------------
if (!argv.includes('--skip-profile-run')) {
  const r = spawnSync(process.execPath, [join(here, 'run-profile.mjs')], {
    stdio: 'inherit',
    env: { ...process.env, CHDB_BUNDLE_DIR: staging, CHDB_PROFILE_OUT: profilesDir },
  });
  if (r.status !== 0) throw new Error(`run-profile.mjs exited with ${r.status}`);
}
const profiles = readdirSync(profilesDir).filter((f) => f.endsWith('.data')).map((f) => join(profilesDir, f));
if (!profiles.length) throw new Error(`no profiles in ${profilesDir}`);
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
const SAFETY = /^(_*pthread_|__futex|futex_|emscripten_futex_|_emscripten_thread_|_emscripten_tls_|__wasm_init_tls|_emscripten_check_mailbox|emscripten_proxy_|em_proxying_|do_proxy|_emscripten_yield|emscripten_exit_with_live_runtime|__cxa_thread_|thrd_|call_once|__wait|__timedwait|__private_cond_signal|init_mparams)/;
const extraHotFile = opt('extra-hot');
const extraHot = extraHotFile ? new Set(readFileSync(extraHotFile, 'utf8').split('\n').filter(Boolean)) : null;
const emitHotFile = opt('emit-hot-names');
const emitHot = emitHotFile ? createWriteStream(emitHotFile) : null;

let hot = 0, cold = 0, keptSafety = 0, keptExtra = 0;
const keepFile = join(outDir, 'keep-funcs.txt');
const keepStream = createWriteStream(keepFile);
{
  console.log(`$ ${wasmSplit} --print-profile=${merged} ${orig}  (streamed)`);
  const p = spawn(wasmSplit, [`--print-profile=${merged}`, orig, ...FEATURES], { stdio: ['ignore', 'pipe', 'inherit'] });
  const rl = createInterface({ input: p.stdout, crlfDelay: Infinity });
  for await (const line of rl) {
    if (line.startsWith('+ ')) {
      hot++;
      emitHot?.write(line.slice(2) + '\n');
    } else if (line.startsWith('- ')) {
      const name = line.slice(2);
      cold++;
      if (SAFETY.test(name)) { keepStream.write(name + '\n'); keptSafety++; }
      else if (extraHot?.has(name)) { keepStream.write(name + '\n'); keptExtra++; }
    }
  }
  await new Promise((res, rej) => p.on('exit', (c) => (c === 0 ? res() : rej(new Error(`print-profile exited with ${c}`)))));
  await Promise.all([keepStream, emitHot].filter(Boolean).map((s) => new Promise((r) => s.end(r))));
}
console.log(`profile: ${hot} hot / ${cold} cold; force-keeping ${keptSafety} thread-runtime + ${keptExtra} extra-hot functions`);

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
run(process.execPath, [join(here, 'patch-glue.mjs'), glue, '--lazy-load']);

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
