#!/usr/bin/env node
// Profiling runner for wasm-split: loads the INSTRUMENTED chdb bundle, executes
// profile-corpus.sql against it (plus the streaming C-API surface), then dumps
// one execution profile per wasm instance — the main instance and, on the
// threaded build, every pthread pool worker (each is its own instance with its
// own instrumentation globals; see patch-glue.mjs --profile-collect). The
// profiles are unioned later by `wasm-split --merge-profiles`.
//
// Queries run synchronously on THIS thread, so every HTTP fixture lives in
// another process: moto (python S3) is spawned directly, the Node mocks run in
// fixture-host.mjs. Data-lake statements are skipped when the pyiceberg venv
// (ICEBERG_PY, default /tmp/iceberg-venv/bin/python) is missing.
//
// Env: CHDB_BUNDLE_DIR  dir with the instrumented+patched chdb.mjs/chdb.wasm (required)
//      CHDB_PROFILE_OUT output dir for profile-*.data + summary.json (required)
//      ICEBERG_PY       python with pyiceberg+moto+deltalake (optional)
//      CHDB_SKIP_LAKE   set to 1 to force-skip the data-lake section

import { readFileSync, writeFileSync, mkdirSync, existsSync, mkdtempSync } from 'node:fs';
import { spawn, execFileSync } from 'node:child_process';
import { join, dirname } from 'node:path';
import { tmpdir } from 'node:os';
import { fileURLToPath, pathToFileURL } from 'node:url';

const here = dirname(fileURLToPath(import.meta.url));
const bundleDir = process.env.CHDB_BUNDLE_DIR;
const outDir = process.env.CHDB_PROFILE_OUT;
if (!bundleDir || !outDir) {
  console.error('CHDB_BUNDLE_DIR and CHDB_PROFILE_OUT are required');
  process.exit(2);
}
mkdirSync(outDir, { recursive: true });

const PY = process.env.ICEBERG_PY || '/tmp/iceberg-venv/bin/python';
const S3_PORT = 8151, CATALOG_PORT = 8152, UNITY_PORT = 8153, HTTP_PORT = 8154;

// CHDB_INIT_PROBE=global: an ORDER-REVERSED second pass. A process gets one
// engine cold start, and which API claims it changes which init paths run:
// the main pass boots via chdb_wasm_connect then runs the corpus; this pass
// boots via connectionless chdb_wasm_query FIRST (the SDK's AsyncChdb.query
// shape) and THEN connects and runs the corpus, so session-after-global init
// paths get profiled too. Data-lake fixtures are skipped (those statements
// auto-skip); everything else runs in both orders.
const INIT_PROBE = process.env.CHDB_INIT_PROBE === 'global';
const PROFILE_PREFIX = process.env.CHDB_PROFILE_PREFIX || 'profile';

const num = (x) => (typeof x === 'bigint' ? Number(x) : x);
// Memory64: binaryen-added exports (__write_profile) keep raw i64 -> BigInt
// pointer params while emscripten-known exports are signature-converted to
// Number; probe rather than hardcode.
const rawCall = (fn, ...args) => {
  for (const conv of [(a) => a, (a) => a.map((x) => (typeof x === 'bigint' ? Number(x) : BigInt(x)))]) {
    try { return fn(...conv(args)); } catch (e) { if (!(e instanceof TypeError) || !/BigInt/.test(e.message)) throw e; }
  }
  throw new Error('no BigInt/Number combination accepted');
};

// --- fixture data ------------------------------------------------------------
const PEOPLE_CSV = '1,alice,9.5\n2,bob,7.25\n3,carol,8.0\n4,dave,6.5\n';
const PEOPLE_NAMES_CSV = 'id,name,score\n' + PEOPLE_CSV;
const PEOPLE_TSV = PEOPLE_CSV.replaceAll(',', '\t');
const PEOPLE_JSONL =
  '{"id": 1, "name": "alice", "score": 9.5}\n{"id": 2, "name": "bob", "score": 7.25}\n{"id": 3, "name": "carol", "score": 8.0}\n';

const staticDir = mkdtempSync(join(tmpdir(), 'chdb-profile-static-'));
writeFileSync(join(staticDir, 'people_names.csv'), PEOPLE_NAMES_CSV);
writeFileSync(join(staticDir, 'people.jsonl'), PEOPLE_JSONL);

// --- fixture services (all out-of-process) -----------------------------------
const children = [];
// {HTTP} only when the fixture host will actually run: in the INIT_PROBE pass
// it does not, and an unconditional substitution would aim the url()
// statements at a dead endpoint (recorded as failures) instead of skipping.
const substitutions = {};
if (!INIT_PROBE) substitutions['{HTTP}'] = `http://127.0.0.1:${HTTP_PORT}`;
let moto = null;

// Registered BEFORE startLake(): moto is spawned inside it, and a failure in a
// later startup step (bucket PUT, table fixtures) must not orphan it on its
// port. 'exit' doesn't fire on signals; without those handlers a killed runner
// orphans moto and fixture-host, and the next run fails on EADDRINUSE.
const cleanup = () => {
  for (const c of children) c.kill('SIGKILL');
  if (moto) moto.kill('SIGKILL');
};
process.on('exit', cleanup);
for (const sig of ['SIGTERM', 'SIGINT']) process.on(sig, () => { cleanup(); process.exit(143); });

async function startLake() {
  if (process.env.CHDB_SKIP_LAKE === '1' || !existsSync(PY)) return false;
  const testDir = join(here, '../../test');
  moto = spawn(PY, ['-m', 'moto.server', '-p', String(S3_PORT)], { stdio: ['ignore', 'ignore', 'pipe'] });
  let motoStderr = '';
  moto.stderr.on('data', (d) => { motoStderr += d; });
  const deadline = Date.now() + 30000;
  for (;;) {
    if (moto.exitCode !== null) { console.error('moto exited early:\n' + motoStderr); return false; }
    try { await fetch(`http://127.0.0.1:${S3_PORT}`); break; }
    catch {
      if (Date.now() > deadline) { console.error('moto did not come up'); moto.kill('SIGKILL'); moto = null; return false; }
      await new Promise((r) => setTimeout(r, 200));
    }
  }
  const endpoint = `http://127.0.0.1:${S3_PORT}`;
  await fetch(`${endpoint}/lakebucket`, { method: 'PUT' });
  await fetch(`${endpoint}/deltabucket`, { method: 'PUT' });
  const iceberg = execFileSync(PY, [join(testDir, 'datalake/make_iceberg_table.py'), endpoint, 'lakebucket'], { encoding: 'utf8' }).trim();
  const delta = execFileSync(PY, [join(testDir, 'datalake/make_delta_table.py'), endpoint, 'deltabucket'], { encoding: 'utf8' }).trim();
  writeFileSync(join(staticDir, 'iceberg-descriptor.json'), iceberg);
  writeFileSync(join(staticDir, 'unity-descriptor.json'), delta);
  substitutions['{S3}'] = endpoint;
  substitutions['{CATALOG}'] = `http://127.0.0.1:${CATALOG_PORT}`;
  substitutions['{UNITY}'] = `http://127.0.0.1:${UNITY_PORT}/api/2.1/unity-catalog`;
  return true;
}

const lake = INIT_PROBE ? false : await startLake();
if (!INIT_PROBE) {
  const hostArgs = ['--http-port', String(HTTP_PORT), '--static-dir', staticDir];
  if (lake) {
    hostArgs.push(
      '--catalog-port', String(CATALOG_PORT), '--iceberg-descriptor', join(staticDir, 'iceberg-descriptor.json'),
      '--unity-port', String(UNITY_PORT), '--unity-descriptor', join(staticDir, 'unity-descriptor.json'));
  }
  // stdin 'pipe' (not 'ignore'): fixture-host watches it to detect parent
  // death even when our cleanup handlers never run (SIGKILL).
  const host = spawn(process.execPath, [join(here, 'fixture-host.mjs'), ...hostArgs], { stdio: ['pipe', 'pipe', 'inherit'] });
  children.push(host);
  await new Promise((resolve, reject) => {
    const t = setTimeout(() => reject(new Error('fixture-host did not become ready')), 15000);
    host.stdout.on('data', (d) => { if (String(d).includes('READY')) { clearTimeout(t); resolve(); } });
    host.on('exit', (c) => reject(new Error(`fixture-host exited early (${c})`)));
  });
  console.log(`fixtures ready (lake: ${lake ? 'on' : 'OFF — data-lake statements will be skipped'})`);
}

// --- load the instrumented module --------------------------------------------
const factory = (await import(pathToFileURL(join(bundleDir, 'chdb.mjs')).href)).default;
const mod = await factory();
console.log('instrumented module ready');

// MEMFS fixtures for file() statements.
mod.FS.mkdir('/corpus');
mod.FS.writeFile('/corpus/people.csv', PEOPLE_CSV);
mod.FS.writeFile('/corpus/people2.csv', PEOPLE_NAMES_CSV); // glob partner
mod.FS.writeFile('/corpus/people_names.csv', PEOPLE_NAMES_CSV);
mod.FS.writeFile('/corpus/people.tsv', PEOPLE_TSV);
mod.FS.writeFile('/corpus/people.jsonl', PEOPLE_JSONL);

// The SDK calls these during init (worker.ts shares the cancel/progress
// offsets with the page) — they must be hot or a split bundle can't even
// initialize without the deferred module.
mod.ccall('chdb_wasm_cancel_flag_addr', 'number', [], []);
mod.ccall('chdb_wasm_progress_addr', 'number', [], []);

// Connectionless queries: AsyncChdb.query goes through chdb_wasm_query, whose
// FIRST call cold-starts the process-global connection.
function globalQueries() {
  for (const [sql, fmt] of [
    ['SELECT number, toString(number) FROM numbers(100)', 'CSV'],
    ['SELECT 1 AS one FORMAT JSON', 'CSV'],
    ['SELECT broken syntax here', 'CSV'],
  ]) {
    const r = mod.ccall('chdb_wasm_query', 'number', ['string', 'string'], [sql, fmt]);
    if (num(r)) {
      mod.ccall('chdb_wasm_result_error', 'number', ['number'], [r]);
      mod.ccall('chdb_wasm_result_buffer', 'number', ['number'], [r]);
      mod.ccall('chdb_wasm_result_length', 'number', ['number'], [r]);
      mod.ccall('chdb_wasm_free_result', null, ['number'], [r]);
    }
  }
}

// The init-probe pass makes the GLOBAL connection the process's cold start;
// the main pass gives that honor to chdb_wasm_connect below.
if (INIT_PROBE) {
  globalQueries();
  console.log('init-probe: global-connection cold start exercised');
}

// Handles (conn, result, stream pointers) are BigInt on Memory64 and must be
// passed back into ccall UNconverted (the raw exports take i64); num() is only
// for heap offsets consumed from JS. Mirrors src/bindings.ts.
const conn = mod.ccall('chdb_wasm_connect', 'number', ['string'], ['']);
if (!num(conn)) throw new Error('chdb_wasm_connect failed');

// Consume results the way src/bindings.ts does — buffer/length/stats getters
// are on every user's path and must land in the primary module.
function runStatement(sql, format = 'CSV') {
  const r = mod.ccall('chdb_wasm_query_conn', 'number', ['number', 'string', 'string'], [conn, sql, format]);
  if (!num(r)) return 'null result';
  const errPtr = num(mod.ccall('chdb_wasm_result_error', 'number', ['number'], [r]));
  const err = errPtr ? mod.UTF8ToString(errPtr) : null;
  if (!err) {
    const buf = num(mod.ccall('chdb_wasm_result_buffer', 'number', ['number'], [r]));
    const len = num(mod.ccall('chdb_wasm_result_length', 'number', ['number'], [r]));
    if (buf && len) mod.HEAPU8[buf]; // touch the data
    mod.ccall('chdb_wasm_result_elapsed', 'number', ['number'], [r]);
    mod.ccall('chdb_wasm_result_rows_read', 'number', ['number'], [r]);
    mod.ccall('chdb_wasm_result_bytes_read', 'number', ['number'], [r]);
    mod.ccall('chdb_wasm_result_scanned_rows', 'number', ['number'], [r]);
    mod.ccall('chdb_wasm_result_scanned_bytes', 'number', ['number'], [r]);
  }
  mod.ccall('chdb_wasm_free_result', null, ['number'], [r]);
  return err;
}

// --- execute the corpus -------------------------------------------------------
const corpus = readFileSync(join(here, 'profile-corpus.sql'), 'utf8');
const statements = [];
let buf = [];
for (const line of corpus.split('\n')) {
  if (/^\s*--/.test(line) || (!buf.length && !line.trim())) continue;
  buf.push(line);
  if (/;\s*$/.test(line)) { statements.push(buf.join('\n')); buf = []; }
}

let ok = 0, skipped = 0;
const failures = [];
const t0 = Date.now();
const isMt = !!mod.PThread;
for (let sql of statements) {
  for (const [k, v] of Object.entries(substitutions)) sql = sql.replaceAll(k, v);
  if (/\{[A-Z0-9]+\}/.test(sql)) { skipped++; continue; }
  if (!isMt && sql.includes('/*mt-only*/')) { skipped++; continue; }
  if (process.env.CHDB_TRACE === '1') console.log(`>>> ${sql.replace(/\n/g, ' ').slice(0, 100)}`);
  try {
    const err = runStatement(sql);
    if (err) failures.push({ sql: sql.slice(0, 120), error: err.slice(0, 200) });
    else ok++;
  } catch (e) {
    // A wasm trap (RuntimeError) means the instance aborted — nothing further
    // can run; surface WHICH statement did it and give up.
    console.error(`FATAL wasm trap on statement: ${sql.replace(/\n/g, ' ').slice(0, 200)}`);
    throw e;
  }
}
console.log(`corpus: ${ok} ok, ${failures.length} failed, ${skipped} skipped, ${((Date.now() - t0) / 1000).toFixed(1)}s`);

if (!INIT_PROBE) globalQueries();

// --- streaming C-API surface ----------------------------------------------------
{
  const s = mod.ccall('chdb_wasm_stream_start', 'number', ['number', 'string', 'string'], [conn, 'SELECT number, toString(number) FROM numbers(100000)', 'CSV']);
  let chunks = 0;
  for (;;) {
    const c = mod.ccall('chdb_wasm_stream_fetch', 'number', ['number', 'number'], [conn, s]);
    if (!num(c)) break;
    const len = num(mod.ccall('chdb_wasm_result_length', 'number', ['number'], [c]));
    mod.ccall('chdb_wasm_free_result', null, ['number'], [c]);
    chunks++;
    if (!len) break;
  }
  mod.ccall('chdb_wasm_free_result', null, ['number'], [s]);
  const s2 = mod.ccall('chdb_wasm_stream_start', 'number', ['number', 'string', 'string'], [conn, 'SELECT number FROM numbers(1000000)', 'CSV']);
  mod.ccall('chdb_wasm_stream_cancel', null, ['number', 'number'], [conn, s2]);
  mod.ccall('chdb_wasm_free_result', null, ['number'], [s2]);
  console.log(`streaming: ${chunks} chunks + cancel`);
}

// Close the session before collecting: exercises shutdown paths and lets
// query/background threads exit, so their pool workers return to the idle
// (message-processing) state where they can answer chdbWriteProfile.
mod.ccall('chdb_wasm_close_conn', null, ['number'], [conn]);

// --- profile collection ---------------------------------------------------------
const wp = mod.wasmExports?.__write_profile;
if (!wp) throw new Error('__write_profile export missing — is this the instrumented bundle?');
const profLen = Number(rawCall(wp, 0n, 0));

const workers = mod.PThread ? [...mod.PThread.runningWorkers, ...mod.PThread.unusedWorkers] : [];
// wasm export names are minified at this link level, so malloc must be reached
// through the glue's Module._malloc alias (raw, unwrapped on Memory64). One
// SLOT PER INSTANCE: a worker that answers after its collection timed out
// still writes into its own slot, never into one being read for another.
const bufPtr = num(rawCall(mod._malloc, profLen * (workers.length + 1)));
if (!bufPtr) throw new Error(`_malloc(${profLen * (workers.length + 1)}) failed for the profile buffers`);

const profiles = [];
{
  const n = Number(rawCall(wp, BigInt(bufPtr), profLen));
  profiles.push(Uint8Array.from(mod.HEAPU8.slice(bufPtr, bufPtr + n)));
  console.log(`profile main: ${n} bytes`);
}

if (workers.length) {
  let collected = 0, timedOut = 0;
  for (let i = 0; i < workers.length; i++) {
    const slot = bufPtr + profLen * (1 + i);
    const n = await new Promise((resolve) => {
      // A worker parked in a blocking wait (Atomics.wait) can't service
      // messages; skip it after a short timeout rather than hanging. Its
      // coverage is lost, which at worst means an unnecessary lazy load at
      // runtime — never a failure (see patch-glue.mjs --lazy-load).
      const t = setTimeout(() => resolve(-2), 5000);
      // Tag check: a worker that answers AFTER its timeout must not resolve a
      // later worker's promise with the wrong length.
      mod.__chdbProfileWritten = (tag, len) => { if (tag !== i) return; clearTimeout(t); resolve(len); };
      workers[i].postMessage({ cmd: 'chdbWriteProfile', ptr: slot, cap: profLen, tag: i });
    });
    if (n > 0) {
      profiles.push(Uint8Array.from(mod.HEAPU8.slice(slot, slot + n)));
      collected++;
    } else if (n === -2) timedOut++;
  }
  console.log(`profile workers: ${collected} collected, ${timedOut} unresponsive of ${workers.length}`);
}

profiles.forEach((p, i) => writeFileSync(join(outDir, `${PROFILE_PREFIX}-${i}.data`), p));
if (!INIT_PROBE) {
  writeFileSync(join(outDir, 'summary.json'), JSON.stringify({
    statements: statements.length, ok, failed: failures.length, skipped, lake, profiles: profiles.length, failures,
  }, null, 2));
}
console.log(`wrote ${profiles.length} ${PROFILE_PREFIX}-*.data profiles to ${outDir}`);

cleanup();
process.exit(0);
