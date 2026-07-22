// Tests for the wasm-split bundles (tools/split/): artifact shape, hot-path
// completeness, lazy loading of the deferred module, negative controls, and
// the split tooling's own guard rails.
//
// Points at split output dirs (each holding chdb.mjs + chdb.wasm +
// chdb.deferred.wasm, as produced by tools/split/split-wasm.mjs --out):
//   CHDB_SPLIT_MT_DIR=/path/mt CHDB_SPLIT_ST_DIR=/path/st node test/split.test.mjs
// Skips cleanly when neither dir is provided (split is an opt-in build mode;
// see tools/split/README.md).
//
// Functional equivalence of the engine itself is NOT re-tested here — run the
// regular suites (matrix/datalake/...) against the split bundle via
// CHDB_WASM_MJS for that. This file tests what is SPLIT-specific.

import assert from 'node:assert';
import { readFileSync, writeFileSync, existsSync, mkdtempSync, mkdirSync, rmSync, copyFileSync } from 'node:fs';
import { spawnSync } from 'node:child_process';
import { join, dirname } from 'node:path';
import { tmpdir } from 'node:os';
import { fileURLToPath } from 'node:url';

const pkgDir = dirname(dirname(fileURLToPath(import.meta.url)));
// An UNSET env var skips that bundle; a SET one pointing at a dir without the
// split artifacts fails loudly — otherwise a pipeline regression that stops
// emitting chdb.deferred.wasm would silently turn CI's run into a skip.
const bundles = [
  ['mt', process.env.CHDB_SPLIT_MT_DIR],
  ['st', process.env.CHDB_SPLIT_ST_DIR],
].filter(([variant, dir]) => {
  if (!dir) return false;
  for (const f of ['chdb.mjs', 'chdb.wasm', 'chdb.deferred.wasm']) {
    if (!existsSync(join(dir, f))) {
      console.error(`FAIL split.test: CHDB_SPLIT_${variant.toUpperCase()}_DIR=${dir} is set but ${f} is missing`);
      process.exit(1);
    }
  }
  return true;
});

if (!bundles.length) {
  console.log('SKIP split.test: set CHDB_SPLIT_MT_DIR / CHDB_SPLIT_ST_DIR to split output dirs');
  process.exit(0);
}

let checks = 0;
const ok = (label) => { checks++; console.log(`ok: ${label}`); };

// Runs a probe script in a CHILD process: lazy-load state is per-instance, so
// cold-path and no-deferred checks each need a fresh process. Returns
// {status, stdout, stderr}; timeout guards against the historical hang modes.
const tmpRoot = mkdtempSync(join(tmpdir(), 'chdb-split-test-'));
process.on('exit', () => rmSync(tmpRoot, { recursive: true, force: true }));
let tmpSeq = 0;
const tmpFile = (name) => { mkdirSync(join(tmpRoot, String(++tmpSeq))); return join(tmpRoot, String(tmpSeq), name); };

function probe(dir, body, timeoutMs = 300000) {
  const script = `
    import { AsyncChdb } from ${JSON.stringify(join(pkgDir, 'dist/index.js'))};
    const db = await AsyncChdb.create({ moduleUrl: ${JSON.stringify(join(dir, 'chdb.mjs'))} });
    const q = async (sql) => (await db.query(sql, 'CSV')).text().trim();
    ${body}
    await db.terminate();
    process.exit(0);
  `;
  const file = tmpFile('probe.mjs');
  writeFileSync(file, script);
  return spawnSync(process.execPath, [file], { encoding: 'utf8', timeout: timeoutMs });
}
const lastLine = (r) => r.stdout.trim().split('\n').pop();

for (const [variant, dir] of bundles) {
  console.log(`===== ${variant}: ${dir}`);

  // ---- artifact shape ----------------------------------------------------
  {
    const primary = new WebAssembly.Module(readFileSync(join(dir, 'chdb.wasm')));
    const placeholders = WebAssembly.Module.imports(primary).filter((i) => i.module.startsWith('placeholder'));
    assert.ok(placeholders.length > 1000, `expected >1000 placeholder imports, got ${placeholders.length}`);
    ok(`${variant}: primary has ${placeholders.length} placeholder stubs`);

    const deferredHead = readFileSync(join(dir, 'chdb.deferred.wasm')).subarray(0, 4);
    assert.deepStrictEqual([...deferredHead], [0x00, 0x61, 0x73, 0x6d], 'deferred module wasm magic');
    ok(`${variant}: deferred module is a wasm binary`);

    const glue = readFileSync(join(dir, 'chdb.mjs'), 'utf8');
    assert.ok(glue.includes('wasmBinaryFile??=findWasmBinary()'), 'lazy-load patch: wasmBinaryFile fallback');
    assert.ok(glue.includes('pt__'), 'lazy-load patch: placeholder-table redispatch');
    assert.ok(!glue.includes('chdbWriteProfile'), 'shipped glue must NOT carry the profile-collect patch');
    ok(`${variant}: glue carries lazy-load patches only`);
  }

  // ---- hot paths: engine + full SDK surface, no deferred involvement ------
  // (asserted for real below against a copy that has no deferred module)
  {
    const r = probe(dir, `
      console.log(await q('SELECT sum(number), count() FROM numbers(1000)'));
      console.log(await q('SELECT number % 3 AS g, count() FROM numbers(9) GROUP BY g ORDER BY g'));
      console.log(await q('SELECT a.number * 10 + b.number FROM numbers(2) a JOIN numbers(2) b ON a.number = b.number ORDER BY 1'));
      try { await q('SELECT no_such_function_xyz(1)'); console.log('NO-ERROR'); }
      catch (e) { console.log(e.message.includes('no_such_function_xyz') ? 'ERROR-OK' : 'BAD:' + e.message.slice(0, 60)); }
    `);
    assert.strictEqual(r.status, 0, `hot probe failed: ${r.stderr?.slice(-300)}`);
    const [sum, ...rest] = r.stdout.trim().split('\n');
    assert.strictEqual(sum, '499500,1000');
    assert.strictEqual(rest.slice(0, 3).join('|'), '0,3|1,3|2,3');
    assert.strictEqual(rest[3], '0');
    assert.strictEqual(rest[4], '11');
    assert.strictEqual(rest[5], 'ERROR-OK');
    ok(`${variant}: hot queries + error reporting exact results`);
  }

  // ---- session + streaming + putFile on the split bundle ------------------
  {
    const r = probe(dir, `
      const conn = await db.connect();
      await conn.query('CREATE TABLE t (x Int32, s String) ENGINE = Memory');
      await conn.query("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')");
      console.log((await conn.query('SELECT sum(x), max(s) FROM t')).text().trim());
      let rows = 0;
      for await (const chunk of conn.queryStream('SELECT number FROM numbers(50000)', 'CSV'))
        rows += chunk.text().split('\\n').filter(Boolean).length;
      console.log('rows=' + rows);
      await db.putFile('/split-test.csv', new TextEncoder().encode('7,x\\n8,y\\n'));
      console.log(await q("SELECT sum(c1), max(c2) FROM file('/split-test.csv', CSV)"));
    `);
    assert.strictEqual(r.status, 0, `session probe failed: ${r.stderr?.slice(-300)}`);
    const lines = r.stdout.trim().split('\n');
    assert.strictEqual(lines[0], '6,"c"');
    assert.strictEqual(lines[1], 'rows=50000');
    assert.strictEqual(lines[2], '15,"y"');
    ok(`${variant}: session DDL/DML, streaming (50k rows), putFile+file() on split bundle`);
  }

  // ---- cold path: lazy-loads the deferred module and computes correctly ---
  {
    // toModifiedJulianDay/fromModifiedJulianDay are deliberately NOT in
    // profile-corpus.sql; enforced here, or adding them to the corpus would
    // silently turn the cold-path checks into hot-path ones.
    const corpus = readFileSync(join(pkgDir, 'tools/split/profile-corpus.sql'), 'utf8');
    assert.ok(!corpus.includes('ModifiedJulianDay'), 'profile-corpus.sql must not contain the cold-probe functions');
    const r = probe(dir, `console.log(await q("SELECT toModifiedJulianDay('2024-03-15'), fromModifiedJulianDay(60384)"));`);
    assert.strictEqual(r.status, 0, `cold probe failed: ${r.stderr?.slice(-300)}`);
    assert.strictEqual(lastLine(r), '60384,"2024-03-15"');
    ok(`${variant}: cold function lazy-loads deferred module, exact result`);
  }

  // ---- negative controls: a COPY of the bundle without the deferred module.
  // Never mutate the real dir: a hard kill mid-test would leave it broken for
  // whatever reads it next (in CI, the split-artifact upload).
  {
    const noDeferred = join(tmpRoot, `no-deferred-${variant}`);
    mkdirSync(noDeferred);
    copyFileSync(join(dir, 'chdb.mjs'), join(noDeferred, 'chdb.mjs'));
    copyFileSync(join(dir, 'chdb.wasm'), join(noDeferred, 'chdb.wasm'));

    // Everything above except the cold query must STILL work: init, session,
    // streaming, files, error reporting — proof the primary is self-contained.
    const hot = probe(noDeferred, `
      console.log(await q('SELECT sum(number), count() FROM numbers(1000)'));
      const conn = await db.connect();
      await conn.query('CREATE TABLE t (x Int32) ENGINE = Memory');
      await conn.query('INSERT INTO t SELECT number FROM numbers(10)');
      console.log((await conn.query('SELECT sum(x) FROM t')).text().trim());
      try { await q('SELECT no_such_function_xyz(1)'); } catch (e) { console.log('ERROR-OK'); }
    `);
    assert.strictEqual(hot.status, 0, `hot-without-deferred failed: ${hot.stderr?.slice(-300)}`);
    assert.strictEqual(hot.stdout.trim().split('\n').join('|'), '499500,1000|45|ERROR-OK');
    ok(`${variant}: primary fully self-contained without deferred module`);

    // The cold query must fail FAST with a real error — not hang (the
    // historical failure mode) and not succeed.
    const cold = probe(noDeferred, `console.log(await q("SELECT toModifiedJulianDay('2024-03-15')"));`, 120000);
    assert.notStrictEqual(cold.status, 0, 'cold query without deferred must fail');
    assert.notStrictEqual(cold.status, null, 'cold query without deferred must not hang until timeout');
    ok(`${variant}: cold call without deferred fails fast (no hang)`);
  }
}

// ---- tooling guard rails (repo-level, bundle-independent) -------------------
{
  // patch-glue is idempotent on an already-patched glue…
  const patched = join(bundles[0][1], 'chdb.mjs');
  const tmp = tmpFile('glue.mjs');
  copyFileSync(patched, tmp);
  const before = readFileSync(tmp, 'utf8');
  const again = spawnSync(process.execPath, [join(pkgDir, 'tools/split/patch-glue.mjs'), tmp, '--lazy-load'], { encoding: 'utf8' });
  assert.strictEqual(again.status, 0, again.stderr);
  assert.ok(again.stdout.includes('already patched'));
  assert.strictEqual(readFileSync(tmp, 'utf8'), before, 'idempotent re-patch must not change the file');
  ok('patch-glue: idempotent on already-patched glue');

  // …and fails loudly when its anchors are missing (emscripten upgrade guard).
  writeFileSync(tmp, 'export default function notAGlue() {}');
  const bad = spawnSync(process.execPath, [join(pkgDir, 'tools/split/patch-glue.mjs'), tmp, '--lazy-load'], { encoding: 'utf8' });
  assert.notStrictEqual(bad.status, 0, 'patch-glue must fail on foreign input');
  ok('patch-glue: fails loudly when anchors are missing');

  // copy-artifacts refuses a WASM_SPLIT_MODULE build tree (its chdb.wasm is
  // the profiling-instrumented module, never shippable).
  const fakeBuild = join(tmpRoot, 'fake-build');
  mkdirSync(fakeBuild);
  for (const f of ['chdb.mjs', 'chdb.wasm', 'chdb.wasm.orig']) writeFileSync(join(fakeBuild, f), 'x');
  const guard = spawnSync(process.execPath, [join(pkgDir, 'scripts/copy-artifacts.mjs'), fakeBuild], { encoding: 'utf8' });
  assert.notStrictEqual(guard.status, 0, 'copy-artifacts must reject an instrumented build tree');
  assert.ok((guard.stderr + guard.stdout).includes('instrumented'), 'guard message names the reason');
  ok('copy-artifacts: refuses to ship from an instrumented build tree');
}

console.log(`PASS split.test (${checks} checks, bundles: ${bundles.map(([v]) => v).join('+')})`);
