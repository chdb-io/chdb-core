#!/usr/bin/env node
// Verifies a split bundle end to end:
//   1. hot path: a corpus-covered query runs without touching the deferred module
//   2. cold path: a query using functions ABSENT from the corpus succeeds — which
//      is only possible if the placeholder stub lazy-loaded chdb.deferred.wasm
//   3. negative control: with chdb.deferred.wasm renamed away, the same cold
//      query must fail while the hot query still works — proving (2) really
//      exercised the lazy loader rather than the primary having the code
//
// Steps 1+2 and step 3 run in separate child processes (a failed deferred load
// can take the wasm instance down with it).
//
// Usage: verify-split.mjs <dir with chdb.mjs + chdb.wasm + chdb.deferred.wasm>

import { spawnSync } from 'node:child_process';
import { renameSync, existsSync, writeFileSync, mkdtempSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { tmpdir } from 'node:os';
import { fileURLToPath } from 'node:url';

const dir = process.argv[2];
if (!dir || !existsSync(join(dir, 'chdb.deferred.wasm'))) {
  console.error('usage: verify-split.mjs <dir with chdb.mjs/chdb.wasm/chdb.deferred.wasm>');
  process.exit(2);
}
const pkgDir = join(dirname(fileURLToPath(import.meta.url)), '../..');

// Deliberately NOT in profile-corpus.sql — keep it that way.
const COLD_SQL = 'SELECT toModifiedJulianDay(\'2024-03-15\'), fromModifiedJulianDay(60384)';
const HOT_SQL = 'SELECT sum(number), count() FROM numbers(1000)';

const probe = (label, sql, expectOk) => {
  const script = `
    import { AsyncChdb } from ${JSON.stringify(join(pkgDir, 'dist/index.js'))};
    const db = await AsyncChdb.create({ moduleUrl: ${JSON.stringify(join(dir, 'chdb.mjs'))} });
    const r = await db.query(${JSON.stringify(sql)}, 'CSV');
    console.log(r.text().trim());
    await db.terminate();
    process.exit(0);
  `;
  const scriptFile = join(mkdtempSync(join(tmpdir(), 'chdb-verify-')), 'probe.mjs');
  writeFileSync(scriptFile, script);
  const r = spawnSync(process.execPath, [scriptFile], { encoding: 'utf8', timeout: 300000 });
  const ok = r.status === 0;
  if (ok !== expectOk) {
    // No process.exit here: it would skip the caller's finally and leave
    // chdb.deferred.wasm renamed away.
    console.error(`FAIL ${label}: expected ${expectOk ? 'success' : 'failure'}, got ${ok ? 'success' : `failure (${(r.stderr || '').split('\n').filter((l) => /Error|error/.test(l))[0] || r.status})`}`);
    if (ok) console.error(`  output: ${r.stdout.trim().slice(0, 200)}`);
    failed = true;
    return;
  }
  console.log(`ok: ${label}${ok ? ` -> ${r.stdout.trim().split('\n').pop().slice(0, 80)}` : ' (failed as expected)'}`);
};
let failed = false;

probe('hot query on split bundle', HOT_SQL, true);
probe('cold query lazy-loads deferred module', COLD_SQL, true);

const deferred = join(dir, 'chdb.deferred.wasm');
renameSync(deferred, deferred + '.hidden');
try {
  probe('hot query without deferred module', HOT_SQL, true);
  probe('cold query without deferred module', COLD_SQL, false);
} finally {
  renameSync(deferred + '.hidden', deferred);
}
if (failed) process.exit(1);
console.log('verify-split: PASS');
