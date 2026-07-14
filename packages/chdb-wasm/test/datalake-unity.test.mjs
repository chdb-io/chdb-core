// End-to-end Unity-catalog + Delta Lake test for the chdb-wasm package, headless
// under Node. delta-rs writes a real Delta table (Parquet + _delta_log JSON, two
// commits) into moto; a mock Unity catalog serves it; chdb-wasm reads it through
// ENGINE=DataLakeCatalog with catalog_type='unity' — exercising the legacy C++
// DeltaLakeMetadata path (log replay via signed exists()/range reads, no rust
// delta-kernel on wasm).
//
// Needs the pyiceberg venv with `deltalake` installed (ICEBERG_PY); skips cleanly
// if absent.
//   CHDB_WASM_MJS=/abs/path/to/chdb.mjs node packages/chdb-wasm/test/datalake-unity.test.mjs

import assert from 'node:assert';
import { spawn, execFileSync } from 'node:child_process';
import { existsSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { AsyncChdb } from '../src/index.ts';
import { startMockUnityCatalog } from './datalake/mock-unity-catalog.mjs';

const MODULE =
  process.env.CHDB_WASM_MJS ||
  fileURLToPath(new URL('../../../buildwasm/programs/wasm/chdb.mjs', import.meta.url));
const PY = process.env.ICEBERG_PY || '/tmp/iceberg-venv/bin/python';
const FIXTURE = fileURLToPath(new URL('./datalake/make_delta_table.py', import.meta.url));

const S3_PORT = 15972;
const CATALOG_PORT = 15973;
const S3_ENDPOINT = `http://127.0.0.1:${S3_PORT}`;
const BUCKET = 'deltabucket';

if (!existsSync(PY)) {
  console.log(`SKIP datalake-unity.test: no venv at ${PY} (set ICEBERG_PY)`);
  process.exit(0);
}
try {
  execFileSync(PY, ['-c', 'import deltalake'], { stdio: 'ignore' });
} catch {
  console.log('SKIP datalake-unity.test: deltalake not installed in the venv');
  process.exit(0);
}

const moto = spawn(PY, ['-m', 'moto.server', '-p', String(S3_PORT)], { stdio: ['ignore', 'ignore', 'pipe'] });
let motoStderr = '';
moto.stderr.on('data', (d) => { motoStderr += d; });
const deadline = Date.now() + 30000;
for (;;) {
  if (moto.exitCode !== null) {
    console.error('moto exited early:\n' + motoStderr);
    process.exit(1);
  }
  try { await fetch(S3_ENDPOINT); break; }
  catch {
    if (Date.now() > deadline) {
      console.error('moto did not come up:\n' + motoStderr);
      moto.kill('SIGKILL');
      process.exit(1);
    }
    await new Promise((r) => setTimeout(r, 200));
  }
}

let mockServer;
let db;
try {
  assert.ok((await fetch(`${S3_ENDPOINT}/${BUCKET}`, { method: 'PUT' })).ok, 'create bucket');
  const descriptor = JSON.parse(execFileSync(PY, [FIXTURE, S3_ENDPOINT, BUCKET], { encoding: 'utf8' }).trim());

  mockServer = await startMockUnityCatalog({ port: CATALOG_PORT, descriptor });

  db = await AsyncChdb.create({ moduleUrl: MODULE });
  const conn = await db.connect();
  const q = async (sql, fmt = 'TabSeparated') => (await conn.query(sql, fmt)).text().trim();

  await q('SET allow_experimental_database_unity_catalog = 1');
  await q(`
    CREATE DATABASE unity
    ENGINE = DataLakeCatalog('http://127.0.0.1:${CATALOG_PORT}/api/2.1/unity-catalog', 'testing', 'testing')
    SETTINGS catalog_type = 'unity', warehouse = 'unity',
             storage_endpoint = '${S3_ENDPOINT}', vended_credentials = false`);

  const tables = await q('SHOW TABLES FROM unity');
  assert.strictEqual(tables, 'lakeschema.sales', `SHOW TABLES: ${tables}`);
  console.log('ok: SHOW TABLES via Unity catalog');

  // Two commits -> log replay over 00000000....json files (signed exists() probes).
  const rows = await q('SELECT id, item, price FROM unity.`lakeschema.sales` ORDER BY id', 'CSV');
  assert.strictEqual(
    rows,
    ['1,"book",10.5', '2,"pen",1.25', '3,"desk",99', '4,"lamp",25'].join('\n'),
    `SELECT rows: ${rows}`);
  console.log('ok: SELECT full scan (Delta, 2 commits)');

  const agg = await q('SELECT count(), max(price) FROM unity.`lakeschema.sales`');
  assert.strictEqual(agg, '4\t99', `aggregate: ${agg}`);
  console.log('ok: aggregation');

  // Catalog-less direct read through the deltaLake() table function (legacy
  // C++ log replay — no rust delta-kernel on wasm).
  const tf = await q(`SELECT count(), max(price) FROM deltaLake('${S3_ENDPOINT}/${BUCKET}/unity/sales', 'testing', 'testing')`);
  assert.strictEqual(tf, '4\t99', `deltaLake direct read: ${tf}`);
  console.log('ok: deltaLake() direct read');

  await q('DROP DATABASE unity');
  console.log('PASS datalake-unity.test (Unity catalog + Delta Lake legacy metadata via wasm web fetch)');
} finally {
  if (db) await db.terminate().catch(() => {});
  if (mockServer) mockServer.close();
  moto.kill('SIGKILL');
}
