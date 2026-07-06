// End-to-end DataLakeCatalog test for the chdb-wasm package, headless under Node.
//
// Pipeline: moto (local S3) <- pyiceberg writes a real Iceberg table (Parquet data,
// Avro manifests, JSON metadata) <- mock Iceberg REST catalog serves the table ->
// chdb-wasm creates ENGINE=DataLakeCatalog, lists tables through the catalog and
// SELECTs the data over HTTP (SigV4-signed range reads through the JS bridge).
//
// Requirements beyond the repo: a Python venv with pyiceberg + moto[server]
// (default /tmp/iceberg-venv, override with ICEBERG_PY). Skips cleanly if absent.
//
//   CHDB_WASM_MJS=/abs/path/to/chdb.mjs node packages/chdb-wasm/test/datalake.test.mjs

import assert from 'node:assert';
import { spawn, execFileSync } from 'node:child_process';
import { existsSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { AsyncChdb } from '../src/index.ts';
import { startMockCatalog } from './datalake/mock-rest-catalog.mjs';

const MODULE =
  process.env.CHDB_WASM_MJS ||
  fileURLToPath(new URL('../../../buildwasm/programs/wasm/chdb.mjs', import.meta.url));
const PY = process.env.ICEBERG_PY || '/tmp/iceberg-venv/bin/python';
const FIXTURE = fileURLToPath(new URL('./datalake/make_iceberg_table.py', import.meta.url));

const S3_PORT = 15968;
const CATALOG_PORT = 15969;
const S3_ENDPOINT = `http://127.0.0.1:${S3_PORT}`;
const BUCKET = 'lakebucket';

if (!existsSync(PY)) {
  console.log(`SKIP datalake.test: no pyiceberg venv at ${PY} (set ICEBERG_PY)`);
  process.exit(0);
}

// --- 1. moto: local S3 ---
const moto = spawn(PY, ['-m', 'moto.server', '-p', String(S3_PORT)], { stdio: ['ignore', 'ignore', 'pipe'] });
let motoStderr = '';
moto.stderr.on('data', (d) => { motoStderr += d; });
const deadline = Date.now() + 30000;
for (;;) {
  try {
    await fetch(S3_ENDPOINT, { method: 'GET' });
    break;
  } catch {
    if (Date.now() > deadline) {
      console.error('moto server did not come up:\n' + motoStderr);
      process.exit(1);
    }
    await new Promise((r) => setTimeout(r, 200));
  }
}

let mockServer;
let db;
try {
  const mkBucket = await fetch(`${S3_ENDPOINT}/${BUCKET}`, { method: 'PUT' });
  assert.ok(mkBucket.ok, `create bucket failed: ${mkBucket.status}`);

  // --- 2. pyiceberg: write a real Iceberg table into moto ---
  const descriptor = JSON.parse(execFileSync(PY, [FIXTURE, S3_ENDPOINT, BUCKET], { encoding: 'utf8' }).trim());
  assert.ok(descriptor.metadata_location.startsWith(`s3://${BUCKET}/`), `unexpected metadata location ${descriptor.metadata_location}`);

  // --- 3. mock Iceberg REST catalog ---
  mockServer = await startMockCatalog({ port: CATALOG_PORT, descriptor });

  // --- 4. chdb-wasm through the catalog ---
  db = await AsyncChdb.create({ moduleUrl: MODULE });
  const conn = await db.connect();
  const q = async (sql, fmt = 'TabSeparated') => (await conn.query(sql, fmt)).text().trim();

  await q('SET allow_experimental_database_iceberg = 1');
  await q(`
    CREATE DATABASE lake
    ENGINE = DataLakeCatalog('http://127.0.0.1:${CATALOG_PORT}/v1', 'testing', 'testing')
    SETTINGS catalog_type = 'rest', warehouse = 'warehouse',
             storage_endpoint = '${S3_ENDPOINT}', vended_credentials = false`);

  const tables = await q('SHOW TABLES FROM lake');
  assert.strictEqual(tables, 'lakehouse.events', `SHOW TABLES: ${tables}`);
  console.log('ok: SHOW TABLES via REST catalog');

  const schema = await q('DESCRIBE lake.`lakehouse.events`', 'CSV');
  assert.ok(schema.includes('"id","Nullable(Int64)"'), `schema id: ${schema}`);
  assert.ok(schema.includes('"name","Nullable(String)"'), `schema name: ${schema}`);
  assert.ok(schema.includes('"score","Nullable(Float64)"'), `schema score: ${schema}`);
  console.log('ok: DESCRIBE (schema from catalog)');

  // Data written in two snapshots (5 + 2 rows) — exercises manifest-list + multiple
  // manifests + multiple Parquet files, all fetched from S3 with SigV4 range reads.
  const rows = await q('SELECT id, name, score FROM lake.`lakehouse.events` ORDER BY id', 'CSV');
  assert.strictEqual(
    rows,
    ['1,"alpha",1.5', '2,"beta",2.5', '3,"gamma",3.5', '4,"delta",4.5', '5,"epsilon",5.5', '6,"zeta",6.5', '7,"eta",7.5'].join('\n'),
    `SELECT rows: ${rows}`);
  console.log('ok: SELECT full scan (7 rows, 2 snapshots)');

  const agg = await q('SELECT count(), sum(id), round(avg(score), 2) FROM lake.`lakehouse.events`');
  assert.strictEqual(agg, '7\t28\t4.5', `aggregate: ${agg}`);
  console.log('ok: aggregation');

  const filtered = await q("SELECT name FROM lake.`lakehouse.events` WHERE id > 5 ORDER BY id", 'CSV');
  assert.strictEqual(filtered, '"zeta"\n"eta"', `filter: ${filtered}`);
  console.log('ok: filtered read');

  await q('DROP DATABASE lake');
  console.log('PASS datalake.test (DataLakeCatalog REST + Iceberg-on-S3 via wasm web fetch)');
} finally {
  if (db) await db.terminate().catch(() => {});
  if (mockServer) mockServer.close();
  moto.kill('SIGKILL');
}
