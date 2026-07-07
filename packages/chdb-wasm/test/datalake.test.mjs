// End-to-end DataLakeCatalog test for the chdb-wasm package, headless under Node.
//
// Pipeline: an S3 backend <- pyiceberg writes real Iceberg tables (Parquet data,
// Avro manifests, JSON metadata) <- mock Iceberg REST catalog serves them ->
// chdb-wasm creates ENGINE=DataLakeCatalog, lists tables through the catalog and
// SELECTs the data over HTTP (SigV4-signed range reads through the JS bridge).
//
// Three tables cover the read paths: plain (2 snapshots), identity-partitioned
// with partition values containing spaces (URL-encoding of object keys), and
// delete-via-overwrite snapshots (pyiceberg cannot write positional deletes;
// that path needs a Spark-written fixture and is a known coverage gap).
//
// S3 backend (CHDB_S3_BACKEND):
//   moto  (default) — python in-process S3; fast, but does NOT verify SigV4
//   minio           — real MinIO server (needs `minio` on PATH or MINIO_BIN);
//                     STRICTLY verifies SigV4 signatures, closing the auth loop
//
// Requirements beyond the repo: a Python venv with pyiceberg + moto[server]
// (default /tmp/iceberg-venv, override with ICEBERG_PY). Skips cleanly if absent.
//
//   CHDB_WASM_MJS=/abs/path/to/chdb.mjs node packages/chdb-wasm/test/datalake.test.mjs

import assert from 'node:assert';
import { spawn, execFileSync } from 'node:child_process';
import { existsSync, mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { AsyncChdb } from '../src/index.ts';
import { startMockCatalog } from './datalake/mock-rest-catalog.mjs';

const MODULE =
  process.env.CHDB_WASM_MJS ||
  fileURLToPath(new URL('../../../buildwasm/programs/wasm/chdb.mjs', import.meta.url));
const PY = process.env.ICEBERG_PY || '/tmp/iceberg-venv/bin/python';
const FIXTURE = fileURLToPath(new URL('./datalake/make_iceberg_table.py', import.meta.url));

const BACKEND = process.env.CHDB_S3_BACKEND || 'moto';
const MINIO_BIN = process.env.MINIO_BIN || 'minio';

const S3_PORT = 15968;
const CATALOG_PORT = 15969;
const S3_ENDPOINT = `http://127.0.0.1:${S3_PORT}`;
const BUCKET = 'lakebucket';
// moto accepts any credentials; MinIO's defaults require >= 8-char secrets.
const [KEY, SECRET] = BACKEND === 'minio' ? ['minioadmin', 'minioadmin'] : ['testing', 'testing'];

if (!existsSync(PY)) {
  console.log(`SKIP datalake.test: no pyiceberg venv at ${PY} (set ICEBERG_PY)`);
  process.exit(0);
}

// --- 1. S3 backend ---
let s3;
if (BACKEND === 'minio') {
  try {
    execFileSync(MINIO_BIN, ['--version'], { stdio: 'ignore' });
  } catch {
    console.log(`SKIP datalake.test[minio]: no minio binary (set MINIO_BIN)`);
    process.exit(0);
  }
  const dataDir = mkdtempSync(join(tmpdir(), 'minio-data-'));
  s3 = spawn(MINIO_BIN, ['server', dataDir, '--address', `127.0.0.1:${S3_PORT}`, '--console-address', '127.0.0.1:0'], {
    stdio: ['ignore', 'ignore', 'pipe'],
    env: { ...process.env, MINIO_ROOT_USER: KEY, MINIO_ROOT_PASSWORD: SECRET },
  });
} else {
  s3 = spawn(PY, ['-m', 'moto.server', '-p', String(S3_PORT)], { stdio: ['ignore', 'ignore', 'pipe'] });
}
let s3Stderr = '';
s3.stderr.on('data', (d) => { s3Stderr += d; });
const deadline = Date.now() + 30000;
for (;;) {
  // A dead child means a squatter may own the port — the probe below would bind
  // to it and silently test against stale state. Bail out instead.
  if (s3.exitCode !== null) {
    console.error(`${BACKEND} exited early:\n` + s3Stderr);
    process.exit(1);
  }
  try {
    await fetch(S3_ENDPOINT, { method: 'GET' });
    break;
  } catch {
    if (Date.now() > deadline) {
      console.error(`${BACKEND} server did not come up:\n` + s3Stderr);
      s3.kill('SIGKILL');
      process.exit(1);
    }
    await new Promise((r) => setTimeout(r, 200));
  }
}

let mockServer;
let db;
try {
  // Create the bucket with a signed request (MinIO rejects anonymous PUTs).
  execFileSync(PY, ['-c', `
import boto3
boto3.client("s3", endpoint_url="${S3_ENDPOINT}", aws_access_key_id="${KEY}",
             aws_secret_access_key="${SECRET}", region_name="us-east-1").create_bucket(Bucket="${BUCKET}")
`]);

  // --- 2. pyiceberg: write real Iceberg tables into the backend ---
  const descriptor = JSON.parse(
    execFileSync(PY, [FIXTURE, S3_ENDPOINT, BUCKET, KEY, SECRET], { encoding: 'utf8' }).trim());
  assert.strictEqual(descriptor.tables.length, 3, 'fixture produced 3 tables');

  // --- 3. mock Iceberg REST catalog ---
  mockServer = await startMockCatalog({ port: CATALOG_PORT, descriptor });

  // --- 4. chdb-wasm through the catalog ---
  db = await AsyncChdb.create({ moduleUrl: MODULE });
  const conn = await db.connect();
  const q = async (sql, fmt = 'TabSeparated') => (await conn.query(sql, fmt)).text().trim();

  await q('SET allow_experimental_database_iceberg = 1');
  await q(`
    CREATE DATABASE lake
    ENGINE = DataLakeCatalog('http://127.0.0.1:${CATALOG_PORT}/v1', '${KEY}', '${SECRET}')
    SETTINGS catalog_type = 'rest', warehouse = 'warehouse',
             storage_endpoint = '${S3_ENDPOINT}', vended_credentials = false`);

  const tables = await q('SHOW TABLES FROM lake');
  assert.strictEqual(
    tables.split('\n').sort().join(','),
    'lakehouse.city_events,lakehouse.deleted_events,lakehouse.events',
    `SHOW TABLES: ${tables}`);
  console.log(`ok: SHOW TABLES via REST catalog (3 tables, backend=${BACKEND})`);

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

  // Partition values with spaces -> object keys like .../city=New York/... must be
  // URL-encoded on the wire and stay consistent with the SigV4 canonical URI.
  const cities = await q('SELECT id, city, amount FROM lake.`lakehouse.city_events` ORDER BY id', 'CSV');
  assert.strictEqual(
    cities,
    ['1,"New York",10', '2,"San Francisco",20', '3,"New York",30', '4,"Los Angeles",40'].join('\n'),
    `partitioned rows: ${cities}`);
  const ny = await q(`SELECT count(), sum(amount) FROM lake.\`lakehouse.city_events\` WHERE city = 'New York'`);
  assert.strictEqual(ny, '2\t40', `partition filter: ${ny}`);
  console.log('ok: identity-partitioned table with spaces in partition values');

  // Deleted rows (id=2, id=4) must be absent: the delete produced an overwrite
  // snapshot whose manifests replace the original data file.
  const mor = await q('SELECT id, v FROM lake.`lakehouse.deleted_events` ORDER BY id', 'CSV');
  assert.strictEqual(mor, ['1,"a"', '3,"c"', '5,"e"'].join('\n'), `post-delete rows: ${mor}`);
  console.log('ok: copy-on-write delete snapshot applied (deleted rows absent)');

  await q('DROP DATABASE lake');
  console.log(`PASS datalake.test (DataLakeCatalog REST + Iceberg-on-S3, backend=${BACKEND})`);
} finally {
  if (db) await db.terminate().catch(() => {});
  if (mockServer) mockServer.close();
  s3.kill('SIGKILL');
}
