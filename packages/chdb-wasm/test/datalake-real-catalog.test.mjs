// DataLakeCatalog against a REAL Iceberg REST catalog service (no mock):
// apache/iceberg-rest-fixture (the Apache Iceberg project's official test
// catalog, used by pyiceberg's own CI) in docker, MinIO as S3 (verifies SigV4),
// pyiceberg writing through the REST protocol, chdb-wasm reading through it.
// This closes the mock's fidelity gap: real LoadTableResult shapes, real
// namespace listing, real pagination behavior.
//
// Requires docker + minio + the pyiceberg venv; skips cleanly without them.
//   node packages/chdb-wasm/test/datalake-real-catalog.test.mjs

import assert from 'node:assert';
import { spawn, execFileSync, execSync } from 'node:child_process';
import { existsSync, mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { AsyncChdb } from '../src/index.ts';

const MODULE =
  process.env.CHDB_WASM_MJS ||
  fileURLToPath(new URL('../../../buildwasm/programs/wasm/chdb.mjs', import.meta.url));
const PY = process.env.ICEBERG_PY || '/tmp/iceberg-venv/bin/python';
const FIXTURE = fileURLToPath(new URL('./datalake/make_iceberg_rest_table.py', import.meta.url));
const MINIO_BIN = process.env.MINIO_BIN || 'minio';
const CATALOG_IMAGE = process.env.ICEBERG_REST_IMAGE || 'apache/iceberg-rest-fixture:latest';

const S3_PORT = 15976;
// The fixture image listens on a fixed port (its REST_PORT env is ignored).
const CATALOG_PORT = 8181;
const S3_ENDPOINT = `http://127.0.0.1:${S3_PORT}`;
const CATALOG_URI = `http://127.0.0.1:${CATALOG_PORT}`;
const BUCKET = 'realcatalog';
const KEY = 'minioadmin';
const SECRET = 'minioadmin';

if (!existsSync(PY)) {
  console.log(`SKIP datalake-real-catalog: no pyiceberg venv at ${PY}`);
  process.exit(0);
}
for (const [cmd, args] of [['docker', ['info']], [MINIO_BIN, ['--version']]]) {
  try { execFileSync(cmd, args, { stdio: 'ignore' }); }
  catch { console.log(`SKIP datalake-real-catalog: ${cmd} unavailable`); process.exit(0); }
}

const CONTAINER = `chdb-iceberg-rest-${process.pid}`;

// --- 1. MinIO ---
const dataDir = mkdtempSync(join(tmpdir(), 'minio-real-'));
const minio = spawn(MINIO_BIN, ['server', dataDir, '--address', `127.0.0.1:${S3_PORT}`, '--console-address', '127.0.0.1:0'], {
  stdio: ['ignore', 'ignore', 'pipe'],
  env: { ...process.env, MINIO_ROOT_USER: KEY, MINIO_ROOT_PASSWORD: SECRET },
});
let minioStderr = '';
minio.stderr.on('data', (d) => { minioStderr += d; });

const waitHttp = async (url, what, timeoutMs = 60000) => {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    try { await fetch(url); return; }
    catch {
      if (Date.now() > deadline) throw new Error(`${what} did not come up`);
      await new Promise((r) => setTimeout(r, 300));
    }
  }
};

let db;
let catalogStarted = false;
try {
  await waitHttp(S3_ENDPOINT, 'minio');
  execFileSync(PY, ['-c', `
import boto3
boto3.client("s3", endpoint_url="${S3_ENDPOINT}", aws_access_key_id="${KEY}",
             aws_secret_access_key="${SECRET}", region_name="us-east-1").create_bucket(Bucket="${BUCKET}")
`]);

  // --- 2. the real REST catalog (docker, host network so it reaches MinIO) ---
  execSync(
    `docker run -d --rm --name ${CONTAINER} --network host ` +
    `-e CATALOG_WAREHOUSE=s3://${BUCKET}/warehouse ` +
    `-e CATALOG_IO__IMPL=org.apache.iceberg.aws.s3.S3FileIO ` +
    `-e CATALOG_S3_ENDPOINT=${S3_ENDPOINT} ` +
    `-e CATALOG_S3_PATH__STYLE__ACCESS=true ` +
    `-e AWS_ACCESS_KEY_ID=${KEY} -e AWS_SECRET_ACCESS_KEY=${SECRET} -e AWS_REGION=us-east-1 ` +
    CATALOG_IMAGE,
    { stdio: ['ignore', 'ignore', 'inherit'] });
  catalogStarted = true;
  await waitHttp(`${CATALOG_URI}/v1/config`, 'iceberg-rest-fixture', 90000);

  // --- 3. pyiceberg writes THROUGH the REST catalog ---
  const descriptor = JSON.parse(
    execFileSync(PY, [FIXTURE, CATALOG_URI, S3_ENDPOINT, KEY, SECRET], { encoding: 'utf8' }).trim());
  assert.strictEqual(descriptor.table, 'readings');

  // --- 4. chdb-wasm against the real catalog ---
  db = await AsyncChdb.create({ moduleUrl: MODULE });
  const conn = await db.connect();
  const q = async (sql, fmt = 'TabSeparated') => (await conn.query(sql, fmt)).text().trim();

  await q('SET allow_experimental_database_iceberg = 1');
  await q(`
    CREATE DATABASE real
    ENGINE = DataLakeCatalog('${CATALOG_URI}/v1', '${KEY}', '${SECRET}')
    SETTINGS catalog_type = 'rest', warehouse = 's3://${BUCKET}/warehouse',
             storage_endpoint = '${S3_ENDPOINT}', vended_credentials = false`);

  const tables = await q('SHOW TABLES FROM real');
  assert.strictEqual(tables, 'smoke.readings', `SHOW TABLES: ${tables}`);
  console.log('ok: SHOW TABLES via the real REST catalog');

  const rows = await q('SELECT sensor, value FROM real.`smoke.readings` ORDER BY value', 'CSV');
  assert.strictEqual(rows, ['"a",10', '"b",20', '"a",30', '"c",40', '"b",50'].join('\n'), `rows: ${rows}`);
  console.log('ok: SELECT (5 rows, 2 snapshots, MinIO-verified SigV4)');

  const agg = await q("SELECT sensor, sum(value) FROM real.`smoke.readings` GROUP BY sensor ORDER BY sensor", 'CSV');
  assert.strictEqual(agg, ['"a",40', '"b",70', '"c",40'].join('\n'), `agg: ${agg}`);
  console.log('ok: aggregation');

  await q('DROP DATABASE real');
  console.log('PASS datalake-real-catalog.test (apache/iceberg-rest-fixture + MinIO)');
} finally {
  if (db) await db.terminate().catch(() => {});
  if (catalogStarted) { try { execSync(`docker stop ${CONTAINER}`, { stdio: 'ignore' }); } catch {} }
  minio.kill('SIGKILL');
}
