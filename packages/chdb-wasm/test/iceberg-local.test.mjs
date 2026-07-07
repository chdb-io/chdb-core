// icebergLocal() smoke test for the chdb-wasm package: a real Iceberg table
// (written by pyiceberg on the host) is mirrored into the wasm MEMFS with
// putFile, then read back with the icebergLocal table function — exercising
// Avro manifest reading + Parquet data reading with zero networking.
//
// Requires a Python venv with pyiceberg (default /tmp/iceberg-venv, override
// with ICEBERG_PY). Skips cleanly if absent.
//
//   CHDB_WASM_MJS=/abs/path/to/chdb.mjs node packages/chdb-wasm/test/iceberg-local.test.mjs

import assert from 'node:assert';
import { execFileSync } from 'node:child_process';
import { existsSync, mkdtempSync, readdirSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, relative } from 'node:path';
import { fileURLToPath } from 'node:url';
import { AsyncChdb } from '../src/index.ts';

const MODULE =
  process.env.CHDB_WASM_MJS ||
  fileURLToPath(new URL('../../../buildwasm/programs/wasm/chdb.mjs', import.meta.url));
const PY = process.env.ICEBERG_PY || '/tmp/iceberg-venv/bin/python';
const FIXTURE = fileURLToPath(new URL('./datalake/make_iceberg_local.py', import.meta.url));

if (!existsSync(PY)) {
  console.log(`SKIP iceberg-local.test: no pyiceberg venv at ${PY} (set ICEBERG_PY)`);
  process.exit(0);
}

const warehouse = mkdtempSync(join(tmpdir(), 'iceberg-wh-'));
const { table_dir } = JSON.parse(execFileSync(PY, [FIXTURE, warehouse], { encoding: 'utf8' }).trim());

const db = await AsyncChdb.create({ moduleUrl: MODULE });
try {
  // Mirror the table directory into MEMFS at /iceberg/items.
  const MEMFS_TABLE = '/iceberg/items';
  const walk = (dir) =>
    readdirSync(dir, { withFileTypes: true }).flatMap((e) =>
      e.isDirectory() ? walk(join(dir, e.name)) : [join(dir, e.name)]);
  const files = walk(table_dir);
  assert.ok(files.some((f) => f.endsWith('.metadata.json')), 'fixture produced metadata json');
  assert.ok(files.some((f) => f.endsWith('.avro')), 'fixture produced avro manifests');
  assert.ok(files.some((f) => f.endsWith('.parquet')), 'fixture produced parquet data');
  for (const f of files)
    await db.putFile(join(MEMFS_TABLE, relative(table_dir, f)), new Uint8Array(readFileSync(f)));

  const q = async (sql, fmt = 'TabSeparated') => (await db.query(sql, fmt)).text().trim();

  const rows = await q(`SELECT k, v FROM icebergLocal('${MEMFS_TABLE}') ORDER BY k`, 'CSV');
  assert.strictEqual(rows, '10,"ten"\n20,"twenty"\n30,"thirty"\n40,"forty"', `rows: ${rows}`);
  console.log('ok: icebergLocal full scan (2 snapshots)');

  const agg = await q(`SELECT count(), sum(k) FROM icebergLocal('${MEMFS_TABLE}')`);
  assert.strictEqual(agg, '4\t100', `agg: ${agg}`);
  console.log('ok: icebergLocal aggregation');

  console.log('PASS iceberg-local.test (Avro manifests + Parquet data in MEMFS)');
} finally {
  await db.terminate().catch(() => {});
  rmSync(warehouse, { recursive: true, force: true });
}
