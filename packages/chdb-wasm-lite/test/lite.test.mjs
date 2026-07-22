// Tests for the chdb-wasm-lite bundle: the size gate, the common-SQL surface
// that must be fully self-contained, and the "not in lite" behavior of
// everything else.
//
//   node test/lite.test.mjs           # uses ../dist (run npm run build first)
//   CHDB_LITE_DIR=/path node ...      # or point at an assembled dist
//
// Skips cleanly when the default dist has not been assembled; an explicitly
// set CHDB_LITE_DIR that is missing artifacts fails loudly.

import assert from 'node:assert';
import { readFileSync, existsSync } from 'node:fs';
import { spawnSync } from 'node:child_process';
import { join, dirname } from 'node:path';
import { writeFileSync, mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { fileURLToPath } from 'node:url';
import { gzipSync } from 'node:zlib';

const pkgDir = dirname(dirname(fileURLToPath(import.meta.url)));
const dir = process.env.CHDB_LITE_DIR || join(pkgDir, 'dist');

if (!existsSync(join(dir, 'chdb.wasm'))) {
  if (process.env.CHDB_LITE_DIR) {
    console.error(`FAIL lite.test: CHDB_LITE_DIR=${dir} is set but chdb.wasm is missing`);
    process.exit(1);
  }
  console.log('SKIP lite.test: no dist/ — run npm run build (see scripts/build-lite.mjs)');
  process.exit(0);
}

let checks = 0;
const ok = (label) => { checks++; console.log(`ok: ${label}`); };

const tmpRoot = mkdtempSync(join(tmpdir(), 'chdb-lite-test-'));
process.on('exit', () => rmSync(tmpRoot, { recursive: true, force: true }));
let tmpSeq = 0;

// Child-process probe through the lite package's OWN dist SDK.
function probe(body, timeoutMs = 300000) {
  const script = `
    import { AsyncChdb } from ${JSON.stringify(join(dir, 'index.js'))};
    const db = await AsyncChdb.create({ moduleUrl: ${JSON.stringify(join(dir, 'chdb.mjs'))} });
    const q = async (sql) => (await db.query(sql, 'CSV')).text().trim();
    ${body}
    await db.terminate();
    process.exit(0);
  `;
  const file = join(tmpRoot, `probe-${++tmpSeq}.mjs`);
  writeFileSync(file, script);
  return spawnSync(process.execPath, [file], { encoding: 'utf8', timeout: timeoutMs });
}

// ---- the size gate: the whole point of lite --------------------------------
{
  const raw = readFileSync(join(dir, 'chdb.wasm'));
  const gz = gzipSync(raw, { level: 9 }).length;
  // Cloudflare Workers allows 10 MiB gzipped for the WHOLE worker; budget
  // 9.5 MiB for the wasm so glue + SDK + user code fit alongside it.
  assert.ok(gz < 9.5 * 1024 * 1024, `chdb.wasm gzips to ${(gz / 1048576).toFixed(2)} MiB — over the 9.5 MiB lite budget`);
  ok(`size gate: chdb.wasm ${(raw.length / 1048576).toFixed(1)} MiB raw, ${(gz / 1048576).toFixed(2)} MiB gzipped (< 9.5 MiB)`);
}

// ---- artifact shape ---------------------------------------------------------
{
  assert.ok(!existsSync(join(dir, 'chdb.deferred.wasm')), 'lite must not ship a deferred module');
  const glue = readFileSync(join(dir, 'chdb.mjs'), 'utf8');
  assert.ok(glue.includes('chdb-wasm-lite'), 'glue must carry the --lite patch');
  assert.ok(!glue.includes('chdbWriteProfile'), 'glue must not carry the profile-collect patch');
  ok('artifact shape: primary-only, lite-patched glue');
}

// ---- the common-SQL surface must be fully self-contained --------------------
{
  const r = probe(`
    console.log(await q('SELECT sum(number), count() FROM numbers(1000)'));
    console.log(await q('SELECT number % 3 AS g, count() FROM numbers(9) GROUP BY g ORDER BY g'));
    console.log(await q('SELECT a.number * 10 + b.number FROM numbers(2) a JOIN numbers(2) b ON a.number = b.number ORDER BY 1'));
    console.log(await q('SELECT number, row_number() OVER (ORDER BY number DESC) FROM numbers(3) ORDER BY number LIMIT 1'));
    console.log(await q("SELECT upper(concat('ch', 'db')), toYear(toDate('2024-03-15')), round(pi(), 2), JSONExtractInt(concat('{', char(34), 'a', char(34), ': 42}'), 'a')"));
    console.log(await q('SELECT quantile(0.5)(number), uniqExact(number % 10), topK(1)(number % 3) FROM numbers(1000)'));
  `);
  assert.strictEqual(r.status, 0, `common-SQL probe failed: ${r.stderr?.slice(-300)}`);
  const lines = r.stdout.trim().split('\n');
  assert.strictEqual(lines[0], '499500,1000');
  assert.strictEqual(lines.slice(1, 4).join('|'), '0,3|1,3|2,3');
  assert.strictEqual(lines.slice(4, 6).join('|'), '0|11'); // join outputs 2 rows
  assert.strictEqual(lines[6], '0,3');
  assert.strictEqual(lines[7], '"CHDB",2024,3.14,42');
  assert.strictEqual(lines[8], '499.5,10,"[0]"');
  ok('common SQL: aggregation, joins, windows, everyday functions — exact results');
}

// ---- files, sessions, streaming ---------------------------------------------
{
  const r = probe(`
    await db.putFile('/data.csv', new TextEncoder().encode('7,x\\n8,y\\n'));
    console.log(await q("SELECT sum(c1), max(c2) FROM file('/data.csv', CSV)"));
    const conn = await db.connect();
    await conn.query('CREATE TABLE t (x Int32, s String) ENGINE = Memory');
    await conn.query("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')");
    console.log((await conn.query('SELECT sum(x), max(s) FROM t')).text().trim());
    let rows = 0;
    for await (const chunk of conn.queryStream('SELECT number FROM numbers(50000)', 'CSV'))
      rows += chunk.text().split('\\n').filter(Boolean).length;
    console.log('rows=' + rows);
    console.log(await q("SELECT count() FROM file('/data.csv', CSV)"));
  `);
  assert.strictEqual(r.status, 0, `files/session probe failed: ${r.stderr?.slice(-300)}`);
  const lines = r.stdout.trim().split('\n');
  assert.strictEqual(lines[0], '15,"y"');
  assert.strictEqual(lines[1], '6,"c"');
  assert.strictEqual(lines[2], 'rows=50000');
  assert.strictEqual(lines[3], '2');
  ok('putFile + file(), session DDL/DML, streaming (50k rows)');
}

// ---- url()/s3() are IN the bundle (they fail on the network, not on lite) ---
{
  const r = probe(`
    for (const sql of ["SELECT * FROM url('http://127.0.0.1:1/x.csv', CSV) LIMIT 0",
                       "SELECT * FROM s3('http://127.0.0.1:1/b/x.csv', NOSIGN, CSV) LIMIT 0"]) {
      try { await q(sql); console.log('UNEXPECTED-OK'); }
      catch (e) {
        const m = e.message;
        console.log(m.includes('chdb-wasm-lite') ? 'COLD' : (/fetch|Failed|HTTP|Cannot/i.test(m) ? 'NETWORK-ERROR' : 'OTHER:' + m.slice(0, 50)));
      }
    }
  `);
  assert.strictEqual(r.status, 0, `remote-read probe failed: ${r.stderr?.slice(-300)}`);
  assert.strictEqual(r.stdout.trim().split('\n').join('|'), 'NETWORK-ERROR|NETWORK-ERROR',
    'url()/s3() must reach the HTTP layer (hot), not the lite cold stub');
  ok('url() and s3() read paths are compiled in (fail on network, not on lite)');
}

// ---- everything else: immediate, self-explanatory error ---------------------
{
  const r = probe(`
    try { await q("SELECT toModifiedJulianDay('2024-03-15')"); console.log('UNEXPECTED-OK'); }
    catch (e) { console.log(e.message.includes('chdb-wasm-lite') ? 'LITE-ERROR-OK' : 'BAD:' + e.message.slice(0, 80)); }
    console.log(await q('SELECT 1 + 1'));
  `, 120000);
  assert.strictEqual(r.status, 0, `cold probe failed or hung: ${r.stderr?.slice(-300)}`);
  const lines = r.stdout.trim().split('\n');
  assert.strictEqual(lines[0], 'LITE-ERROR-OK');
  assert.strictEqual(lines[1], '2', 'the instance must stay usable after a lite cold error');
  ok('out-of-corpus function throws the lite error fast; instance stays usable');
}

console.log(`PASS lite.test (${checks} checks)`);
