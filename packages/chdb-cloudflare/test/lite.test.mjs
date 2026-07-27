// Tests for the chdb-cloudflare bundle: the size gate, the common-SQL surface
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
import { fileURLToPath, pathToFileURL } from 'node:url';
import { gzipSync } from 'node:zlib';

const pkgDir = dirname(dirname(fileURLToPath(import.meta.url)));
const dir = process.env.CHDB_LITE_DIR || join(pkgDir, 'dist');

// All three are read/imported below — gate them together so a partially
// assembled dist (interrupted build-lite.mjs) fails with this clean message
// instead of a raw ENOENT downstream.
const missing = ['chdb.wasm', 'chdb.mjs', 'index.js'].filter((f) => !existsSync(join(dir, f)));
if (missing.length) {
  if (process.env.CHDB_LITE_DIR) {
    console.error(`FAIL lite.test: CHDB_LITE_DIR=${dir} is set but ${missing.join(', ')} is missing`);
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

// The lite bundle links with WASM_JSPI (its async-fetch HTTP bridge is what
// makes url()/s3() work inside Workers); the glue's WebAssembly.Suspending /
// WebAssembly.promising wrappers need the flag to instantiate under Node at
// all. Probe for either wrapper.
const glueSrc = readFileSync(join(dir, 'chdb.mjs'), 'utf8');
const jspiFlags = glueSrc.includes('WebAssembly.promising') || glueSrc.includes('WebAssembly.Suspending')
  ? ['--experimental-wasm-jspi'] : [];

// Child-process probe through the lite package's OWN dist SDK.
function probe(body, timeoutMs = 300000) {
  // file:// URLs, not raw paths: absolute-path ESM specifiers are POSIX-only.
  const script = `
    import { AsyncChdb } from ${JSON.stringify(pathToFileURL(join(dir, 'index.js')).href)};
    const db = await AsyncChdb.create({ moduleUrl: ${JSON.stringify(pathToFileURL(join(dir, 'chdb.mjs')).href)} });
    const q = async (sql) => (await db.query(sql, 'CSV')).text().trim();
    ${body}
    await db.terminate();
    process.exit(0);
  `;
  const file = join(tmpRoot, `probe-${++tmpSeq}.mjs`);
  writeFileSync(file, script);
  const r = spawnSync(process.execPath, [...jspiFlags, file], { encoding: 'utf8', timeout: timeoutMs });
  // A timeout/spawn failure leaves status=null and often an empty stderr;
  // surface the real cause where every assertion below will print it.
  if (r.error || r.signal) r.stderr = `spawn: ${r.error ?? ''} signal=${r.signal ?? ''}\n${r.stderr ?? ''}`;
  return r;
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
  assert.ok(glue.includes('chdb-cloudflare: this SQL feature is not included'), 'glue must carry the --lite patch');
  assert.ok(!glue.includes('chdbWriteProfile'), 'glue must not carry the profile-collect patch');
  assert.ok(glue.includes('WebAssembly.promising'),
    'lite glue must be a JSPI build (WASM_JSPI=ON) — the async HTTP bridge is what lets url()/s3() run in Workers');
  ok('artifact shape: primary-only, lite-patched, JSPI glue');
}

// ---- the common-SQL surface must be fully self-contained --------------------
// Every query prints one LABELED line (multi-row results joined with ' | '),
// so a failure names its query and inserting/reordering probes cannot silently
// shift later assertions.
{
  const r = probe(`
    const row = (label, sql) => q(sql).then((t) => console.log(label + '=' + t.split(String.fromCharCode(10)).join(' | ')));
    await row('agg', 'SELECT sum(number), count() FROM numbers(1000)');
    await row('grp', 'SELECT number % 3 AS g, count() FROM numbers(9) GROUP BY g ORDER BY g');
    await row('join', 'SELECT a.number * 10 + b.number FROM numbers(2) a JOIN numbers(2) b ON a.number = b.number ORDER BY 1');
    await row('win', 'SELECT number, row_number() OVER (ORDER BY number DESC) FROM numbers(3) ORDER BY number LIMIT 1');
    await row('fn', "SELECT upper(concat('ch', 'db')), toYear(toDate('2024-03-15')), round(pi(), 2), JSONExtractInt(concat('{', char(34), 'a', char(34), ': 42}'), 'a')");
    await row('stat', 'SELECT quantile(0.5)(number), uniqExact(number % 10), topK(1)(number % 3) FROM numbers(1000)');
    await row('date', "SELECT toStartOfInterval(toDateTime('2024-03-15 12:34:56'), INTERVAL 15 MINUTE), dateDiff('day', toDate('2024-01-01'), toDate('2024-03-15'))");
    await row('arr', 'SELECT arrayFilter(x -> x % 2 = 0, range(6)), arraySum([1, 2, 3])');
    await row('ip', "SELECT isIPAddressInRange('192.168.1.5', '192.168.1.0/24'), domain('https://www.example.com/a?q=1')");
    await row('fmt', "SELECT n, s FROM format(JSONEachRow, concat('{', char(34), 'n', char(34), ': 1, ', char(34), 's', char(34), ': ', char(34), 'one', char(34), '}')) ORDER BY n");
    await row('lc', 'SELECT lc, count() FROM (SELECT toLowCardinality(toString(number % 3)) AS lc FROM numbers(9)) GROUP BY lc ORDER BY lc');
  `);
  assert.strictEqual(r.status, 0, `common-SQL probe failed: ${r.stderr?.slice(-300)}`);
  const got = Object.fromEntries(r.stdout.trim().split('\n').filter((l) => l.includes('=')).map((l) => [l.slice(0, l.indexOf('=')), l.slice(l.indexOf('=') + 1)]));
  assert.strictEqual(got.agg, '499500,1000');
  assert.strictEqual(got.grp, '0,3 | 1,3 | 2,3');
  assert.strictEqual(got.join, '0 | 11');
  assert.strictEqual(got.win, '0,3');
  assert.strictEqual(got.fn, '"CHDB",2024,3.14,42');
  assert.strictEqual(got.stat, '499.5,10,"[0]"');
  assert.strictEqual(got.date, '"2024-03-15 12:30:00",74');
  assert.strictEqual(got.arr, '"[0,2,4]",6');
  assert.strictEqual(got.ip, '1,"www.example.com"');
  assert.strictEqual(got.fmt, '1,"one"');
  assert.strictEqual(got.lc, '"0",3 | "1",3 | "2",3');
  ok('common SQL: aggregation, joins, windows, dates, arrays, IP/URL, format(), LowCardinality — exact labeled results');
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

// ---- url()/s3() through the JSPI async HTTP bridge --------------------------
// The fixture server lives in the SAME process as the query: only possible
// because JSPI suspends the wasm stack at fetch() and frees the event loop —
// this doubles as the regression test for that property.
{
  const r = probe(`
    const { createServer } = await import('node:http');
    const CSV = 'id,name,score\\n1,alice,9.5\\n2,bob,7.25\\n3,carol,8.0\\n';
    const srv = createServer((req, res) => { res.setHeader('content-type', 'text/csv'); res.end(CSV); });
    await new Promise((resolve) => srv.listen(0, '127.0.0.1', resolve));
    const base = 'http://127.0.0.1:' + srv.address().port;
    console.log(await q("SELECT * FROM url('" + base + "/people_names.csv', CSVWithNames) ORDER BY id"));
    console.log(await q("SELECT count(), max(score) FROM s3('" + base + "/s3bucket/people_names.csv', NOSIGN, CSVWithNames)"));
    srv.close();
  `);
  assert.strictEqual(r.status, 0, `remote-read probe failed: ${r.stderr?.slice(-300)}`);
  const lines = r.stdout.trim().split('\n');
  assert.strictEqual(lines.slice(0, 3).join('|'), '1,"alice",9.5|2,"bob",7.25|3,"carol",8');
  assert.strictEqual(lines[3], '3,9.5');
  ok('url() and s3() read real HTTP data via the JSPI bridge — exact results');
}

// ---- the Workers entry (createChdb): the API Cloudflare users actually call --
// Driven exactly as a Worker would (pre-compiled WebAssembly.Module +
// instantiateWasm), minus workerd itself. The concurrent pair at the end is
// the mutex regression test: url() SUSPENDS the wasm stack at fetch() while a
// second query is issued concurrently — without the entry's FIFO lock that
// second call would re-enter the non-reentrant engine mid-query.
{
  const script = `
    import { readFileSync } from 'node:fs';
    import { createServer } from 'node:http';
    import { pathToFileURL } from 'node:url';
    const dir = ${JSON.stringify(dir)};
    const CSV = 'id,name,score\\n1,alice,9.5\\n2,bob,7.25\\n3,carol,8.0\\n';
    const srv = createServer((req, res) => { res.setHeader('content-type', 'text/csv'); res.end(CSV); });
    await new Promise((resolve) => srv.listen(0, '127.0.0.1', resolve));
    const base = 'http://127.0.0.1:' + srv.address().port;

    const factory = (await import(pathToFileURL(dir + '/chdb.mjs').href)).default;
    const wasmModule = new WebAssembly.Module(readFileSync(dir + '/chdb.wasm'));
    const { createChdb } = await import(pathToFileURL(dir + '/workers.js').href);
    const db = await createChdb(factory, wasmModule);

    console.log('plain=' + (await db.query('SELECT sum(number) FROM numbers(1000)')).text().trim());
    await db.putFile('/w.csv', new TextEncoder().encode('5,x' + String.fromCharCode(10) + '6,y' + String.fromCharCode(10)));
    console.log('file=' + (await db.query("SELECT sum(c1), max(c2) FROM file('/w.csv', CSV)")).text().trim());
    const conn = await db.connect();
    await conn.query('CREATE TABLE wt (x Int32) ENGINE = Memory');
    await conn.query('INSERT INTO wt VALUES (1), (2), (3)');
    console.log('session=' + (await conn.query('SELECT sum(x) FROM wt')).text().trim());
    let rows = 0;
    for await (const c of conn.queryStream('SELECT number FROM numbers(30000)', 'CSV')) rows += c.text().split(String.fromCharCode(10)).filter(Boolean).length;
    console.log('stream=' + rows);
    await conn.close();
    const [u, p] = await Promise.all([
      db.query("SELECT count(), max(score) FROM url('" + base + "/people_names.csv', CSVWithNames)"),
      db.query('SELECT max(number) FROM numbers(100)'),
    ]);
    console.log('concurrent=' + u.text().trim() + ';' + p.text().trim());
    // close() while a stream generator is paused at yield: resuming must throw
    // the clear session-closed error (NOT touch the freed handles), and the
    // instance must stay usable afterwards.
    const conn2 = await db.connect();
    const gen = conn2.queryStream('SELECT number FROM numbers(1000000)', 'CSV');
    const first = await gen.next();
    if (first.done) throw new Error('stream ended in one chunk; test needs a paused generator');
    await conn2.close();
    let after = 'NO-ERROR';
    try { await gen.next(); } catch (e) { after = e.message.includes('closed') ? 'CLOSED-ERROR-OK' : 'OTHER:' + e.message.slice(0, 60); }
    console.log('closeDuringStream=' + after + ';' + (await db.query('SELECT 41 + 1')).text().trim());
    srv.close();
    process.exit(0);
  `;
  const file = join(tmpRoot, 'probe-workers.mjs');
  writeFileSync(file, script);
  const r = spawnSync(process.execPath, [...jspiFlags, file], { encoding: 'utf8', timeout: 300000 });
  if (r.error || r.signal) r.stderr = `spawn: ${r.error ?? ''} signal=${r.signal ?? ''}\n${r.stderr ?? ''}`;
  assert.strictEqual(r.status, 0, `workers-entry probe failed: ${r.stderr?.slice(-400)}`);
  const got = Object.fromEntries(r.stdout.trim().split('\n').filter((l) => l.includes('=')).map((l) => [l.slice(0, l.indexOf('=')), l.slice(l.indexOf('=') + 1)]));
  assert.strictEqual(got.plain, '499500');
  assert.strictEqual(got.file, '11,"y"');
  assert.strictEqual(got.session, '6');
  assert.strictEqual(got.stream, '30000');
  assert.strictEqual(got.concurrent, '3,9.5;99', 'concurrent url()+plain must BOTH be correct (FIFO mutex across the JSPI suspension)');
  assert.strictEqual(got.closeDuringStream, 'CLOSED-ERROR-OK;42',
    'resuming a stream after close() must throw the session-closed error, and the instance must stay usable');
  ok('workers entry (createChdb): query/putFile/session/stream, concurrency serialized across JSPI suspension, close-during-stream is safe');
}

// ---- everything else: immediate, self-explanatory error ---------------------
{
  const r = probe(`
    try { await q("SELECT toModifiedJulianDay('2024-03-15')"); console.log('UNEXPECTED-OK'); }
    catch (e) { console.log(e.message.includes('chdb-cloudflare') ? 'LITE-ERROR-OK' : 'BAD:' + e.message.slice(0, 80)); }
    console.log(await q('SELECT 1 + 1'));
  `, 120000);
  assert.strictEqual(r.status, 0, `cold probe failed or hung: ${r.stderr?.slice(-300)}`);
  const lines = r.stdout.trim().split('\n');
  assert.strictEqual(lines[0], 'LITE-ERROR-OK');
  assert.strictEqual(lines[1], '2', 'the instance must stay usable after a lite cold error');
  ok('out-of-corpus function throws the lite error fast; instance stays usable');
}

console.log(`PASS lite.test (${checks} checks)`);
