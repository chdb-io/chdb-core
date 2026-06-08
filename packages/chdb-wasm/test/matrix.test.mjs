// Query matrix test for the chdb-wasm package. Exercises a broad cross-section of
// SQL (aggregation, GROUP BY, ORDER BY, JOIN, subquery, CTE, window, formats,
// format() parsing, system tables, CREATE/INSERT/SELECT/DROP) through the
// worker-based AsyncChdb API, asserting exact results.
//
// Runs against whichever bundle CHDB_WASM_MJS points at, so CI can run it for both
// the threaded (mt) and single-threaded (st) builds:
//   CHDB_WASM_MJS=/abs/path/to/chdb.mjs node packages/chdb-wasm/test/matrix.test.mjs

import assert from 'node:assert';
import { AsyncChdb } from '../src/index.ts';

const MODULE =
  process.env.CHDB_WASM_MJS ||
  '/home/ubuntu/code/chdb-wasm/buildwasm/programs/wasm/chdb.mjs';

const db = await AsyncChdb.create({ moduleUrl: MODULE });
const q = async (sql, fmt = 'TabSeparated') => (await db.query(sql, fmt)).text().trim();

let pass = 0;
const cases = [];
const check = (name, fn) => cases.push([name, fn]);

// --- basics / functions ---
check('select1', async () => assert.strictEqual(await q('SELECT 1'), '1'));
check('arith', async () => assert.strictEqual(await q('SELECT 2 + 3 * 4, pow(2, 10)'), '14\t1024'));
check('string', async () => assert.strictEqual(await q("SELECT lower('AB'), substring('hello', 2, 3), length('abc')"), 'ab\tell\t3'));
check('date', async () => assert.strictEqual(await q("SELECT toYYYYMM(toDate('2024-03-01'))"), '202403'));

// --- scan / aggregation ---
check('numbers', async () => assert.strictEqual(await q('SELECT count() FROM numbers(1000000)'), '1000000'));
check('agg', async () => assert.strictEqual(await q('SELECT count(), sum(number), min(number), max(number) FROM numbers(100000)'), '100000\t4999950000\t0\t99999'));
check('agg_heavy', async () => assert.strictEqual(await q('SELECT count() FROM numbers(50000000)'), '50000000'));
check('groupby', async () => assert.strictEqual(await q('SELECT number % 3 AS g, count() FROM numbers(3000) GROUP BY g ORDER BY g', 'CSV'), '0,1000\n1,1000\n2,1000'));
check('having', async () => assert.strictEqual(await q('SELECT number % 10 AS g, count() c FROM numbers(10000) GROUP BY g HAVING c > 500 ORDER BY g LIMIT 1', 'CSV'), '0,1000'));

// --- order / distinct / relational ---
check('orderby', async () => assert.strictEqual(await q('SELECT number FROM numbers(100000) ORDER BY number DESC LIMIT 3', 'CSV'), '99999\n99998\n99997'));
check('distinct', async () => assert.strictEqual(await q('SELECT DISTINCT number % 4 AS x FROM numbers(1000) ORDER BY x', 'CSV'), '0\n1\n2\n3'));
check('join_small', async () => assert.strictEqual(await q('SELECT a.number FROM numbers(5) a JOIN numbers(5) b ON a.number = b.number ORDER BY 1', 'CSV'), '0\n1\n2\n3\n4'));
check('join_big', async () => assert.strictEqual(await q('SELECT count() FROM numbers(100000) a JOIN numbers(100000) b ON a.number = b.number'), '100000'));
check('subquery', async () => assert.strictEqual(await q('SELECT sum(x) FROM (SELECT number * 2 AS x FROM numbers(1000))'), '999000'));
check('cte', async () => assert.strictEqual(await q('WITH t AS (SELECT number n FROM numbers(100)) SELECT sum(n) FROM t'), '4950'));
check('arrayjoin', async () => assert.strictEqual(await q('SELECT arrayJoin([1, 2, 3]) AS x ORDER BY x', 'CSV'), '1\n2\n3'));
check('window', async () => assert.strictEqual(await q('SELECT sum(number) OVER (ORDER BY number) FROM numbers(4)', 'CSV'), '0\n1\n3\n6'));

// --- high-order aggregates ---
check('uniq', async () => assert.strictEqual(await q('SELECT uniqExact(number % 100) FROM numbers(100000)'), '100'));
check('quantile', async () => assert.strictEqual(await q('SELECT quantileExact(0.5)(number) FROM numbers(10001)'), '5000'));

// --- output formats ---
check('fmt_csv', async () => assert.strictEqual(await q('SELECT 1, 2, 3', 'CSV'), '1,2,3'));
check('fmt_json_each', async () => assert.strictEqual(await q('SELECT 1 AS a, 2 AS b', 'JSONEachRow'), '{"a":1,"b":2}'));
check('fmt_values', async () => assert.strictEqual(await q('SELECT number FROM numbers(3)', 'Values'), '(0),(1),(2)'));
check('fmt_pretty', async () => assert.ok((await q('SELECT 1 AS a', 'Pretty')).includes('1'), 'Pretty renders'));

// --- input parsing (format table function) ---
check('parse_csv', async () => assert.strictEqual(await q("SELECT sum(c1) FROM format(CSV, 'c1 Int32', '1\n2\n3')"), '6'));
check('parse_json', async () => assert.strictEqual(await q("SELECT count() FROM format(JSONEachRow, '{\"a\":1}\n{\"a\":2}')"), '2'));

// --- system tables ---
check('sys_one', async () => assert.strictEqual(await q('SELECT dummy FROM system.one'), '0'));
check('sys_settings', async () => assert.ok(Number(await q('SELECT count() FROM system.settings')) > 100, 'has settings'));
check('sys_functions', async () => assert.ok(Number(await q('SELECT count() FROM system.functions')) > 100, 'has functions'));

// --- DDL / DML read-write round trip ---
check('ddl_dml', async () => {
  await db.query('CREATE TABLE m (id Int32, s String) ENGINE = Memory');
  await db.query("INSERT INTO m VALUES (1, 'a'), (2, 'b'), (3, 'c')");
  await db.query('INSERT INTO m SELECT number, toString(number) FROM numbers(100)');
  assert.strictEqual(await q('SELECT count(), sum(id) FROM m'), '103\t4956');
  await db.query('CREATE TABLE m2 ENGINE = Memory AS SELECT number n FROM numbers(10)');
  assert.strictEqual(await q('SELECT sum(n) FROM m2'), '45');
  await db.query('DROP TABLE m');
  await db.query('DROP TABLE m2');
});

// --- local file IO: put a file into the wasm FS, read it back with file() ---
check('file_csv', async () => {
  await db.putFile('/m/data.csv', new TextEncoder().encode('id,name\n1,alice\n2,bob\n3,carol\n'));
  assert.strictEqual(await q("SELECT count() FROM file('/m/data.csv','CSVWithNames')"), '3');
  assert.strictEqual(await q("SELECT name FROM file('/m/data.csv','CSVWithNames') WHERE id=2", 'CSV'), '"bob"');
});
check('file_jsonl', async () => {
  await db.putFile('/m/data.jsonl', new TextEncoder().encode('{"x":10}\n{"x":20}\n{"x":30}\n'));
  assert.strictEqual(await q("SELECT sum(x) FROM file('/m/data.jsonl','JSONEachRow')"), '60');
});
check('file_nested_dirs', async () => {
  // putFile must mkdir every parent segment, not just one level.
  await db.putFile('/x/y/z/data.csv', new TextEncoder().encode('id\n1\n2\n'));
  assert.strictEqual(await q("SELECT count() FROM file('/x/y/z/data.csv','CSVWithNames')"), '2');
});
// Note: registerFile() is browser-only — it reads the Blob with FileReaderSync, a Web
// Worker API not available under Node. The browser test (browser-run.mjs) covers it on
// both bundles; in Node, use putFile()/a real path instead.

let failed = 0;
for (const [name, fn] of cases) {
  try { await fn(); pass++; console.log(`ok   ${name}`); }
  catch (e) { failed++; console.error(`FAIL ${name}: ${(e && e.message || e).toString().split('\n')[0]}`); }
}
await db.terminate();

console.log(`\nmatrix: ${pass}/${cases.length} passed (module=${MODULE.includes('-st') || MODULE.includes('/st/') ? 'st' : 'mt'})`);
if (failed) { console.error(`${failed} failed`); process.exit(1); }
