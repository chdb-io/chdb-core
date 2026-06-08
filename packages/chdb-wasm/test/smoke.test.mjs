// Async smoke test for the chdb-wasm package. Drives the worker-based AsyncChdb
// API (the wasm runs in a worker; the main thread stays free).
//
// Run with a Memory64-capable Node (>= 23):
//   CHDB_WASM_MJS=/abs/path/to/chdb.mjs node packages/chdb-wasm/test/smoke.test.mjs

import assert from 'node:assert';
import { AsyncChdb, getPlatformFeatures, selectBundle } from '../src/index.ts';

const MODULE =
  process.env.CHDB_WASM_MJS ||
  '/home/ubuntu/code/chdb-wasm/buildwasm/programs/wasm/chdb.mjs';

// Platform features: chdb-wasm hard-requires Memory64 + WASM_BIGINT; in Node coi is
// forced true. Assert the invariant selectBundle relies on (rather than just printing).
const features = getPlatformFeatures();
console.log('platform features:', features);
assert.ok(features.wasmBigInt, 'BigInt64Array (WASM_BIGINT) must be available');
assert.ok(features.wasmMemory64, 'Memory64 must be available (Node >= 23)');
assert.strictEqual(features.crossOriginIsolated, true, 'coi is forced true in Node');
const picked = selectBundle({ baseUrl: 'file:///x' });
assert.ok(picked.supported, `selectBundle must be supported in Node: ${JSON.stringify(picked.reasons || [])}`);
assert.ok(picked.variant === 'mt' || picked.variant === 'st', `bundle variant: ${picked.variant}`);

const db = await AsyncChdb.create({ moduleUrl: MODULE });

// 1. implicit-connection queries
assert.strictEqual((await db.query('SELECT 1')).text().trim(), '1');
assert.strictEqual((await db.query('SELECT 2 + 3 AS x')).text().trim(), '5');
assert.strictEqual((await db.query('SELECT count() FROM numbers(1000)')).text().trim(), '1000');
assert.strictEqual(
  (await db.query("SELECT 1 AS a, 'hi' AS b", 'JSONEachRow')).text().trim(),
  '{"a":1,"b":"hi"}',
);

// 2. result metadata
const m = await db.query('SELECT number FROM numbers(1000)');
assert.ok(Number(m.rowsRead) >= 1000, `rowsRead=${m.rowsRead}`);
console.log(`metadata: rowsRead=${m.rowsRead} bytesRead=${m.bytesRead} elapsed=${m.elapsedSeconds.toFixed(4)}s`);

// 3. main thread stays responsive: a timer keeps ticking while queries run
let ticks = 0;
const timer = setInterval(() => { ticks++; }, 1);
const [a, b, c] = await Promise.all([
  db.query('SELECT 1'),
  db.query('SELECT 2'),
  db.query('SELECT 3'),
]);
clearInterval(timer);
assert.deepStrictEqual([a.text().trim(), b.text().trim(), c.text().trim()], ['1', '2', '3']);
console.log(`main-thread timer ticked ${ticks} times during concurrent queries (non-blocking)`);

// 4. explicit connection handle
const conn = await db.connect();
assert.strictEqual((await conn.query('SELECT 7')).text().trim(), '7');

// 4b. streaming a larger result
let streamRows = 0;
let streamChunks = 0;
for await (const chunk of conn.queryStream('SELECT number FROM numbers(50000)')) {
  streamChunks++;
  streamRows += chunk.text().trim().split('\n').filter(Boolean).length;
}
assert.strictEqual(streamRows, 50000, `streamed rows=${streamRows}`);
console.log(`streamed ${streamRows} rows in ${streamChunks} chunk(s)`);
await conn.close();

// 5. errors surface as ChdbError (rejected promise), don't crash the worker
let threw = false;
try {
  await db.query('SELECT this_is_not_valid_sql FROM nope');
} catch (e) {
  threw = true;
  assert.ok(String(e.message).length > 0);
}
assert.ok(threw, 'invalid query should reject');
// worker still usable after an error
assert.strictEqual((await db.query('SELECT 42')).text().trim(), '42');

await db.terminate();
console.log('chdb-wasm async smoke tests passed');
