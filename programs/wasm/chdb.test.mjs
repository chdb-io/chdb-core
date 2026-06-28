// Minimal end-to-end test for chdb-wasm, mirroring duckdb-wasm's smoke tests.
// Run with a Memory64-capable Node (>= 23):  node chdb.test.mjs
//
// It loads the Emscripten module, runs a few queries via the exported C API,
// and asserts on the returned CSV text.

import assert from 'node:assert';
import createChdbModule from './chdb.mjs';

const mod = await createChdbModule();

// Under the Memory64 ABI, pointers cross the ccall boundary as BigInt. Normalize
// to Number for the JS heap-string helpers (the raw value is fine to pass back
// into ccall, which accepts BigInt for pointer-typed arguments).
const num = x => (typeof x === 'bigint' ? Number(x) : x);

function query(sql, format = 'CSV') {
    const r = mod.ccall('chdb_wasm_query', 'number', ['string', 'string'], [sql, format]);
    if (!num(r)) throw new Error('chdb_wasm_query returned null');
    const errPtr = mod.ccall('chdb_wasm_result_error', 'number', ['number'], [r]);
    const err = num(errPtr) ? mod.UTF8ToString(num(errPtr)) : '';
    const bufPtr = mod.ccall('chdb_wasm_result_buffer', 'number', ['number'], [r]);
    const len = mod.ccall('chdb_wasm_result_length', 'number', ['number'], [r]);
    const out = num(bufPtr) ? mod.UTF8ToString(num(bufPtr), num(len)) : '';
    mod.ccall('chdb_wasm_free_result', null, ['number'], [r]);
    if (err) throw new Error('query error: ' + err);
    return out;
}

// 1. trivial constant
assert.strictEqual(query('SELECT 1').trim(), '1');

// 2. arithmetic + alias
assert.strictEqual(query('SELECT 2 + 3 AS x').trim(), '5');

// 3. a small generated table aggregation
assert.strictEqual(query('SELECT count() FROM numbers(1000)').trim(), '1000');

// 4. string + filter
assert.strictEqual(query("SELECT number FROM numbers(5) WHERE number % 2 = 0").trim().split('\n').join(','), '0,2,4');

// 5. structured output format
assert.strictEqual(query("SELECT 1 AS a, 'hi' AS b", 'JSONEachRow').trim(), '{"a":1,"b":"hi"}');

console.log('chdb-wasm smoke tests passed');
