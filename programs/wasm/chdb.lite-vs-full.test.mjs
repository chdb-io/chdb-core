// Asserts that a chdb-wasm bundle's aggregate-function registry matches the
// variant it was built as: the lite trim set (default) drops ~70 niche
// aggregates, the full build (CHDB_WASM_FULL=1, i.e. CHDB_LITE=OFF) keeps them.
//
// The probes are the two categories most visibly missing from lite:
//   lttb / largestTriangleThreeBuckets   (downsampling)
//   quantileExactInclusive / quantilesExactInclusive  (R-7 interpolation)
// A control (quantileExact) must work in BOTH, so a bundle that fails to load
// or answers nothing at all can't masquerade as "correctly trimmed".
//
// Run with a Memory64-capable Node (>= 23):
//   node chdb.lite-vs-full.test.mjs <path-to-chdb.mjs> <lite|full>
//   node chdb.lite-vs-full.test.mjs                    # ./chdb.mjs, expects lite

import assert from 'node:assert';
import { resolve } from 'node:path';
import { pathToFileURL } from 'node:url';

const [rawPath = './chdb.mjs', expect = 'lite'] = process.argv.slice(2);
if (!['lite', 'full'].includes(expect)) {
    console.error('usage: node chdb.lite-vs-full.test.mjs [path-to-chdb.mjs] [lite|full]');
    process.exit(2);
}

const { default: createChdbModule } = await import(pathToFileURL(resolve(rawPath)).href);
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
    if (err) { const e = new Error(err); e.isQueryError = true; throw e; }
    return out;
}

// Control: the engine answers at all, and keeps an aggregate BOTH variants have.
assert.strictEqual(query('SELECT count() FROM numbers(1000)').trim(), '1000');
assert.strictEqual(query('SELECT quantileExact(0.5)(number) FROM numbers(11)').trim(), '5');
console.log('control: count()=1000, quantileExact(0.5) over numbers(11)=5  [both variants: OK]');

// Each probe must return `expected` on a full build, and raise an unknown-function
// error on a lite one.
function checkAggregate(label, sql, expected) {
    let result = null;
    let errMsg = null;
    try {
        result = query(sql).trim();
    } catch (e) {
        if (!e.isQueryError) throw e;
        errMsg = e.message;
    }

    if (expect === 'full') {
        if (expected instanceof RegExp)
            assert.match(result ?? '', expected, `${label}: got ${result} (err: ${errMsg})`);
        else
            assert.strictEqual(result, expected, `${label}: expected ${expected}, got ${result} (err: ${errMsg})`);
        console.log(`${label} -> ${result}  [present in full: OK]`);
    } else {
        assert.strictEqual(result, null, `${label}: expected it to be absent from the lite build, got: ${result}`);
        assert.match(
            errMsg,
            /Unknown aggregate function|UNKNOWN_AGGREGATE_FUNCTION|does not exist|UNKNOWN_FUNCTION/i,
            `${label}: expected an unknown-function error, got: ${errMsg}`);
        console.log(`${label} -> unknown function  [absent from lite: OK]`);
    }
}

// lttb downsamples 10 points to 4 buckets. The first and last input points are
// always kept; on perfectly linear data every candidate triangle has zero area,
// so the two middle picks are implementation tie-breaks — assert the shape and
// the exact endpoints, not the interior.
const lttbShape = /^"\[\(0,0\),\(\d+,\d+\),\(\d+,\d+\),\(9,90\)\]"$/;
checkAggregate('lttb(4)',
    'SELECT lttb(4)(number, number * 10) FROM numbers(10)', lttbShape);
checkAggregate('largestTriangleThreeBuckets(4)',
    'SELECT largestTriangleThreeBuckets(4)(number, number * 10) FROM numbers(10)', lttbShape);

// quantileExactInclusive is R-7 interpolation: over 0..9, p50 = 4.5 (vs
// quantileExact's non-interpolating 5), p25 = 2.25.
checkAggregate('quantileExactInclusive(0.5)',
    'SELECT quantileExactInclusive(0.5)(number) FROM numbers(10)', '4.5');
checkAggregate('quantilesExactInclusive(0.25,0.5)',
    'SELECT quantilesExactInclusive(0.25, 0.5)(number) FROM numbers(10)', '"[2.25,4.5]"');

console.log(`\nregistry matches the '${expect}' variant: ${rawPath}`);
