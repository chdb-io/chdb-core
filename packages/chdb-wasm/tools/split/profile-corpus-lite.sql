-- Profiling corpus for chdb-wasm-LITE: the single-file bundle sized for
-- Cloudflare Workers (10 MiB gzipped Worker limit). The lite bundle ships ONLY
-- the split primary — there is no deferred module to lazy-load — so whatever
-- this corpus does not cover is simply unavailable (a clear "not in lite"
-- error, see patch-glue.mjs --lite). That makes the selection principle the
-- opposite of the full corpus: not "everything a user might run" but "the
-- most common things users run", packed under the size budget.
--
-- In: core SQL operators (filters, aggregation, joins, windows, CTEs), the
--     everyday scalar/aggregate functions, file() over MEMFS in the common
--     formats, remote reads via url() and s3() (single-object path-style —
--     the lite bundle links with WASM_JSPI, whose async fetch() bridge is the
--     one HTTP transport that works inside Workers), default-database DDL on
--     Memory tables, error reporting.
-- Out: data-lake catalogs/Iceberg/Delta, exotic function families, MergeTree
--     internals (cannot run on the single-threaded build this bundle is
--     based on).
--
-- Runner rules are identical to profile-corpus.sql (see run-profile.mjs);
-- pass this file via CHDB_CORPUS. Statements with {HTTP} run against the
-- fixture static server; the /*mt-only*/ marker never applies (lite is st).

-- ============================================================================
-- 1. Types, literals, casts
-- ============================================================================
SELECT 1, -2, 3.5, 'hello', NULL, [1, 2], (1, 'a'), map('k', 1);
SELECT toInt8(1), toInt16(2), toInt32(3), toInt64(4), toUInt32(5), toUInt64(6), toFloat32(1.5), toFloat64(2.5);
SELECT toDecimal32(1.2345, 3), toDecimal64(1.23456, 5), toString(123), toFixedString('ab', 4);
SELECT toInt32OrZero('bad'), toInt32OrNull('bad'), toFloat64OrZero('x'), toNullable(5), assumeNotNull(toNullable(6));
SELECT CAST(42 AS String), CAST('2024-01-02' AS Date), 42::Float64, '[1,2,3]'::Array(Int32), accurateCastOrNull(300, 'Int8');
SELECT toDate('2024-03-15'), toDateTime('2024-03-15 12:34:56'), toDateTime64('2024-03-15 12:34:56.789', 3);
SELECT toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0'), generateUUIDv4() != generateUUIDv4(), toIPv4('1.2.3.4'), toBool(1);
SELECT CAST('a' AS Enum('a' = 1, 'b' = 2)), toLowCardinality('tag'), toTypeName([1, 2]), toIntervalDay(2);

-- ============================================================================
-- 2. Operators
-- ============================================================================
SELECT 7 + 3, 7 - 3, 7 * 3, 7 / 3, 7 % 3, intDiv(7, 3), -abs(-5);
SELECT 1 < 2, 2 <= 2, 3 > 2, 1 = 1, 1 != 2, 1 AND 0, 1 OR 0, NOT 1;
SELECT 5 BETWEEN 1 AND 10, 3 IN (1, 2, 3), 4 NOT IN (1, 2, 3);
SELECT number IN (SELECT number FROM numbers(5) WHERE number % 2 = 0) FROM numbers(6);
SELECT 'foobar' LIKE 'foo%', 'foobar' ILIKE 'FOO%', match('foobar', '^fo+bar$');
SELECT NULL IS NULL, 1 IS NOT NULL, ifNull(NULL, 'fallback'), coalesce(NULL, NULL, 3), nullIf(5, 5);
SELECT 'a' || 'b', [1, 2][1], map('x', 10)['x'], (1, 'two').2;
SELECT if(1 = 1, 'yes', 'no'), multiIf(0, 'a', 1, 'b', 'c'), CASE WHEN 2 > 1 THEN 'gt' ELSE 'le' END;
SELECT greatest(1, 2, 3), least(1, 2, 3), bitAnd(12, 10), bitOr(12, 10), bitXor(12, 10);

-- ============================================================================
-- 3. Core clauses
-- ============================================================================
SELECT count() FROM numbers(10000) WHERE number % 7 = 3;
SELECT number % 5 AS k, count(), sum(number), avg(number), min(number), max(number) FROM numbers(10000) GROUP BY k ORDER BY k;
SELECT number % 5 AS k, count() AS c FROM numbers(10000) GROUP BY k HAVING c > 1500 ORDER BY c DESC, k;
SELECT number % 3 AS a, count() FROM numbers(1000) GROUP BY a WITH TOTALS ORDER BY a;
SELECT number % 3 AS a, number % 4 AS b, count() FROM numbers(1000) GROUP BY a, b WITH ROLLUP ORDER BY a, b;
SELECT number % 2 AS a, number % 3 AS b, count() FROM numbers(1000) GROUP BY a, b WITH CUBE ORDER BY a, b;
SELECT DISTINCT number % 4 FROM numbers(100) ORDER BY 1;
SELECT number FROM numbers(100) ORDER BY number DESC LIMIT 5 OFFSET 10;
SELECT number % 3 AS g, number FROM numbers(30) ORDER BY g, number LIMIT 2 BY g;
SELECT toNullable(if(number % 3 = 0, NULL, number)) AS v FROM numbers(9) ORDER BY v ASC NULLS FIRST;
SELECT intDiv(number, 100) AS bucket, count() FROM numbers(100000) GROUP BY bucket ORDER BY bucket LIMIT 10;
-- Aggregation hash tables, join maps and sorters specialize PER KEY TYPE
-- (String keys, fixed-width keys by size, nullable, multi-key): cover the
-- common key types or `GROUP BY name` / time-bucket dashboards go cold.
-- NB: keys must be MATERIALIZED via a subquery — the optimizer rewrites
-- GROUP BY toString(x) to GROUP BY x (injective-function elimination), so a
-- direct computed key never exercises the String hash method at all.
SELECT s, count(), sum(n) FROM (SELECT toString(number % 5) AS s, number AS n FROM numbers(1000)) GROUP BY s ORDER BY s;
SELECT s, count() FROM (SELECT toString(number % 3) AS s FROM numbers(100)) GROUP BY s ORDER BY s;
SELECT toStartOfInterval(toDateTime('2024-03-15 12:00:00') + number, INTERVAL 15 MINUTE) AS bucket, count() FROM numbers(3600) GROUP BY bucket ORDER BY bucket;
SELECT toDate('2024-01-01') + number % 7 AS d, count(), sum(number) FROM numbers(1000) GROUP BY d ORDER BY d;
SELECT toUInt32(number % 7) AS k, count() FROM numbers(1000) GROUP BY k ORDER BY k;
SELECT toInt32(number % 7) - 3 AS k, count() FROM numbers(1000) GROUP BY k ORDER BY k;
SELECT toInt16(number % 5) AS k, count() FROM numbers(100) GROUP BY k ORDER BY k;
SELECT round(number / 7) AS f, count() FROM numbers(100) GROUP BY f ORDER BY f;
SELECT s, n, count() FROM (SELECT toString(number % 3) AS s, number % 2 AS n FROM numbers(1000)) GROUP BY s, n ORDER BY s, n;
SELECT s1, s2, count() FROM (SELECT toString(number % 3) AS s1, toString(number % 2) AS s2 FROM numbers(100)) GROUP BY s1, s2 ORDER BY s1, s2;
-- multi-key aggregation packs fixed-width keys by TOTAL byte width
-- (keys16/32/64/128): cover the common width combinations
SELECT k, j, count() FROM (SELECT toInt16(number % 5) AS k, toUInt8(number % 3) AS j FROM numbers(1000)) GROUP BY k, j ORDER BY k, j;
SELECT k, j, count() FROM (SELECT toUInt32(number % 5) AS k, toUInt16(number % 3) AS j FROM numbers(1000)) GROUP BY k, j ORDER BY k, j;
SELECT k, j, count() FROM (SELECT number % 5 AS k, toUInt32(number % 3) AS j FROM numbers(1000)) GROUP BY k, j ORDER BY k, j;
SELECT d, s, count() FROM (SELECT toDate('2024-01-01') + number % 5 AS d, toString(number % 3) AS s FROM numbers(1000)) GROUP BY d, s ORDER BY d, s;
SELECT d, j, count() FROM (SELECT toDate('2024-01-01') + number % 5 AS d, toUInt8(number % 3) AS j FROM numbers(1000)) GROUP BY d, j ORDER BY d, j;
SELECT toFixedString(toString(number % 3), 4) AS fs, count() FROM numbers(100) GROUP BY fs ORDER BY fs;
SELECT toNullable(number % 4) AS k, count() FROM numbers(100) GROUP BY k ORDER BY k;
SELECT toDecimal64(number % 5, 2) AS d, count() FROM numbers(100) GROUP BY d ORDER BY d;
SELECT toDateTime('2024-03-15 12:00:00') + number AS t FROM numbers(100) ORDER BY t DESC LIMIT 5;
SELECT toDate('2024-01-01') + number % 10 AS d FROM numbers(100) ORDER BY d LIMIT 3;
SELECT toString(number % 10) AS s, number / 3 AS f FROM numbers(100) ORDER BY s, f DESC LIMIT 5;
SELECT DISTINCT toString(number % 5) FROM numbers(100) ORDER BY 1;

-- ============================================================================
-- 4. Subqueries, CTEs, set operations, JOINs
-- ============================================================================
SELECT (SELECT max(number) FROM numbers(100)) + 1;
WITH evens AS (SELECT number FROM numbers(20) WHERE number % 2 = 0) SELECT count(), sum(number) FROM evens;
SELECT number FROM numbers(3) UNION ALL SELECT number + 10 FROM numbers(3) ORDER BY number;
SELECT number FROM numbers(10) INTERSECT SELECT number FROM numbers(5, 10) ORDER BY number;
SELECT number FROM numbers(10) EXCEPT SELECT number FROM numbers(5, 10) ORDER BY number;
SELECT a.number, b.number FROM numbers(5) AS a INNER JOIN (SELECT number FROM numbers(3, 5)) AS b ON a.number = b.number ORDER BY a.number;
SELECT a.number, b.n FROM numbers(5) AS a LEFT JOIN (SELECT number, number * 10 AS n FROM numbers(3)) AS b ON a.number = b.number ORDER BY a.number;
SELECT a.number, b.number FROM (SELECT number FROM numbers(4)) AS a FULL OUTER JOIN (SELECT number + 2 AS number FROM numbers(4)) AS b ON a.number = b.number ORDER BY a.number, b.number;
SELECT count() FROM numbers(10) AS a CROSS JOIN numbers(10) AS b;
SELECT x.number FROM numbers(4) AS x JOIN numbers(4) AS y USING (number) ORDER BY x.number;
SELECT count() FROM numbers(1000) AS a JOIN numbers(1000) AS b ON a.number = b.number;
-- join maps per key type: String / UInt32 / Date keys + a String LEFT JOIN
SELECT count() FROM (SELECT toString(number % 10) AS k FROM numbers(1000)) AS a JOIN (SELECT toString(number) AS k FROM numbers(10)) AS b ON a.k = b.k;
SELECT count() FROM (SELECT toString(number % 10) AS k FROM numbers(100)) AS a LEFT JOIN (SELECT toString(number) AS k, number AS v FROM numbers(5)) AS b ON a.k = b.k;
SELECT count() FROM (SELECT toUInt32(number) AS k FROM numbers(100)) AS a JOIN (SELECT toUInt32(number * 2) AS k FROM numbers(50)) AS b ON a.k = b.k;
SELECT count() FROM (SELECT toDate('2024-01-01') + number % 10 AS d FROM numbers(100)) AS a JOIN (SELECT toDate('2024-01-01') + number AS d FROM numbers(10)) AS b ON a.d = b.d;
SELECT count() FROM (SELECT toString(number % 10) AS s FROM numbers(100)) WHERE s IN (SELECT toString(number) FROM numbers(5));
-- arithmetic over join output and windowed/limited shapes: expression
-- specializations differ from the bare-column forms above, keep both hot
SELECT a.number * 10 + b.number FROM numbers(2) a JOIN numbers(2) b ON a.number = b.number ORDER BY 1;
SELECT number, row_number() OVER (ORDER BY number DESC) FROM numbers(3) ORDER BY number LIMIT 1;
SELECT quantile(0.5)(number), uniqExact(number % 10), topK(1)(number % 3) FROM numbers(1000);

-- ============================================================================
-- 5. Window functions
-- ============================================================================
SELECT number, row_number() OVER (ORDER BY number DESC) FROM numbers(10) ORDER BY number;
SELECT number % 3 AS g, number, rank() OVER (PARTITION BY number % 3 ORDER BY number) FROM numbers(12) ORDER BY g, number;
SELECT number, sum(number) OVER (ORDER BY number ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM numbers(10);
SELECT number, avg(number) OVER (ORDER BY number ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING) FROM numbers(10);
SELECT number, lagInFrame(number, 1) OVER w, leadInFrame(number, 1) OVER w FROM numbers(8) WINDOW w AS (ORDER BY number ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING);
SELECT number, dense_rank() OVER (ORDER BY number % 3), first_value(number) OVER w, last_value(number) OVER w FROM numbers(6) WINDOW w AS (ORDER BY number ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW);
SELECT s, sum(f) OVER (PARTITION BY s ORDER BY f) FROM (SELECT toString(number % 3) AS s, number / 2 AS f FROM numbers(12)) ORDER BY s, f;

-- ============================================================================
-- 6. Everyday aggregates
-- ============================================================================
SELECT count(), count(DISTINCT number % 10), countIf(number % 2 = 0) FROM numbers(1000);
SELECT sum(number), sumIf(number, number % 2 = 1), avg(number), avgIf(number, number > 500) FROM numbers(1000);
SELECT min(number), max(number), any(number), anyLast(number), argMin(number, number % 7), argMax(number, number % 7) FROM numbers(1000);
SELECT uniq(number % 100), uniqExact(number % 100), uniqCombined(number % 100) FROM numbers(10000);
SELECT quantile(0.5)(number), quantiles(0.25, 0.5, 0.75)(number), quantileExact(0.9)(number), median(number) FROM numbers(10000);
SELECT topK(3)(number % 10), groupArray(number), groupUniqArray(number % 3) FROM numbers(10);
SELECT corr(toFloat64(number), number * 2.0), stddevPop(number), varSamp(number) FROM numbers(1000);
SELECT countMerge(s) FROM (SELECT countState() AS s FROM numbers(100));
-- Analyzer REWRITE passes execute their rewrite bodies only when the pattern
-- matches; these are everyday shapes (sum(x*c) -> sum(x)*c and friends —
-- AggregateFunctionsArithmeticOperationsPass helpers were cold without them).
SELECT sum(number * 10), sum(2 * number), max(number * 600), min(number * 3), avg(number * 100) FROM numbers(1000);
SELECT sum(number / 4), min(number + 7), max(number - 2), sum(number * 1.5) FROM numbers(1000);
SELECT max(toInt64(number) * -2), min(toInt64(number) * -2) FROM numbers(100);
SELECT count(*), count(1) FROM numbers(100);
SELECT sum(if(number % 2 = 0, 1, 0)), sum(if(number % 2 = 0, number, 0)) FROM numbers(100);
SELECT arrayExists(x -> x = 2, [1, 2, 3]);
SELECT sumArray([number, number + 1]) FROM numbers(100);
-- aggregates specialize per input type: cover the common non-UInt64 shapes
-- (String/Date/Nullable/Float/Int32) or their instantiations stay cold
SELECT min(s), max(s), any(s), anyLast(s), argMax(s, n), uniqExact(s) FROM (SELECT toString(number % 5) AS s, number AS n FROM numbers(100));
SELECT min(d), max(d), count(DISTINCT d) FROM (SELECT toDate('2024-01-01') + number AS d FROM numbers(10));
SELECT min(v), max(v), sum(v), avg(v) FROM (SELECT toNullable(number) AS v FROM numbers(100));
SELECT min(f), max(f), sum(f), avg(f), quantile(0.5)(f) FROM (SELECT number / 3 AS f FROM numbers(100));
SELECT min(i), max(i), sum(i), avg(i) FROM (SELECT toInt32(number) AS i FROM numbers(100));
SELECT sumIf(i, i % 2 = 0), minIf(s, n > 5), maxIf(s, n < 90), anyIf(s, n = 7) FROM (SELECT toInt32(number) AS i, toString(number) AS s, number AS n FROM numbers(100));
SELECT min(a), max(a), sum(a), min(b), max(b), sum(b), min(c), max(c) FROM (SELECT toInt64(number) - 50 AS a, toUInt32(number) AS b, toUInt8(number % 250) AS c FROM numbers(100));
SELECT sum(d), min(d), max(d), count(d) FROM (SELECT toDecimal64(number, 2) AS d FROM numbers(100));
SELECT min(t), max(t), count(DISTINCT t) FROM (SELECT toDateTime64('2024-03-15 12:00:00.000', 3) + number AS t FROM numbers(100));
SELECT lc, count() FROM (SELECT toLowCardinality(toString(number % 3)) AS lc FROM numbers(1000)) GROUP BY lc ORDER BY lc;
SELECT stddevSamp(number), covarPop(toFloat64(number), toFloat64(number * 2)), sumMap(map(number % 3, 1)) FROM numbers(100);
-- event/funnel analytics over timestamped conditions (log processing shape)
SELECT windowFunnel(100)(toDateTime(number), number % 5 = 0, number % 5 = 1) FROM numbers(100);

-- ============================================================================
-- 6b. TYPE MATRIX (generated systematically; see the key-type note in §3).
--     Aggregates, GROUP BY keys, JOIN keys, IN-sets and DISTINCT all
--     specialize per column type: cross the common operations with the
--     common types so swapping a column type never goes lite-cold.
-- ============================================================================
-- universal aggregates x each type:
SELECT min(v), max(v), any(v), anyLast(v), uniq(v), uniqExact(v), topK(2)(v), argMax(v, n), argMin(v, n), length(groupArray(v)) FROM (SELECT toInt8(number % 100 - 50) AS v, number AS n FROM numbers(300));
SELECT min(v), max(v), any(v), anyLast(v), uniq(v), uniqExact(v), topK(2)(v), argMax(v, n), argMin(v, n), length(groupArray(v)) FROM (SELECT toInt16(number % 1000 - 500) AS v, number AS n FROM numbers(300));
SELECT min(v), max(v), any(v), anyLast(v), uniq(v), uniqExact(v), topK(2)(v), argMax(v, n), argMin(v, n), length(groupArray(v)) FROM (SELECT toInt32(number % 1000) AS v, number AS n FROM numbers(300));
SELECT min(v), max(v), any(v), anyLast(v), uniq(v), uniqExact(v), topK(2)(v), argMax(v, n), argMin(v, n), length(groupArray(v)) FROM (SELECT toInt64(number) - 500 AS v, number AS n FROM numbers(300));
SELECT min(v), max(v), any(v), anyLast(v), uniq(v), uniqExact(v), topK(2)(v), argMax(v, n), argMin(v, n), length(groupArray(v)) FROM (SELECT toUInt16(number % 60000) AS v, number AS n FROM numbers(300));
SELECT min(v), max(v), any(v), anyLast(v), uniq(v), uniqExact(v), topK(2)(v), argMax(v, n), argMin(v, n), length(groupArray(v)) FROM (SELECT toFloat32(number / 7) AS v, number AS n FROM numbers(300));
SELECT min(v), max(v), any(v), anyLast(v), uniq(v), uniqExact(v), topK(2)(v), argMax(v, n), argMin(v, n), length(groupArray(v)) FROM (SELECT number / 7 AS v, number AS n FROM numbers(300));
SELECT min(v), max(v), any(v), anyLast(v), uniq(v), uniqExact(v), topK(2)(v), argMax(v, n), argMin(v, n), length(groupArray(v)) FROM (SELECT concat('v', toString(number % 50)) AS v, number AS n FROM numbers(300));
SELECT min(v), max(v), any(v), anyLast(v), uniq(v), uniqExact(v), topK(2)(v), argMax(v, n), argMin(v, n), length(groupArray(v)) FROM (SELECT toFixedString(toString(number % 10), 4) AS v, number AS n FROM numbers(300));
SELECT min(v), max(v), any(v), anyLast(v), uniq(v), uniqExact(v), topK(2)(v), argMax(v, n), argMin(v, n), length(groupArray(v)) FROM (SELECT toDate('2024-01-01') + number % 300 AS v, number AS n FROM numbers(300));
SELECT min(v), max(v), any(v), anyLast(v), uniq(v), uniqExact(v), topK(2)(v), argMax(v, n), argMin(v, n), length(groupArray(v)) FROM (SELECT toDateTime('2024-01-01 00:00:00') + number * 7 AS v, number AS n FROM numbers(300));
SELECT min(v), max(v), any(v), anyLast(v), uniq(v), uniqExact(v), topK(2)(v), argMax(v, n), argMin(v, n), length(groupArray(v)) FROM (SELECT toDateTime64('2024-01-01 00:00:00.000', 3) + number AS v, number AS n FROM numbers(300));
SELECT min(v), max(v), any(v), anyLast(v), uniq(v), uniqExact(v), topK(2)(v), argMax(v, n), argMin(v, n), length(groupArray(v)) FROM (SELECT toDecimal64(number % 100, 2) AS v, number AS n FROM numbers(300));
SELECT min(v), max(v), any(v), anyLast(v), uniq(v), uniqExact(v), topK(2)(v), argMax(v, n), argMin(v, n), length(groupArray(v)) FROM (SELECT toUUID(concat('61f0c404-5cb3-11e7-907b-a6006ad3dba', toString(number % 10))) AS v, number AS n FROM numbers(300));
SELECT min(v), max(v), any(v), anyLast(v), uniq(v), uniqExact(v), topK(2)(v), argMax(v, n), argMin(v, n), length(groupArray(v)) FROM (SELECT toIPv4(concat('10.0.', toString(intDiv(number, 250) % 250), '.', toString(number % 250))) AS v, number AS n FROM numbers(300));
SELECT min(v), max(v), any(v), anyLast(v), uniq(v), uniqExact(v), topK(2)(v), argMax(v, n), argMin(v, n), length(groupArray(v)) FROM (SELECT CAST(if(number % 2 = 0, 'a', 'b') AS Enum('a' = 1, 'b' = 2)) AS v, number AS n FROM numbers(300));
SELECT min(v), max(v), any(v), anyLast(v), uniq(v), uniqExact(v), topK(2)(v), argMax(v, n), argMin(v, n), length(groupArray(v)) FROM (SELECT if(number % 7 = 0, NULL, number / 3) AS v, number AS n FROM numbers(300));
SELECT min(v), max(v), any(v), anyLast(v), uniq(v), uniqExact(v), topK(2)(v), argMax(v, n), argMin(v, n), length(groupArray(v)) FROM (SELECT if(number % 7 = 0, NULL, toString(number % 10)) AS v, number AS n FROM numbers(300));
SELECT min(v), max(v), any(v), anyLast(v), uniq(v), uniqExact(v), topK(2)(v), argMax(v, n), argMin(v, n), length(groupArray(v)) FROM (SELECT if(number % 7 = 0, NULL, toInt64(number)) AS v, number AS n FROM numbers(300));
SELECT min(v), max(v), any(v), anyLast(v), uniq(v), uniqExact(v), topK(2)(v), argMax(v, n), argMin(v, n), length(groupArray(v)) FROM (SELECT if(number % 7 = 0, NULL, toDateTime('2024-01-01 00:00:00') + number) AS v, number AS n FROM numbers(300));
SELECT min(v), max(v), any(v), anyLast(v), uniq(v), uniqExact(v), topK(2)(v), argMax(v, n), argMin(v, n), length(groupArray(v)) FROM (SELECT toLowCardinality(toString(number % 10)) AS v, number AS n FROM numbers(300));
SELECT argMax(v, d), argMin(v, d) FROM (SELECT toString(number % 5) AS v, toDate('2024-01-01') + number AS d FROM numbers(100));
SELECT argMax(n, t), argMin(n, t) FROM (SELECT number AS n, toDateTime('2024-01-01 00:00:00') + number AS t FROM numbers(100));
-- numeric aggregates x each numeric type (Decimal: no avg/quantile):
SELECT sum(v), avg(v), quantile(0.5)(v), median(v) FROM (SELECT toInt8(number % 100 - 50) AS v FROM numbers(300));
SELECT sum(v), avg(v), quantile(0.5)(v), median(v) FROM (SELECT toInt16(number % 1000 - 500) AS v FROM numbers(300));
SELECT sum(v), avg(v), quantile(0.5)(v), median(v) FROM (SELECT toInt64(number) - 500 AS v FROM numbers(300));
SELECT sum(v), avg(v), quantile(0.5)(v), median(v) FROM (SELECT toInt32(number % 1000) AS v FROM numbers(300));
SELECT sum(v), avg(v), quantile(0.5)(v), median(v) FROM (SELECT toUInt32(number) AS v FROM numbers(300));
SELECT sum(v), avg(v), quantile(0.5)(v), median(v) FROM (SELECT toUInt16(number % 60000) AS v FROM numbers(300));
SELECT sum(v), avg(v), quantile(0.5)(v), median(v) FROM (SELECT toFloat32(number / 7) AS v FROM numbers(300));
SELECT sum(v), avg(v), quantile(0.5)(v), median(v) FROM (SELECT number / 7 AS v FROM numbers(300));
SELECT sum(v), avg(v), quantile(0.5)(v), median(v) FROM (SELECT if(number % 7 = 0, NULL, number / 3) AS v FROM numbers(300));
SELECT sum(v), avg(v), quantile(0.5)(v), median(v) FROM (SELECT if(number % 7 = 0, NULL, toInt64(number)) AS v FROM numbers(300));
SELECT sum(v), min(v), max(v) FROM (SELECT toDecimal64(number % 100, 2) AS v FROM numbers(300));
-- GROUP BY keys not already covered in section 3. Plain ORDER BY on purpose:
-- with a LIMIT the sort takes the partial-sort path and the full pdqsort
-- permutation (also instantiated per column type) would stay cold.
--
SELECT v, count() FROM (SELECT toInt8(number % 100 - 50) AS v FROM numbers(300)) GROUP BY v ORDER BY v;
SELECT v, count() FROM (SELECT toInt16(number % 1000 - 500) AS v FROM numbers(300)) GROUP BY v ORDER BY v;
SELECT v, count() FROM (SELECT toInt64(number) - 500 AS v FROM numbers(300)) GROUP BY v ORDER BY v;
SELECT v, count() FROM (SELECT toUInt16(number % 60000) AS v FROM numbers(300)) GROUP BY v ORDER BY v;
SELECT v, count() FROM (SELECT toFloat32(number / 7) AS v FROM numbers(300)) GROUP BY v ORDER BY v;
SELECT v, count() FROM (SELECT toDateTime64('2024-01-01 00:00:00.000', 3) + number AS v FROM numbers(300)) GROUP BY v ORDER BY v;
SELECT v, count() FROM (SELECT toUUID(concat('61f0c404-5cb3-11e7-907b-a6006ad3dba', toString(number % 10))) AS v FROM numbers(300)) GROUP BY v ORDER BY v;
SELECT v, count() FROM (SELECT toIPv4(concat('10.0.', toString(intDiv(number, 250) % 250), '.', toString(number % 250))) AS v FROM numbers(300)) GROUP BY v ORDER BY v;
SELECT v, count() FROM (SELECT CAST(if(number % 2 = 0, 'a', 'b') AS Enum('a' = 1, 'b' = 2)) AS v FROM numbers(300)) GROUP BY v ORDER BY v;
SELECT v, count() FROM (SELECT if(number % 7 = 0, NULL, number / 3) AS v FROM numbers(300)) GROUP BY v ORDER BY v;
SELECT v, count() FROM (SELECT if(number % 7 = 0, NULL, toString(number % 10)) AS v FROM numbers(300)) GROUP BY v ORDER BY v;
SELECT v, count() FROM (SELECT if(number % 7 = 0, NULL, toDateTime('2024-01-01 00:00:00') + number) AS v FROM numbers(300)) GROUP BY v ORDER BY v;
-- low-row-count variants: sorting the grouped output takes the pdqsort
-- path below ~256 rows and radix sort above — different per-type code.
SELECT v, count() FROM (SELECT toInt8(number % 100 - 50) AS v FROM numbers(300) WHERE number < 30) GROUP BY v ORDER BY v;
SELECT v, count() FROM (SELECT toInt16(number % 1000 - 500) AS v FROM numbers(300) WHERE number < 30) GROUP BY v ORDER BY v;
SELECT v, count() FROM (SELECT toInt64(number) - 500 AS v FROM numbers(300) WHERE number < 30) GROUP BY v ORDER BY v;
SELECT v, count() FROM (SELECT toUInt16(number % 60000) AS v FROM numbers(300) WHERE number < 30) GROUP BY v ORDER BY v;
SELECT v, count() FROM (SELECT toFloat32(number / 7) AS v FROM numbers(300) WHERE number < 30) GROUP BY v ORDER BY v;
SELECT v, count() FROM (SELECT toDateTime64('2024-01-01 00:00:00.000', 3) + number AS v FROM numbers(300) WHERE number < 30) GROUP BY v ORDER BY v;
SELECT v, count() FROM (SELECT toUUID(concat('61f0c404-5cb3-11e7-907b-a6006ad3dba', toString(number % 10))) AS v FROM numbers(300) WHERE number < 30) GROUP BY v ORDER BY v;
SELECT v, count() FROM (SELECT toIPv4(concat('10.0.', toString(intDiv(number, 250) % 250), '.', toString(number % 250))) AS v FROM numbers(300) WHERE number < 30) GROUP BY v ORDER BY v;
SELECT v, count() FROM (SELECT CAST(if(number % 2 = 0, 'a', 'b') AS Enum('a' = 1, 'b' = 2)) AS v FROM numbers(300) WHERE number < 30) GROUP BY v ORDER BY v;
SELECT v, count() FROM (SELECT if(number % 7 = 0, NULL, number / 3) AS v FROM numbers(300) WHERE number < 30) GROUP BY v ORDER BY v;
SELECT v, count() FROM (SELECT if(number % 7 = 0, NULL, toString(number % 10)) AS v FROM numbers(300) WHERE number < 30) GROUP BY v ORDER BY v;
SELECT v, count() FROM (SELECT if(number % 7 = 0, NULL, toDateTime('2024-01-01 00:00:00') + number) AS v FROM numbers(300) WHERE number < 30) GROUP BY v ORDER BY v;
-- DESC sorts: an ascending sort of already-ascending data yields an identity
-- permutation and IColumn::permute is skipped — descending guarantees the
-- per-type permute instantiation actually runs.
SELECT fs FROM (SELECT toFixedString(toString(number % 30), 4) AS fs FROM numbers(30)) ORDER BY fs DESC;
SELECT fs FROM (SELECT toFixedString(toString(number), 8) AS fs FROM numbers(300)) ORDER BY fs DESC LIMIT 5;
SELECT v FROM (SELECT toDateTime64('2024-01-01 00:00:00.000', 3) + number AS v FROM numbers(30)) ORDER BY v DESC;
SELECT v FROM (SELECT toFloat32(number) AS v FROM numbers(30)) ORDER BY v DESC;
SELECT v FROM (SELECT toUUID(concat('61f0c404-5cb3-11e7-907b-a6006ad3dba', toString(number % 10))) AS v FROM numbers(30)) ORDER BY v DESC;
SELECT v FROM (SELECT toIPv4(concat('10.0.0.', toString(number % 30))) AS v FROM numbers(30)) ORDER BY v DESC;

-- Adaptive method variants: the aggregator picks Cache-vs-NoCache AND
-- single-vs-TWO-LEVEL hash methods from process-history statistics and
-- cardinality, not from the query alone. 150k distinct keys crosses the
-- two-level threshold mid-query; the repeats let the statistics choose the
-- other variants on later runs — all combinations must profile hot.
SELECT count() FROM (SELECT fs, count() FROM (SELECT toFixedString(toString(number), 8) AS fs FROM numbers(150000)) GROUP BY fs);
SELECT count() FROM (SELECT fs, count() FROM (SELECT toFixedString(toString(number), 8) AS fs FROM numbers(150000)) GROUP BY fs);
SELECT count() FROM (SELECT fs, count() FROM (SELECT toFixedString(toString(number), 8) AS fs FROM numbers(150000)) GROUP BY fs);
SELECT count() FROM (SELECT s, count() FROM (SELECT toString(number) AS s FROM numbers(150000)) GROUP BY s);
SELECT count() FROM (SELECT s, count() FROM (SELECT toString(number) AS s FROM numbers(150000)) GROUP BY s);
SELECT count() FROM (SELECT s, count() FROM (SELECT toString(number) AS s FROM numbers(150000)) GROUP BY s);
SELECT count() FROM (SELECT k, count() FROM (SELECT toUInt32(number) AS k FROM numbers(150000)) GROUP BY k);
SELECT count() FROM (SELECT k, count() FROM (SELECT toUInt32(number) AS k FROM numbers(150000)) GROUP BY k);
SELECT count() FROM (SELECT k, count() FROM (SELECT toUInt32(number) AS k FROM numbers(150000)) GROUP BY k);
SELECT count() FROM (SELECT s, n, count() FROM (SELECT toString(number) AS s, number AS n FROM numbers(150000)) GROUP BY s, n);
SELECT count() FROM (SELECT s, n, count() FROM (SELECT toString(number) AS s, number AS n FROM numbers(150000)) GROUP BY s, n);
SELECT count() FROM (SELECT s, n, count() FROM (SELECT toString(number) AS s, number AS n FROM numbers(150000)) GROUP BY s, n);

-- JOIN keys not already covered in section 4:
SELECT count() FROM (SELECT toInt32(number % 1000) AS k FROM numbers(300)) AS a JOIN (SELECT toInt32(number % 1000) AS k FROM numbers(50)) AS b ON a.k = b.k;
SELECT count() FROM (SELECT toInt64(number) - 500 AS k FROM numbers(300)) AS a JOIN (SELECT toInt64(number) - 500 AS k FROM numbers(50)) AS b ON a.k = b.k;
SELECT count() FROM (SELECT toDateTime('2024-01-01 00:00:00') + number * 7 AS k FROM numbers(300)) AS a JOIN (SELECT toDateTime('2024-01-01 00:00:00') + number * 7 AS k FROM numbers(50)) AS b ON a.k = b.k;
SELECT count() FROM (SELECT toUUID(concat('61f0c404-5cb3-11e7-907b-a6006ad3dba', toString(number % 10))) AS k FROM numbers(300)) AS a JOIN (SELECT toUUID(concat('61f0c404-5cb3-11e7-907b-a6006ad3dba', toString(number % 10))) AS k FROM numbers(50)) AS b ON a.k = b.k;
SELECT count() FROM (SELECT toIPv4(concat('10.0.', toString(intDiv(number, 250) % 250), '.', toString(number % 250))) AS k FROM numbers(300)) AS a JOIN (SELECT toIPv4(concat('10.0.', toString(intDiv(number, 250) % 250), '.', toString(number % 250))) AS k FROM numbers(50)) AS b ON a.k = b.k;
SELECT count() FROM (SELECT CAST(if(number % 2 = 0, 'a', 'b') AS Enum('a' = 1, 'b' = 2)) AS k FROM numbers(300)) AS a JOIN (SELECT CAST(if(number % 2 = 0, 'a', 'b') AS Enum('a' = 1, 'b' = 2)) AS k FROM numbers(50)) AS b ON a.k = b.k;
SELECT count() FROM (SELECT if(number % 7 = 0, NULL, toString(number % 10)) AS k FROM numbers(300)) AS a JOIN (SELECT if(number % 7 = 0, NULL, toString(number % 10)) AS k FROM numbers(50)) AS b ON a.k = b.k;
SELECT count() FROM (SELECT concat('v', toString(number % 50)) AS s, number % 2 AS n FROM numbers(100)) AS a JOIN (SELECT concat('v', toString(number % 50)) AS s, number % 2 AS n FROM numbers(20)) AS b ON a.s = b.s AND a.n = b.n;
-- IN-set membership and DISTINCT specialize per type too:
SELECT count() FROM (SELECT toDate('2024-01-01') + number % 300 AS v FROM numbers(300)) WHERE v IN (SELECT toDate('2024-01-01') + number % 300 FROM numbers(20));
SELECT count() FROM (SELECT toDateTime('2024-01-01 00:00:00') + number * 7 AS v FROM numbers(300)) WHERE v IN (SELECT toDateTime('2024-01-01 00:00:00') + number * 7 FROM numbers(20));
SELECT count() FROM (SELECT toInt32(number % 1000) AS v FROM numbers(300)) WHERE v IN (SELECT toInt32(number % 1000) FROM numbers(20));
SELECT count() FROM (SELECT toUUID(concat('61f0c404-5cb3-11e7-907b-a6006ad3dba', toString(number % 10))) AS v FROM numbers(300)) WHERE v IN (SELECT toUUID(concat('61f0c404-5cb3-11e7-907b-a6006ad3dba', toString(number % 10))) FROM numbers(20));
SELECT count() FROM (SELECT DISTINCT toDate('2024-01-01') + number % 300 AS v FROM numbers(300));
SELECT count() FROM (SELECT DISTINCT toDateTime('2024-01-01 00:00:00') + number * 7 AS v FROM numbers(300));
SELECT count() FROM (SELECT DISTINCT number / 7 AS v FROM numbers(300));
SELECT count() FROM (SELECT DISTINCT toInt32(number % 1000) AS v FROM numbers(300));

-- ============================================================================
-- 7. Everyday scalar functions
-- ============================================================================
SELECT length('hello'), lower('MiXeD'), upper('MiXeD'), reverse('abc'), repeat('ab', 3), char(72, 105);
SELECT substring('clickhouse', 1, 5), trim('  pad  '), leftPad('7', 4, '0'), concat('a', 'b', 'c'), format('{} scored {}', 'alice', 95);
SELECT startsWith('clickhouse', 'click'), endsWith('clickhouse', 'house'), position('clickhouse', 'house');
SELECT replaceOne('aaa', 'a', 'b'), replaceAll('aaa', 'a', 'b'), replaceRegexpAll('a1b2c3', '\d', '#');
SELECT splitByChar(',', 'a,b,c'), arrayStringConcat(['x', 'y', 'z'], '|'), extract('key=val', 'key=(\w+)'), extractAll('a1b22c333', '\d+');
SELECT splitByString(', ', 'a, b, c'), concatWithSeparator('-', 'x', 'y'), left('clickhouse', 5), right('clickhouse', 5), substringIndex('a.b.c', '.', 2);
SELECT positionCaseInsensitive('Hello World', 'world'), countSubstrings('abcabc', 'bc'), multiSearchAny('error: disk full', ['error', 'warn']), lengthUTF8('中文'), toValidUTF8('ok');
SELECT replaceRegexpOne('2024-03-15', '(\d+)-(\d+)-(\d+)', '\3/\2/\1'), countMatches('a1b2c3', '[0-9]'), empty(''), notEmpty([1]);
SELECT formatReadableSize(123456789), hex('AB'), unhex('4142'), isValidUTF8('ok');
SELECT now() > toDateTime('2020-01-01'), today() >= yesterday(), toYear(toDate('2024-03-15')), toMonth(toDate('2024-03-15')), toDayOfWeek(toDate('2024-03-15'));
SELECT toStartOfMonth(toDate('2024-03-15')), toStartOfDay(toDateTime('2024-03-15 12:34:56')), toMonday(toDate('2024-03-15')), toYYYYMM(toDate('2024-03-15'));
SELECT addDays(toDate('2024-03-15'), 4), subtractMonths(toDate('2024-03-15'), 1), dateDiff('day', toDate('2024-01-01'), toDate('2024-03-15'));
SELECT formatDateTime(toDateTime('2024-03-15 12:34:56'), '%Y/%m/%d %H:%M:%S'), parseDateTimeBestEffort('15 Mar 2024 12:34:56'), toUnixTimestamp(toDateTime('2024-03-15 12:34:56'));
SELECT toDate('2024-03-15') + INTERVAL 10 DAY, date_trunc('week', toDateTime('2024-03-15 12:34:56'));
SELECT toHour(t), toMinute(t), toSecond(t), toStartOfHour(t), toStartOfFifteenMinutes(t) FROM (SELECT toDateTime('2024-03-15 12:34:56') AS t);
-- time-bucketing, the dashboard workhorse
SELECT toStartOfInterval(toDateTime('2024-03-15 12:34:56'), INTERVAL 15 MINUTE), toStartOfWeek(toDate('2024-03-15')), age('day', toDate('2024-01-01'), toDate('2024-03-15'));
SELECT fromUnixTimestamp(1710500096), now64(3) >= toDateTime64('2020-01-01 00:00:00.000', 3), toTimeZone(toDateTime('2024-03-15 12:34:56', 'UTC'), 'America/New_York');
SELECT abs(-3), round(2.567, 2), floor(2.9), ceil(2.1), sqrt(2), pow(2, 10), exp(1), log(e()), sin(pi() / 2);
SELECT isFinite(1.0), isNaN(0 / 0.0), sign(-2.5), least(1.5, 2.5), pi();
SELECT range(5), arrayMap(x -> x * 2, [1, 2, 3]), arrayFilter(x -> x % 2 = 0, range(10)), arraySum([1, 2, 3]), arraySort([3, 1, 2]);
SELECT has([1, 2, 3], 2), indexOf([7, 8, 9], 9), arrayConcat([1, 2], [3]), arraySlice([1, 2, 3, 4, 5], 2, 3), arrayDistinct([1, 2, 2, 3]);
SELECT arrayExists(x -> x > 2, [1, 2, 3]), arrayAll(x -> x > 0, [1, 2, 3]), arrayCount(x -> x % 2 = 0, range(10)), arrayFirst(x -> x > 1, [1, 2, 3]), arrayFirstIndex(x -> x > 1, [1, 2, 3]);
SELECT arrayAvg([1, 2, 3]), arrayMin([3, 1, 2]), arrayMax([3, 1, 2]), arrayProduct([1.0, 2, 3]), arrayCompact([1, 1, 2, 2, 3]);
SELECT arrayReverse([1, 2, 3]), arrayFlatten([[1, 2], [3]]), arrayZip(['a', 'b'], [1, 2]), arrayIntersect([1, 2, 3], [2, 3, 4]);
SELECT arrayEnumerate([10, 20]), arrayPushBack([1, 2], 3), arrayPopFront([1, 2, 3]), arrayResize([1, 2], 4, 0);
SELECT arrayJoin([1, 2, 3]) AS x, x * 10 FROM system.one;
SELECT number, arr FROM numbers(3) ARRAY JOIN [10, 20] AS arr ORDER BY number, arr;
SELECT mapKeys(map('a', 1, 'b', 2)), mapValues(map('a', 1)), tuple(1, 'a').1;
SELECT mapContains(map('a', 1), 'a'), mapFromArrays(['k1', 'k2'], [1, 2]), tupleElement((1, 'a'), 2), untuple((1, 2));
SELECT JSONExtractInt('{"a": 42}', 'a'), JSONExtractString('{"a": "hi"}', 'a'), JSONExtract('{"a": [1, 2]}', 'a', 'Array(Int64)'), JSONHas('{"a": 1}', 'a'), isValidJSON('{"ok": 1}');
SELECT JSONExtractRaw('{"a": {"b": 1}}', 'a'), JSON_VALUE('{"a": {"b": "v"}}', '$.a.b'), toJSONString(map('a', [1, 2]));
SELECT JSONExtractFloat('{"a": 1.5}', 'a'), JSONExtractBool('{"a": true}', 'a'), JSONLength('{"a": 1, "b": 2}'), JSONType('{"a": [1]}', 'a'), JSONExtractArrayRaw('{"a": [1, 2]}', 'a');
SELECT cityHash64('chdb'), sipHash64('chdb'), xxHash64('chdb'), hex(MD5('chdb')), hex(SHA256('chdb'));
SELECT rand() >= 0, randCanonical() BETWEEN 0 AND 1, length(randomString(16));
SELECT domain('https://www.example.com/a/b?q=1'), path('https://example.com/a/b?q=1'), extractURLParameter('https://example.com/p?q=1&r=2', 'r'), encodeURLComponent('a b&c');
SELECT protocol('https://example.com/a'), topLevelDomain('https://example.com/a'), queryString('https://example.com/p?q=1&r=2'), cutQueryString('https://example.com/p?q=1'), decodeURLComponent('a%20b');
SELECT IPv4NumToString(toUInt32(3232235777)), IPv4StringToNum('192.168.1.1'), isIPAddressInRange('192.168.1.5', '192.168.1.0/24'), toIPv6('::1');
SELECT transform(2, [1, 2, 3], ['one', 'two', 'three'], 'other'), bar(5, 0, 10, 10), version() != '', currentDatabase();
-- the everyday scalar families applied to COLUMNS, not just constants —
-- per-row vector execution can be a different instantiation from const-fold
SELECT count() FROM (SELECT upper(concat('u', toString(number))) AS s FROM numbers(100)) WHERE s LIKE 'U1%';
SELECT max(lower(s)), min(upper(s)) FROM (SELECT concat('User', toString(number % 10)) AS s FROM numbers(100));
SELECT max(formatDateTime(toDateTime('2024-03-15 12:00:00') + number, '%Y-%m-%d %H:%M')) FROM numbers(100);
SELECT sum(toHour(t)), max(toStartOfHour(t)) FROM (SELECT toDateTime('2024-03-15 00:00:00') + number * 137 AS t FROM numbers(500));
SELECT count() FROM (SELECT replaceAll(toString(number), '1', 'x') AS s FROM numbers(100)) WHERE s != '';
SELECT sum(length(splitByChar(',', concat(toString(number), ',x')))) FROM numbers(100);
SELECT sum(JSONExtractInt(j, 'v')) FROM (SELECT concat('{"v": ', toString(number), '}') AS j FROM numbers(100));
SELECT max(domain(u)) FROM (SELECT concat('https://ex', toString(number % 3), '.com/p') AS u FROM numbers(100));

-- ============================================================================
-- 8. DDL/DML: default database (Workers users type unqualified names) + views
-- ============================================================================
CREATE TABLE lite_t (id UInt32, name String, score Float64, tags Array(String)) ENGINE = Memory;
INSERT INTO lite_t VALUES (1, 'alice', 9.5, ['a', 'b']), (2, 'bob', 7.25, ['b']), (3, 'carol', 8.0, []);
INSERT INTO lite_t SELECT number + 10, concat('user', toString(number)), number * 1.5, [toString(number % 3)] FROM numbers(50);
SELECT count(), avg(score), uniqExact(name) FROM lite_t;
SELECT name, score FROM lite_t WHERE has(tags, 'b') ORDER BY score DESC;
SHOW TABLES;
DESCRIBE lite_t;
EXISTS TABLE lite_t;
CREATE VIEW lite_v AS SELECT name, score FROM lite_t WHERE score > 5;
SELECT count() FROM lite_v;
CREATE TEMPORARY TABLE lite_tmp (a Int32);
INSERT INTO lite_tmp VALUES (1), (2);
SELECT sum(a) FROM lite_tmp;
TRUNCATE TABLE lite_t;
DROP TABLE lite_t;
DROP TABLE lite_v;
-- MergeTree cannot start on the single-threaded build; keep its (clean) error
-- path hot so lite users get a real message instead of a missing-function one.
CREATE TABLE lite_mt (x Int32) ENGINE = MergeTree ORDER BY x;

-- ============================================================================
-- 9. Table functions: numbers / values / format / view / generateRandom
-- ============================================================================
SELECT count() FROM numbers(1000);
SELECT count() FROM numbers(100, 900);
SELECT * FROM values('a Int32, b String', (1, 'x'), (2, 'y')) ORDER BY a;
SELECT * FROM format(JSONEachRow, '{"n": 1, "s": "one"}\n{"n": 2, "s": "two"}') ORDER BY n;
SELECT * FROM format(CSVWithNames, 'id,name\n1,alpha\n2,beta') ORDER BY id;
SELECT count() FROM view(SELECT number FROM numbers(10) WHERE number > 4);
SELECT line FROM format(LineAsString, 'first line\nsecond line') ORDER BY line;
SELECT JSONExtractInt(json, 'any', 'shape') FROM format(JSONAsString, '{"any": {"shape": 1}}');
SELECT name FROM format(CSVWithNames, 'id,name\n1,alice\n2,bob') WHERE name LIKE 'a%';
SELECT count() FROM (SELECT * FROM generateRandom('i UInt64, s String', 42) LIMIT 100);

-- ============================================================================
-- 10. file() over MEMFS — the main data path in Workers (putFile + query)
-- ============================================================================
SELECT * FROM file('/corpus/people.csv', CSV, 'id UInt32, name String, score Float64') ORDER BY id;
SELECT * FROM file('/corpus/people_names.csv', CSVWithNames) ORDER BY id;
SELECT * FROM file('/corpus/people.tsv', TSV, 'id UInt32, name String, score Float64') ORDER BY id;
SELECT * FROM file('/corpus/people.jsonl', JSONEachRow) ORDER BY id;
DESCRIBE file('/corpus/people_names.csv');
INSERT INTO FUNCTION file('/corpus/lite.parquet', Parquet) SELECT number AS id, toString(number) AS s, number / 3 AS f FROM numbers(1000);
SELECT count(), sum(id), max(f) FROM file('/corpus/lite.parquet', Parquet);
SELECT id, s FROM file('/corpus/lite.parquet') WHERE id % 100 = 0 ORDER BY id;
INSERT INTO FUNCTION file('/corpus/lite.arrow', Arrow) SELECT number AS id FROM numbers(100) SETTINGS output_format_parallel_formatting = 0;
SELECT count() FROM file('/corpus/lite.arrow', Arrow);
INSERT INTO FUNCTION file('/corpus/lite.native', Native) SELECT number AS n FROM numbers(100);
SELECT sum(n) FROM file('/corpus/lite.native', Native);
INSERT INTO FUNCTION file('/corpus/lite.csv.gz', CSV) SELECT number, toString(number) FROM numbers(200);
SELECT count() FROM file('/corpus/lite.csv.gz', CSV, 'a UInt64, b String');
CREATE TABLE lite_from_file ENGINE = Memory AS SELECT * FROM file('/corpus/people_names.csv');
SELECT count() FROM lite_from_file;
DROP TABLE lite_from_file;

-- ============================================================================
-- 11. Remote reads: url() and s3() (single-object, path-style, NOSIGN)
-- ============================================================================
SELECT * FROM url('{HTTP}/people_names.csv', CSVWithNames) ORDER BY id;
SELECT count() FROM url('{HTTP}/people.jsonl', JSONEachRow);
DESCRIBE url('{HTTP}/people_names.csv');
SELECT * FROM s3('{HTTP}/s3bucket/people_names.csv', NOSIGN, CSVWithNames) ORDER BY id;
SELECT count(), max(score) FROM s3('{HTTP}/s3bucket/people_names.csv', NOSIGN, CSVWithNames);

-- ============================================================================
-- 12. Output formats
-- ============================================================================
SELECT number, toString(number) AS s FROM numbers(5) FORMAT CSVWithNames;
SELECT number, toString(number) AS s FROM numbers(5) FORMAT TSV;
SELECT number, [number] AS arr FROM numbers(3) FORMAT JSON;
SELECT number, map('k', number) AS m FROM numbers(3) FORMAT JSONEachRow;
SELECT number, toString(number) AS s FROM numbers(5) FORMAT Pretty;
SELECT number, toString(number) AS s FROM numbers(5) FORMAT PrettyCompact;
SELECT number, toString(number) AS s FROM numbers(2) FORMAT Vertical;
SELECT number FROM numbers(3) FORMAT Values;
SELECT number, toString(number) AS s FROM numbers(3) FORMAT JSONCompact;
SELECT number, toString(number) AS s FROM numbers(3) FORMAT JSONCompactEachRow;
SELECT number, toString(number) AS s FROM numbers(3) FORMAT TSVWithNames;
SELECT number, toString(number) AS s FROM numbers(3) FORMAT CSVWithNamesAndTypes;
SELECT number FROM numbers(3) FORMAT JSONStrings;
SELECT number FROM numbers(3) FORMAT Markdown;
SELECT number FROM numbers(3) FORMAT RowBinary;
SELECT number FROM numbers(3) FORMAT Parquet;
SELECT number FROM numbers(1000) FORMAT Null;

-- ============================================================================
-- 13. Introspection + deliberate error paths (error reporting must be hot)
-- ============================================================================
EXPLAIN PLAN SELECT number % 3 AS k, count() FROM numbers(100) GROUP BY k;
SET max_block_size = 65409;
SHOW DATABASES;
SELECT name, engine FROM system.tables WHERE database = currentDatabase() ORDER BY name;
SELECT count() FROM system.functions;
SELECT name, value FROM system.settings WHERE changed ORDER BY name LIMIT 10;
SELECT * FROM system.one;
SELECT * FROM this_table_does_not_exist;
SELECT no_such_function(42);
SELECT toInt32('definitely not a number');
SELECT intDiv(1, 0);
SELECT FROM WHERE;
SELECT count() FROM file('/corpus/missing_file.csv', CSV, 'a Int32');
SELECT 'lite corpus done';
