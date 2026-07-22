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
--     formats, remote reads via url() and s3() (single-object path-style),
--     default-database DDL on Memory tables, error reporting.
-- Out: data-lake catalogs/Iceberg/Delta (their sync-HTTP bridge cannot run in
--     Workers anyway), exotic function families, MergeTree internals (cannot
--     run on the single-threaded build this bundle is based on).
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
SELECT DISTINCT number % 4 FROM numbers(100) ORDER BY 1;
SELECT number FROM numbers(100) ORDER BY number DESC LIMIT 5 OFFSET 10;
SELECT number % 3 AS g, number FROM numbers(30) ORDER BY g, number LIMIT 2 BY g;
SELECT toNullable(if(number % 3 = 0, NULL, number)) AS v FROM numbers(9) ORDER BY v ASC NULLS FIRST;
SELECT intDiv(number, 100) AS bucket, count() FROM numbers(100000) GROUP BY bucket ORDER BY bucket LIMIT 10;

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
SELECT sumArray([number, number + 1]) FROM numbers(100);
-- aggregates specialize per input type: cover the common non-UInt64 shapes
-- (String/Date/Nullable/Float/Int32) or their instantiations stay cold
SELECT min(s), max(s), any(s), anyLast(s), argMax(s, n), uniqExact(s) FROM (SELECT toString(number % 5) AS s, number AS n FROM numbers(100));
SELECT min(d), max(d), count(DISTINCT d) FROM (SELECT toDate('2024-01-01') + number AS d FROM numbers(10));
SELECT min(v), max(v), sum(v), avg(v) FROM (SELECT toNullable(number) AS v FROM numbers(100));
SELECT min(f), max(f), sum(f), avg(f), quantile(0.5)(f) FROM (SELECT number / 3 AS f FROM numbers(100));
SELECT min(i), max(i), sum(i), avg(i) FROM (SELECT toInt32(number) AS i FROM numbers(100));
SELECT sumIf(i, i % 2 = 0), minIf(s, n > 5), maxIf(s, n < 90), anyIf(s, n = 7) FROM (SELECT toInt32(number) AS i, toString(number) AS s, number AS n FROM numbers(100));

-- ============================================================================
-- 7. Everyday scalar functions
-- ============================================================================
SELECT length('hello'), lower('MiXeD'), upper('MiXeD'), reverse('abc'), repeat('ab', 3), char(72, 105);
SELECT substring('clickhouse', 1, 5), trim('  pad  '), leftPad('7', 4, '0'), concat('a', 'b', 'c'), format('{} scored {}', 'alice', 95);
SELECT startsWith('clickhouse', 'click'), endsWith('clickhouse', 'house'), position('clickhouse', 'house');
SELECT replaceOne('aaa', 'a', 'b'), replaceAll('aaa', 'a', 'b'), replaceRegexpAll('a1b2c3', '\d', '#');
SELECT splitByChar(',', 'a,b,c'), arrayStringConcat(['x', 'y', 'z'], '|'), extract('key=val', 'key=(\w+)'), extractAll('a1b22c333', '\d+');
SELECT formatReadableSize(123456789), hex('AB'), unhex('4142'), isValidUTF8('ok');
SELECT now() > toDateTime('2020-01-01'), today() >= yesterday(), toYear(toDate('2024-03-15')), toMonth(toDate('2024-03-15')), toDayOfWeek(toDate('2024-03-15'));
SELECT toStartOfMonth(toDate('2024-03-15')), toStartOfDay(toDateTime('2024-03-15 12:34:56')), toMonday(toDate('2024-03-15')), toYYYYMM(toDate('2024-03-15'));
SELECT addDays(toDate('2024-03-15'), 4), subtractMonths(toDate('2024-03-15'), 1), dateDiff('day', toDate('2024-01-01'), toDate('2024-03-15'));
SELECT formatDateTime(toDateTime('2024-03-15 12:34:56'), '%Y/%m/%d %H:%M:%S'), parseDateTimeBestEffort('15 Mar 2024 12:34:56'), toUnixTimestamp(toDateTime('2024-03-15 12:34:56'));
SELECT toDate('2024-03-15') + INTERVAL 10 DAY, date_trunc('week', toDateTime('2024-03-15 12:34:56'));
SELECT abs(-3), round(2.567, 2), floor(2.9), ceil(2.1), sqrt(2), pow(2, 10), exp(1), log(e()), sin(pi() / 2);
SELECT isFinite(1.0), isNaN(0 / 0.0), sign(-2.5), least(1.5, 2.5), pi();
SELECT range(5), arrayMap(x -> x * 2, [1, 2, 3]), arrayFilter(x -> x % 2 = 0, range(10)), arraySum([1, 2, 3]), arraySort([3, 1, 2]);
SELECT has([1, 2, 3], 2), indexOf([7, 8, 9], 9), arrayConcat([1, 2], [3]), arraySlice([1, 2, 3, 4, 5], 2, 3), arrayDistinct([1, 2, 2, 3]);
SELECT arrayJoin([1, 2, 3]) AS x, x * 10 FROM system.one;
SELECT number, arr FROM numbers(3) ARRAY JOIN [10, 20] AS arr ORDER BY number, arr;
SELECT mapKeys(map('a', 1, 'b', 2)), mapValues(map('a', 1)), tuple(1, 'a').1;
SELECT JSONExtractInt('{"a": 42}', 'a'), JSONExtractString('{"a": "hi"}', 'a'), JSONExtract('{"a": [1, 2]}', 'a', 'Array(Int64)'), JSONHas('{"a": 1}', 'a'), isValidJSON('{"ok": 1}');
SELECT JSONExtractRaw('{"a": {"b": 1}}', 'a'), JSON_VALUE('{"a": {"b": "v"}}', '$.a.b'), toJSONString(map('a', [1, 2]));
SELECT cityHash64('chdb'), sipHash64('chdb'), xxHash64('chdb'), hex(MD5('chdb')), hex(SHA256('chdb'));
SELECT rand() >= 0, randCanonical() BETWEEN 0 AND 1, length(randomString(16));
SELECT domain('https://www.example.com/a/b?q=1'), path('https://example.com/a/b?q=1'), extractURLParameter('https://example.com/p?q=1&r=2', 'r'), encodeURLComponent('a b&c');
SELECT transform(2, [1, 2, 3], ['one', 'two', 'three'], 'other'), bar(5, 0, 10, 10), version() != '', currentDatabase();

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
SELECT number FROM numbers(3) FORMAT Markdown;
SELECT number FROM numbers(3) FORMAT RowBinary;
SELECT number FROM numbers(3) FORMAT Parquet;
SELECT number FROM numbers(1000) FORMAT Null;

-- ============================================================================
-- 13. Introspection + deliberate error paths (error reporting must be hot)
-- ============================================================================
EXPLAIN PLAN SELECT number % 3 AS k, count() FROM numbers(100) GROUP BY k;
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
