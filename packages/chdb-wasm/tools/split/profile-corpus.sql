-- Profiling corpus for wasm-split: every statement here marks the functions it
-- executes as "hot", keeping them in the primary chdb.wasm. Anything not
-- exercised is split into chdb.deferred.wasm and lazy-loaded on first use, so
-- this file should cover what typical users run: SQL operators and clauses,
-- the common scalar-function families, aggregate functions (+ combinators),
-- table functions, input/output formats, DDL/DML, and error handling.
--
-- Runner: tools/split/run-profile.mjs. Rules:
--   * statements end with `;` at end of line (no semicolons inside strings)
--   * `--` lines are comments
--   * {HTTP} {S3} {CATALOG} {UNITY} are substituted by the runner when the
--     corresponding fixture service is up; statements with an unsubstituted
--     placeholder are skipped
--   * failures are recorded, not fatal: a few statements fail on purpose to
--     keep the error-formatting machinery hot
--
-- Keep statements small (row counts in the thousands): coverage is what
-- matters, not volume.

-- ============================================================================
-- 1. Literals, types, casts
-- ============================================================================
SELECT 1, -2, 3.5, -0.25, 1e10, 0x1F, 'hello', NULL;
SELECT toTypeName(42), toTypeName(4.2), toTypeName('s'), toTypeName(NULL), toTypeName([1, 2]), toTypeName((1, 'a')), toTypeName(map('k', 1));
SELECT toInt8(1), toInt16(2), toInt32(3), toInt64(4);
SELECT toUInt8(1), toUInt16(2), toUInt32(3), toUInt64(4);
SELECT toFloat32(1.5), toFloat64(2.5), toDecimal32(1.2345, 3), toDecimal64(1.23456, 5);
SELECT toInt32OrZero('bad'), toInt32OrNull('bad'), toUInt64OrDefault('x', toUInt64(7)), toFloat64OrZero('nan-ish');
SELECT toString(123), toFixedString('ab', 4), toLowCardinality('tag'), toNullable(5), assumeNotNull(toNullable(6));
SELECT CAST(42 AS String), CAST('2024-01-02' AS Date), 42::Float64, '[1,2,3]'::Array(Int32), accurateCast(300, 'Int16'), accurateCastOrNull(300, 'Int8');
SELECT toDate('2024-03-15'), toDate32('1950-06-01'), toDateTime('2024-03-15 12:34:56'), toDateTime64('2024-03-15 12:34:56.789', 3);
SELECT toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0'), generateUUIDv4() != generateUUIDv4();
SELECT toIPv4('1.2.3.4'), toIPv6('::ffff:1.2.3.4'), toBool(1), toBool('true');
SELECT CAST('a' AS Enum('a' = 1, 'b' = 2)), CAST(2 AS Enum('a' = 1, 'b' = 2));
SELECT toIntervalSecond(30), toIntervalDay(2), INTERVAL 3 MONTH;
SELECT reinterpretAsUInt32(reinterpretAsString(305419896)), reinterpret(-1, 'UInt8');

-- ============================================================================
-- 2. Operators and expressions
-- ============================================================================
SELECT 7 + 3, 7 - 3, 7 * 3, 7 / 3, 7 % 3, intDiv(7, 3), moduloOrZero(7, 0), -abs(-5);
SELECT 1 < 2, 2 <= 2, 3 > 2, 3 >= 4, 1 = 1, 1 != 2, 'a' < 'b';
SELECT 1 AND 0, 1 OR 0, NOT 1, xor(1, 0);
SELECT 5 BETWEEN 1 AND 10, 3 IN (1, 2, 3), 4 NOT IN (1, 2, 3), (1, 'a') = (1, 'a');
SELECT number IN (SELECT number FROM numbers(5) WHERE number % 2 = 0) FROM numbers(6);
SELECT 'foobar' LIKE 'foo%', 'foobar' ILIKE 'FOO%', 'foobar' NOT LIKE 'baz%', match('foobar', '^fo+bar$');
SELECT NULL IS NULL, 1 IS NOT NULL, ifNull(NULL, 'fallback'), coalesce(NULL, NULL, 3), nullIf(5, 5);
SELECT 'a' || 'b' || 'c', [1, 2][1], map('x', 10)['x'], (1, 'two').2;
SELECT bitAnd(12, 10), bitOr(12, 10), bitXor(12, 10), bitNot(0), bitCount(255), bitTest(5, 0), byteSwap(3735928559);
SELECT if(1 = 1, 'yes', 'no'), multiIf(0, 'a', 1, 'b', 'c'), CASE WHEN 2 > 1 THEN 'gt' ELSE 'le' END, CASE 2 WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'many' END;
SELECT greatest(1, 2, 3), least(1, 2, 3), greatest('a', 'b'), max2(1.5, 2.5), min2(1.5, 2.5);

-- ============================================================================
-- 3. Core clauses: WHERE / GROUP BY / HAVING / ORDER BY / LIMIT / DISTINCT
-- ============================================================================
SELECT count() FROM numbers(10000) WHERE number % 7 = 3;
SELECT number % 5 AS k, count(), sum(number), avg(number), min(number), max(number) FROM numbers(10000) GROUP BY k ORDER BY k;
SELECT number % 5 AS k, count() AS c FROM numbers(10000) GROUP BY k HAVING c > 1500 ORDER BY c DESC, k;
SELECT number % 3 AS a, number % 4 AS b, count() FROM numbers(1000) GROUP BY a, b WITH ROLLUP ORDER BY a, b;
SELECT number % 3 AS a, number % 4 AS b, count() FROM numbers(1000) GROUP BY a, b WITH CUBE ORDER BY a, b;
SELECT number % 3 AS a, count() FROM numbers(1000) GROUP BY a WITH TOTALS ORDER BY a;
SELECT number % 3 AS a, number % 2 AS b, count() FROM numbers(1000) GROUP BY GROUPING SETS ((a), (b), (a, b)) ORDER BY a, b;
SELECT DISTINCT number % 4 FROM numbers(100) ORDER BY 1;
SELECT DISTINCT ON (number % 3) number, number % 3 FROM numbers(30) ORDER BY number % 3, number;
SELECT number FROM numbers(100) ORDER BY number DESC LIMIT 5;
SELECT number FROM numbers(100) ORDER BY number LIMIT 5 OFFSET 20;
SELECT number % 3 AS g, number FROM numbers(30) ORDER BY g, number LIMIT 2 BY g;
SELECT toNullable(if(number % 3 = 0, NULL, number)) AS v FROM numbers(9) ORDER BY v ASC NULLS FIRST;
SELECT number FROM numbers(3) ORDER BY number WITH FILL FROM 0 TO 10 STEP 2;
SELECT intDiv(number, 100) AS bucket, count() FROM numbers(100000) GROUP BY bucket ORDER BY bucket LIMIT 10;

-- ============================================================================
-- 4. Subqueries, CTEs, set operations
-- ============================================================================
SELECT (SELECT max(number) FROM numbers(100)) + 1;
SELECT number FROM (SELECT number * 2 AS number FROM numbers(10)) WHERE number > 10 ORDER BY number;
WITH 10 AS lim SELECT count() FROM numbers(100) WHERE number < lim;
WITH evens AS (SELECT number FROM numbers(20) WHERE number % 2 = 0) SELECT count(), sum(number) FROM evens;
WITH RECURSIVE fib AS (SELECT 0 AS a, 1 AS b, 1 AS i UNION ALL SELECT b, a + b, i + 1 FROM fib WHERE i < 12) SELECT max(b) FROM fib;
SELECT number FROM numbers(3) UNION ALL SELECT number + 10 FROM numbers(3) ORDER BY number;
SELECT number % 3 FROM numbers(10) UNION DISTINCT SELECT number % 4 FROM numbers(10) ORDER BY 1;
SELECT number FROM numbers(10) INTERSECT SELECT number FROM numbers(5, 10) ORDER BY number;
SELECT number FROM numbers(10) EXCEPT SELECT number FROM numbers(5, 10) ORDER BY number;

-- ============================================================================
-- 5. JOINs
-- ============================================================================
SELECT a.number, b.number FROM numbers(5) AS a INNER JOIN (SELECT number FROM numbers(3, 5)) AS b ON a.number = b.number ORDER BY a.number;
SELECT a.number, b.n FROM numbers(5) AS a LEFT JOIN (SELECT number AS number, number * 10 AS n FROM numbers(3)) AS b ON a.number = b.number ORDER BY a.number;
SELECT a.number, b.number FROM (SELECT number FROM numbers(3)) AS a RIGHT JOIN numbers(5) AS b ON a.number = b.number ORDER BY b.number;
SELECT a.number, b.number FROM (SELECT number FROM numbers(4)) AS a FULL OUTER JOIN (SELECT number + 2 AS number FROM numbers(4)) AS b ON a.number = b.number ORDER BY a.number, b.number;
SELECT count() FROM numbers(10) AS a CROSS JOIN numbers(10) AS b;
SELECT a.number FROM numbers(6) AS a SEMI LEFT JOIN (SELECT number FROM numbers(3)) AS b ON a.number = b.number ORDER BY a.number;
SELECT a.number FROM numbers(6) AS a ANTI LEFT JOIN (SELECT number FROM numbers(3)) AS b ON a.number = b.number ORDER BY a.number;
SELECT a.k, a.t, b.t FROM (SELECT 1 AS k, number * 10 AS t FROM numbers(5)) AS a ASOF JOIN (SELECT 1 AS k, number * 15 AS t FROM numbers(5)) AS b ON a.k = b.k AND a.t >= b.t ORDER BY a.t;
SELECT x.number FROM numbers(4) AS x JOIN numbers(4) AS y USING (number) ORDER BY x.number;
SELECT count() FROM numbers(100) AS a JOIN numbers(100) AS b ON a.number = b.number JOIN numbers(100) AS c ON b.number = c.number;
SET join_algorithm = 'partial_merge';
SELECT count() FROM numbers(1000) AS a JOIN numbers(1000) AS b ON a.number = b.number;
SET join_algorithm = 'hash';

-- ============================================================================
-- 6. Window functions
-- ============================================================================
SELECT number, row_number() OVER (ORDER BY number DESC) AS rn FROM numbers(10) ORDER BY number;
SELECT number % 3 AS g, number, rank() OVER (PARTITION BY number % 3 ORDER BY number) AS r, dense_rank() OVER (PARTITION BY number % 3 ORDER BY number) AS dr FROM numbers(12) ORDER BY g, number;
SELECT number, sum(number) OVER (ORDER BY number ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running FROM numbers(10);
SELECT number, avg(number) OVER (ORDER BY number ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING) AS centered FROM numbers(10);
SELECT number, lagInFrame(number, 1) OVER w AS prev, leadInFrame(number, 1) OVER w AS next FROM numbers(8) WINDOW w AS (ORDER BY number ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING);
SELECT number, first_value(number) OVER w AS f, last_value(number) OVER w AS l FROM numbers(8) WINDOW w AS (PARTITION BY number % 2 ORDER BY number ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) ORDER BY number;
SELECT number, ntile(3) OVER (ORDER BY number) AS bucket FROM numbers(9);
SELECT number % 4 AS g, max(number) OVER (PARTITION BY g) FROM numbers(16) QUALIFY number = max(number) OVER (PARTITION BY g) ORDER BY g;
SELECT number, count() OVER (ORDER BY number RANGE BETWEEN 3 PRECEDING AND CURRENT ROW) FROM numbers(10);

-- ============================================================================
-- 7. Aggregate functions and combinators
-- ============================================================================
SELECT count(), count(DISTINCT number % 10), countIf(number % 2 = 0) FROM numbers(1000);
SELECT sum(number), sumIf(number, number % 2 = 1), avg(number), avgIf(number, number > 500) FROM numbers(1000);
SELECT min(number), max(number), minIf(number, number > 10), maxIf(number, number < 990), any(number), anyLast(number) FROM numbers(1000);
SELECT argMin(number, number % 7), argMax(number, number % 7) FROM numbers(1000);
SELECT uniq(number % 100), uniqExact(number % 100), uniqCombined(number % 100), uniqCombined64(number % 100), uniqHLL12(number % 100) FROM numbers(10000);
SELECT quantile(0.5)(number), quantiles(0.25, 0.5, 0.75)(number), quantileExact(0.9)(number), quantileTiming(0.5)(number % 1000), quantileBFloat16(0.5)(number), median(number) FROM numbers(10000);
SELECT topK(3)(number % 10), topKWeighted(3)(number % 10, number % 4 + 1) FROM numbers(10000);
SELECT groupArray(number), groupUniqArray(number % 3), groupArraySample(3)(number) FROM numbers(10);
SELECT groupArrayMovingSum(3)(number), groupArrayMovingAvg(3)(number) FROM numbers(10);
SELECT groupBitAnd(number), groupBitOr(number), groupBitXor(number) FROM numbers(1, 100);
SELECT groupBitmap(toUInt32(number % 50)), bitmapCardinality(groupBitmapState(toUInt32(number % 50))) FROM numbers(1000);
SELECT corr(toFloat64(number), number * 2.0), covarPop(toFloat64(number), number + 1.0), covarSamp(toFloat64(number), number + 1.0) FROM numbers(100);
SELECT stddevPop(number), stddevSamp(number), varPop(number), varSamp(number), skewPop(number), skewSamp(number), kurtPop(number), kurtSamp(number) FROM numbers(1000);
SELECT deltaSum(number), sumCount(number) FROM numbers(100);
SELECT histogram(5)(toFloat64(number % 100)) FROM numbers(1000);
SELECT simpleLinearRegression(toFloat64(number), toFloat64(2 * number + 1)) FROM numbers(100);
SELECT mannWhitneyUTest(toFloat64(number % 17), number % 2) FROM numbers(200);
SELECT studentTTest(toFloat64(number % 13), number % 2), welchTTest(toFloat64(number % 13), number % 2) FROM numbers(200);
SELECT rankCorr(toFloat64(number % 7), toFloat64(number % 11)) FROM numbers(200);
SELECT cramersV(number % 3, number % 5), theilsU(number % 3, number % 5) FROM numbers(1000);
SELECT sumMap(map(number % 4, number)) FROM numbers(100);
SELECT sumArray([number, number + 1]), avgArray([number, number * 2]) FROM numbers(100);
SELECT countMerge(s) FROM (SELECT countState() AS s FROM numbers(100));
SELECT avgMerge(s) FROM (SELECT avgState(number) AS s FROM numbers(100) GROUP BY number % 10);
SELECT finalizeAggregation(initializeAggregation('uniqState', 42)), finalizeAggregation(sumStateOrDefault) FROM (SELECT sumState(number) AS sumStateOrDefault FROM numbers(10));
SELECT sumForEach([number, number + 1]) FROM numbers(10);
SELECT countResample(0, 10, 2)(number) FROM numbers(10);
SELECT sumDistinct(number % 5), avgOrNull(number), maxOrDefault(number) FROM numbers(100);
SELECT sequenceMatch('(?1)(?2)')(toDateTime(number), number % 3 = 0, number % 3 = 1) FROM numbers(100);
SELECT windowFunnel(10)(toDateTime(number), number % 5 = 0, number % 5 = 1, number % 5 = 2) FROM numbers(100);
SELECT retention(number % 7 = 0, number % 7 = 1, number % 7 = 2) FROM numbers(49);
SELECT exponentialMovingAverage(5)(toFloat64(number), number) FROM numbers(50);
SELECT number % 3 AS g, groupArrayIf(number, number % 2 = 0) FROM numbers(20) GROUP BY g ORDER BY g;

-- ============================================================================
-- 8. String functions
-- ============================================================================
SELECT length('hello'), lengthUTF8('héllo'), empty(''), notEmpty('x'), byteSize('abc'), char(72, 105);
SELECT lower('MiXeD'), upper('MiXeD'), initcap('hello world');
SELECT reverse('abc'), reverseUTF8('héllo'), repeat('ab', 3), space(4) || '|';
SELECT substring('clickhouse', 6), substring('clickhouse', 1, 5), substringUTF8('héllo', 2, 3), left('clickhouse', 5), right('clickhouse', 5);
SELECT trim('  pad  '), trimLeft('  pad'), trimRight('pad  '), trim(BOTH 'x' FROM 'xxpadxx'), leftPad('7', 4, '0'), rightPad('7', 4, '.');
SELECT concat('a', 'b', 'c'), concatWithSeparator('-', 'x', 'y', 'z'), format('{} scored {}', 'alice', 95);
SELECT startsWith('clickhouse', 'click'), endsWith('clickhouse', 'house'), position('clickhouse', 'house'), positionCaseInsensitive('ClickHouse', 'HOUSE'), locate('house', 'clickhouse');
SELECT replaceOne('aaa', 'a', 'b'), replaceAll('aaa', 'a', 'b'), replaceRegexpOne('2024-03-15', '(\d+)-(\d+)-(\d+)', '\3/\2/\1'), replaceRegexpAll('a1b2c3', '\d', '#'), translate('abc', 'abc', 'xyz');
SELECT splitByChar(',', 'a,b,c'), splitByString(', ', 'a, b, c'), splitByRegexp('\d+', 'a1bb22ccc'), arrayStringConcat(['x', 'y', 'z'], '|');
SELECT extract('key=val', 'key=(\w+)'), extractAll('a1b22c333', '\d+'), extractGroups('alice:95', '(\w+):(\d+)'), extractAllGroupsVertical('a:1,b:2', '(\w):(\d)');
SELECT match('hello123', '^[a-z]+\d+$'), countMatches('a1b2c3', '\d'), countSubstrings('banana', 'an'), regexpExtract('foo123bar', '([0-9]+)', 1);
SELECT multiSearchAny('clickhouse', ['house', 'click']), multiSearchFirstIndex('clickhouse', ['x', 'house']), multiSearchAllPositions('clickhouse', ['click', 'house']);
SELECT levenshteinDistance('kitten', 'sitting'), jaroSimilarity('martha', 'marhta'), stringJaccardIndex('abc', 'abd'), ngramDistance('clickhouse', 'clckhouse');
SELECT isValidUTF8('ok'), toValidUTF8('ok');
SELECT formatReadableSize(123456789), formatReadableTimeDelta(361);
SELECT hex('AB'), unhex('4142'), bin('a'), unbin('01100001');

-- ============================================================================
-- 9. Date/time functions
-- ============================================================================
SELECT now() > toDateTime('2020-01-01'), now64(3) > toDateTime64('2020-01-01 00:00:00', 3), today() >= yesterday();
SELECT toYear(d), toQuarter(d), toMonth(d), toDayOfMonth(d), toDayOfWeek(d), toDayOfYear(d), toISOWeek(d), toISOYear(d) FROM (SELECT toDate('2024-03-15') AS d);
SELECT toHour(t), toMinute(t), toSecond(t), toUnixTimestamp(t) FROM (SELECT toDateTime('2024-03-15 12:34:56') AS t);
SELECT toStartOfYear(t), toStartOfQuarter(t), toStartOfMonth(t), toMonday(t), toStartOfWeek(t), toLastDayOfMonth(t) FROM (SELECT toDate('2024-03-15') AS t);
SELECT toStartOfDay(t), toStartOfHour(t), toStartOfMinute(t), toStartOfFifteenMinutes(t), toStartOfFiveMinutes(t), toStartOfSecond(toDateTime64('2024-03-15 12:34:56.789', 3)) FROM (SELECT toDateTime('2024-03-15 12:34:56') AS t);
SELECT toYYYYMM(toDate('2024-03-15')), toYYYYMMDD(toDate('2024-03-15')), toYYYYMMDDhhmmss(toDateTime('2024-03-15 12:34:56'));
SELECT addYears(d, 1), addMonths(d, 2), addWeeks(d, 3), addDays(d, 4), subtractDays(d, 5), addHours(toDateTime(d), 6), subtractMinutes(toDateTime(d), 7) FROM (SELECT toDate('2024-03-15') AS d);
SELECT dateDiff('day', toDate('2024-01-01'), toDate('2024-03-15')), dateDiff('hour', toDateTime('2024-03-15 00:00:00'), toDateTime('2024-03-15 12:00:00')), age('year', toDate('2000-06-15'), toDate('2024-03-15'));
SELECT date_add(DAY, 3, toDate('2024-03-15')), date_sub(MONTH, 1, toDate('2024-03-15')), timestamp_diff(MINUTE, toDateTime('2024-03-15 12:00:00'), toDateTime('2024-03-15 12:45:00'));
SELECT toDate('2024-03-15') + INTERVAL 10 DAY, now() - INTERVAL 1 HOUR > toDateTime('2020-01-01');
SELECT formatDateTime(toDateTime('2024-03-15 12:34:56'), '%Y/%m/%d %H:%M:%S'), formatDateTime(toDate('2024-03-15'), '%W %j');
SELECT parseDateTime('2024*03*15', '%Y*%m*%d'), parseDateTimeBestEffort('15 Mar 2024 12:34:56');
SELECT makeDate(2024, 3, 15), makeDateTime(2024, 3, 15, 12, 34, 56), YYYYMMDDToDate(20240315), fromUnixTimestamp(1700000000);
SELECT monthName(toDate('2024-03-15')), toRelativeDayNum(toDate('2024-03-15')) > 0, timeSlot(toDateTime('2024-03-15 12:34:56'));
SELECT toTimeZone(toDateTime('2024-03-15 12:00:00', 'UTC'), 'Asia/Shanghai'), timeZoneOf(toDateTime('2024-03-15 12:00:00', 'UTC')), toDateTime('2024-03-15 12:00:00', 'America/New_York');
SELECT toStartOfInterval(toDateTime('2024-03-15 12:34:56'), INTERVAL 10 MINUTE), date_trunc('week', toDateTime('2024-03-15 12:34:56'));

-- ============================================================================
-- 10. Math functions
-- ============================================================================
SELECT abs(-3), sign(-2.5), exp(1), log(e()), log2(8), log10(1000), exp2(10), exp10(3), sqrt(2), cbrt(27), pow(2, 10), intExp2(8), intExp10(4);
SELECT sin(pi() / 2), cos(0), tan(pi() / 4), asin(1), acos(0), atan(1), atan2(1, 1), sinh(1), cosh(1), tanh(1);
SELECT degrees(pi()), radians(180), erf(1), erfc(1), lgamma(10), tgamma(5), factorial(10), hypot(3, 4);
SELECT round(2.567), round(2.567, 2), roundBankers(2.5), floor(2.9), ceil(2.1), trunc(-2.9), round(1234, -2), roundToExp2(100), roundDuration(95), roundAge(35);
SELECT isFinite(1.0), isInfinite(1 / 0.0), isNaN(0 / 0.0), ifNotFinite(1 / 0.0, -1), log1p(0.5);
SELECT e(), pi(), plus(2, 3), minus(5, 2), multiply(4, 6), divide(10, 4), negate(7);

-- ============================================================================
-- 11. Array functions
-- ============================================================================
SELECT [1, 2, 3], array(4, 5), range(5), range(2, 10, 2), arrayWithConstant(3, 'x'), emptyArrayInt32();
SELECT length([1, 2, 3]), empty([]), notEmpty([1]), arrayElement([10, 20, 30], 2), [10, 20, 30][-1];
SELECT arrayMap(x -> x * 2, [1, 2, 3]), arrayMap((x, y) -> x + y, [1, 2], [10, 20]), arrayFilter(x -> x % 2 = 0, range(10));
SELECT arrayExists(x -> x > 2, [1, 2, 3]), arrayAll(x -> x > 0, [1, 2, 3]), arrayCount(x -> x % 3 = 0, range(20)), arrayFirst(x -> x > 5, range(10)), arrayLast(x -> x < 5, range(10)), arrayFirstIndex(x -> x = 3, range(10));
SELECT arraySum([1, 2, 3]), arrayProduct([1.0, 2, 3]), arrayMin([3, 1, 2]), arrayMax([3, 1, 2]), arrayAvg([1, 2, 3]), arrayDifference([10, 13, 17]);
SELECT arraySort([3, 1, 2]), arrayReverseSort([1, 3, 2]), arraySort(x -> -x, [1, 3, 2]), arrayReverse([1, 2, 3]);
SELECT arrayDistinct([1, 2, 2, 3, 1]), arrayUniq([1, 2, 2, 3]), arrayCompact([1, 1, 2, 2, 1]), arrayEnumerate([9, 8, 7]), arrayEnumerateUniq([1, 1, 2, 1]);
SELECT has([1, 2, 3], 2), hasAll([1, 2, 3, 4], [2, 4]), hasAny([1, 2], [9, 2]), hasSubstr([1, 2, 3, 4], [2, 3]), indexOf([7, 8, 9], 9), countEqual([1, 2, 2, 3], 2);
SELECT arrayConcat([1, 2], [3], [4, 5]), arraySlice([1, 2, 3, 4, 5], 2, 3), arrayResize([1, 2], 4, 0), arrayPushBack([1, 2], 3), arrayPushFront([2, 3], 1), arrayPopBack([1, 2, 3]), arrayPopFront([1, 2, 3]);
SELECT arrayFlatten([[1, 2], [3]]), arrayZip([1, 2], ['a', 'b']), arrayIntersect([1, 2, 3], [2, 3, 4]);
SELECT arrayReduce('sum', range(10)), arrayReduce('uniq', [1, 1, 2]), arrayReduceInRanges('sum', [(1, 3), (2, 2)], [10, 20, 30, 40]);
SELECT arrayJoin([1, 2, 3]) AS x, x * 10 FROM system.one;
SELECT number, arr FROM numbers(3) ARRAY JOIN [10, 20] AS arr ORDER BY number, arr;
SELECT number, arr FROM numbers(2) LEFT ARRAY JOIN emptyArrayInt32() AS arr;
SELECT arrayAUC([0.1, 0.4, 0.35, 0.8], [0, 0, 1, 1]);

-- ============================================================================
-- 12. Tuple / Map functions
-- ============================================================================
SELECT tuple(1, 'a', 3.5) AS t, tupleElement(t, 1), t.3, untuple((7, 8));
SELECT map('a', 1, 'b', 2) AS m, mapKeys(m), mapValues(m), mapContains(m, 'a'), length(m);
SELECT mapFromArrays(['x', 'y'], [10, 20]), mapFilter((k, v) -> v > 1, map('a', 1, 'b', 2));
SELECT CAST(([1, 2], ['one', 'two']), 'Map(Int32, String)'), CAST(map(1, 'one'), 'Array(Tuple(Int32, String))');
SELECT tupleHammingDistance((1, 2, 3), (1, 9, 3));

-- ============================================================================
-- 13. JSON functions (+ JSON type)
-- ============================================================================
SELECT JSONExtractInt('{"a": 42}', 'a'), JSONExtractFloat('{"a": 4.2}', 'a'), JSONExtractBool('{"a": true}', 'a'), JSONExtractString('{"a": "hi"}', 'a');
SELECT JSONExtractRaw('{"a": {"b": 1}}', 'a'), JSONExtractArrayRaw('{"a": [1, 2]}', 'a'), JSONExtractKeysAndValues('{"a": 1, "b": 2}', 'Int32');
SELECT JSONHas('{"a": 1}', 'a'), JSONLength('{"a": [1, 2, 3]}', 'a'), JSONType('{"a": [1]}', 'a'), JSONArrayLength('[1, 2, 3]'), isValidJSON('{"ok": 1}'), isValidJSON('nope{');
SELECT JSONExtract('{"a": [1, 2]}', 'a', 'Array(Int64)'), JSONExtract('{"x": {"y": 5}}', 'x', 'y', 'Int64');
SELECT JSON_VALUE('{"a": {"b": "v"}}', '$.a.b'), JSON_QUERY('{"a": [1, 2]}', '$.a'), JSON_EXISTS('{"a": 1}', '$.a');
SELECT simpleJSONExtractInt('{"k": 7}', 'k'), simpleJSONExtractString('{"k": "s"}', 'k'), simpleJSONHas('{"k": 1}', 'k');
SELECT toJSONString(map('a', [1, 2])), toJSONString((1, 'two', [3]));
SET allow_experimental_json_type = 1;
SELECT '{"user": {"name": "alice", "score": 9.5}, "tags": ["x", "y"]}'::JSON AS j, j.user.name, j.tags, JSONAllPaths(j);

-- ============================================================================
-- 14. URL / IP / UUID / hash / random / encoding functions
-- ============================================================================
SELECT protocol(u), domain(u), domainWithoutWWW(u), topLevelDomain(u), port(u), netloc(u) FROM (SELECT 'https://www.example.co.uk:8443/a/b?q=1&r=2#frag' AS u);
SELECT path(u), pathFull(u), queryString(u), fragment(u), queryStringAndFragment(u) FROM (SELECT 'https://example.com/a/b?q=1&r=2#frag' AS u);
SELECT extractURLParameter(u, 'r'), extractURLParameters(u), extractURLParameterNames(u), cutQueryString(u) FROM (SELECT 'https://example.com/p?q=1&r=2' AS u);
SELECT URLHierarchy('https://example.com/a/b/c'), URLPathHierarchy('https://example.com/a/b/c'), encodeURLComponent('a b&c'), decodeURLComponent('a%20b%26c');
SELECT IPv4NumToString(16909060), IPv4StringToNum('1.2.3.4'), IPv6NumToString(IPv6StringToNum('2001:db8::1')), IPv4ToIPv6(toIPv4('1.2.3.4'));
SELECT isIPv4String('1.2.3.4'), isIPv6String('::1'), isIPAddressInRange('192.168.1.7', '192.168.0.0/16'), IPv4CIDRToRange(toIPv4('10.0.0.1'), 8);
SELECT hex(MD5('chdb')), hex(SHA1('chdb')), hex(SHA224('chdb')), hex(SHA256('chdb')), hex(SHA512('chdb')), halfMD5('chdb');
SELECT sipHash64('chdb'), cityHash64('chdb'), farmHash64('chdb'), metroHash64('chdb'), wyHash64('chdb');
SELECT xxHash64('chdb'), xxh3('chdb'), CRC32('chdb'), CRC64('chdb'), gccMurmurHash('chdb');
SELECT javaHash('chdb'), hiveHash('chdb');
SELECT rand() >= 0, rand64() >= 0, randCanonical() BETWEEN 0 AND 1, randConstant() >= 0, randUniform(0, 10) BETWEEN 0 AND 10;
SELECT randNormal(0, 1) IS NOT NULL, randExponential(1) >= 0, length(randomString(16)), length(randomPrintableASCII(8));
SELECT UUIDStringToNum('61f0c404-5cb3-11e7-907b-a6006ad3dba0') AS n, UUIDNumToString(n), toString(generateUUIDv7()) != '';

-- ============================================================================
-- 15. Conditional / misc / introspection functions
-- ============================================================================
SELECT transform(2, [1, 2, 3], ['one', 'two', 'three'], 'other'), transform('b', ['a', 'b'], [1, 2], 0);
SELECT bar(5, 0, 10, 10), materialize(42), identity('x'), ignore(1, 2, 3), isConstant(3), rowNumberInAllBlocks() FROM numbers(2);
SELECT version() != '', currentDatabase(), currentUser() != '', hostName() != '', uptime() >= 0, timezone() != '';
SELECT blockSize() > 0, blockNumber() >= 0 FROM numbers(5);
SELECT defaultValueOfArgumentType(CAST(1 AS Nullable(Int32))), defaultValueOfTypeName('String'), toColumnTypeName(now());
SELECT greatCircleDistance(-73.98, 40.75, 116.39, 39.91), geoDistance(-73.98, 40.75, 116.39, 39.91), greatCircleAngle(0, 0, 10, 10);
SELECT geohashEncode(-5.60302734375, 42.593994140625), geohashDecode('ezs42');
SELECT sleepEachRow(0.001) FROM numbers(2);

-- ============================================================================
-- 16. DDL + DML: Memory / MergeTree family / views / mutations
-- ============================================================================
-- Atomic (the default engine) is mt-only: on the single-threaded build CREATE
-- DATABASE aborts the wasm instance (pre-existing st limitation, not split
-- related); the Memory-engine fallback keeps this section running there.
CREATE DATABASE IF NOT EXISTS corpus /*mt-only*/;
CREATE DATABASE IF NOT EXISTS corpus ENGINE = Memory;
CREATE TABLE corpus.mem (id UInt32, name String, score Float64, tags Array(String), created DateTime) ENGINE = Memory;
INSERT INTO corpus.mem VALUES (1, 'alice', 9.5, ['a', 'b'], '2024-01-01 10:00:00'), (2, 'bob', 7.25, ['b'], '2024-01-02 11:30:00'), (3, 'carol', 8.0, [], '2024-01-03 09:15:00');
INSERT INTO corpus.mem SELECT number + 10, concat('user', toString(number)), number * 1.5, [toString(number % 3)], toDateTime('2024-02-01 00:00:00') + number * 3600 FROM numbers(50);
SELECT count(), avg(score), uniqExact(name) FROM corpus.mem;
SELECT name, score FROM corpus.mem WHERE has(tags, 'b') ORDER BY score DESC;
CREATE TABLE corpus.mt (id UInt64, ts DateTime, kind LowCardinality(String), val Nullable(Float64), payload String) ENGINE = MergeTree ORDER BY (kind, id) PARTITION BY toYYYYMM(ts) SETTINGS index_granularity = 256;
INSERT INTO corpus.mt SELECT number, toDateTime('2024-01-01 00:00:00') + number * 900, ['clicks', 'views', 'buys'][number % 3 + 1], if(number % 11 = 0, NULL, number / 7), concat('p-', toString(cityHash64(number))) FROM numbers(5000);
SELECT kind, count(), round(avg(val), 3), min(ts), max(ts) FROM corpus.mt GROUP BY kind ORDER BY kind;
SELECT count() FROM corpus.mt PREWHERE kind = 'clicks' WHERE val > 100;
SELECT id, kind, val FROM corpus.mt WHERE id BETWEEN 100 AND 120 ORDER BY id LIMIT 10;
SELECT partition, rows FROM system.parts WHERE database = 'corpus' AND table = 'mt' AND active ORDER BY partition;
ALTER TABLE corpus.mt ADD COLUMN extra UInt8 DEFAULT 0;
ALTER TABLE corpus.mt UPDATE extra = 1 WHERE id < 100 SETTINGS mutations_sync = 1;
SELECT countIf(extra = 1) FROM corpus.mt;
ALTER TABLE corpus.mt DROP COLUMN extra;
DELETE FROM corpus.mt WHERE id % 100 = 7;
SELECT count() FROM corpus.mt;
OPTIMIZE TABLE corpus.mt FINAL;
CREATE TABLE corpus.repl (k UInt32, v String, ver UInt32) ENGINE = ReplacingMergeTree(ver) ORDER BY k;
INSERT INTO corpus.repl VALUES (1, 'old', 1), (1, 'new', 2), (2, 'only', 1);
SELECT k, v FROM corpus.repl FINAL ORDER BY k;
CREATE TABLE corpus.summing (k UInt32, s UInt64) ENGINE = SummingMergeTree ORDER BY k;
INSERT INTO corpus.summing SELECT number % 5, number FROM numbers(100);
OPTIMIZE TABLE corpus.summing FINAL;
SELECT k, s FROM corpus.summing ORDER BY k;
CREATE TABLE corpus.agg (k UInt8, st AggregateFunction(avg, Float64)) ENGINE = AggregatingMergeTree ORDER BY k;
INSERT INTO corpus.agg SELECT number % 3, avgState(toFloat64(number)) FROM numbers(1000) GROUP BY number % 3;
SELECT k, avgMerge(st) FROM corpus.agg GROUP BY k ORDER BY k;
CREATE VIEW corpus.v AS SELECT kind, count() AS c FROM corpus.mt GROUP BY kind;
SELECT * FROM corpus.v ORDER BY kind;
CREATE MATERIALIZED VIEW corpus.mv ENGINE = SummingMergeTree ORDER BY kind AS SELECT kind, count() AS c FROM corpus.mt GROUP BY kind;
INSERT INTO corpus.mt (id, ts, kind, val, payload) SELECT number + 100000, now(), 'views', 1.0, 'x' FROM numbers(100);
SELECT kind, sum(c) FROM corpus.mv GROUP BY kind ORDER BY kind;
CREATE TABLE corpus.nullsink (x UInt64) ENGINE = Null;
INSERT INTO corpus.nullsink SELECT number FROM numbers(1000);
CREATE TEMPORARY TABLE tmp_corpus (a Int32);
INSERT INTO tmp_corpus VALUES (1), (2);
SELECT sum(a) FROM tmp_corpus;
CREATE FUNCTION corpus_add2 AS (a, b) -> a + b * 2;
SELECT corpus_add2(3, 4);
DROP FUNCTION corpus_add2;
TRUNCATE TABLE corpus.mem;
SELECT count() FROM corpus.mem;
RENAME TABLE corpus.repl TO corpus.repl2;
EXISTS TABLE corpus.repl2;
DROP TABLE corpus.repl2;
SHOW TABLES FROM corpus;
SHOW CREATE TABLE corpus.mt;
DESCRIBE TABLE corpus.mt;

-- ============================================================================
-- 17. Spill-to-disk + heavier pipelines (external sort/aggregation paths)
-- ============================================================================
SET max_bytes_before_external_group_by = 262144;
SELECT number % 10000 AS k, count(), sum(number) FROM numbers(200000) GROUP BY k FORMAT Null;
SET max_bytes_before_external_group_by = 0;
SET max_bytes_before_external_sort = 262144;
SELECT number FROM numbers(200000) ORDER BY cityHash64(number) LIMIT 5;
SET max_bytes_before_external_sort = 0;
SELECT uniqExact(number) FROM numbers(300000);
SELECT sum(cityHash64(number)) FROM numbers(500000);

-- ============================================================================
-- 18. Table functions: numbers / generateRandom / values / format / view / merge
-- ============================================================================
SELECT count() FROM numbers(1000);
SELECT count() FROM numbers(100, 900);
SELECT sum(zero) FROM zeros(1000);
SELECT count(), uniqExact(i % 10) FROM (SELECT i FROM generateRandom('i UInt64, s String, a Array(Int8)', 42) LIMIT 500);
SELECT * FROM values('a Int32, b String', (1, 'x'), (2, 'y')) ORDER BY a;
SELECT * FROM format(JSONEachRow, '{"n": 1, "s": "one"}\n{"n": 2, "s": "two"}') ORDER BY n;
SELECT * FROM format(CSVWithNames, 'id,name\n1,alpha\n2,beta') ORDER BY id;
SELECT count() FROM view(SELECT number FROM numbers(10) WHERE number > 4);
SELECT count() FROM merge('corpus', '^m');
SELECT * FROM null('x Int32') LIMIT 0;
SELECT number FROM numbers(5) SETTINGS max_block_size = 2;

-- ============================================================================
-- 19. file() over MEMFS in many formats (runner pre-writes /corpus/*)
-- ============================================================================
SELECT * FROM file('/corpus/people.csv', CSV, 'id UInt32, name String, score Float64') ORDER BY id;
SELECT * FROM file('/corpus/people_names.csv', CSVWithNames) ORDER BY id;
SELECT * FROM file('/corpus/people.tsv', TSV, 'id UInt32, name String, score Float64') ORDER BY id;
SELECT * FROM file('/corpus/people.jsonl', JSONEachRow) ORDER BY id;
DESCRIBE file('/corpus/people_names.csv');
SELECT count() FROM file('/corpus/people*.csv', CSVWithNames);
INSERT INTO FUNCTION file('/corpus/out.parquet', Parquet) SELECT number AS id, toString(number) AS s, number / 3 AS f FROM numbers(1000);
SELECT count(), sum(id), max(f) FROM file('/corpus/out.parquet', Parquet);
SELECT id, s FROM file('/corpus/out.parquet') WHERE id % 100 = 0 ORDER BY id;
INSERT INTO FUNCTION file('/corpus/out.arrow', Arrow) SELECT number AS id, concat('r', toString(number)) AS s FROM numbers(500);
SELECT count(), any(s) FROM file('/corpus/out.arrow', Arrow);
INSERT INTO FUNCTION file('/corpus/out.native', Native) SELECT number AS n, map('k', number) AS m FROM numbers(100);
SELECT sum(n), any(m['k']) FROM file('/corpus/out.native', Native);
INSERT INTO FUNCTION file('/corpus/out.csv.gz', CSV) SELECT number, randomPrintableASCII(10) FROM numbers(200);
SELECT count() FROM file('/corpus/out.csv.gz', CSV, 'a UInt64, b String');
CREATE TABLE corpus.from_file ENGINE = Memory AS SELECT * FROM file('/corpus/people_names.csv');
SELECT count() FROM corpus.from_file;

-- ============================================================================
-- 20. Output formats (FORMAT clause exercises each serializer)
-- ============================================================================
SELECT number, toString(number) AS s, number / 2.0 AS f FROM numbers(5) FORMAT CSV;
SELECT number, toString(number) AS s FROM numbers(5) FORMAT CSVWithNames;
SELECT number, toString(number) AS s FROM numbers(5) FORMAT TSV;
SELECT number, toString(number) AS s FROM numbers(5) FORMAT TabSeparatedWithNamesAndTypes;
SELECT number, [number, number + 1] AS arr FROM numbers(3) FORMAT JSON;
SELECT number, map('k', number) AS m FROM numbers(3) FORMAT JSONEachRow;
SELECT number FROM numbers(3) FORMAT JSONCompact;
SELECT number FROM numbers(3) FORMAT JSONColumns;
SELECT number, toString(number) AS s FROM numbers(5) FORMAT Pretty;
SELECT number, toString(number) AS s FROM numbers(5) FORMAT PrettyCompact;
SELECT number, toString(number) AS s FROM numbers(5) FORMAT PrettySpace;
SELECT number, toString(number) AS s FROM numbers(2) FORMAT Vertical;
SELECT number FROM numbers(3) FORMAT Values;
SELECT number, toString(number) AS s FROM numbers(3) FORMAT Markdown;
SELECT number FROM numbers(3) FORMAT SQLInsert;
SELECT number FROM numbers(3) FORMAT XML;
SELECT number FROM numbers(3) FORMAT RowBinary;
SELECT number FROM numbers(3) FORMAT Parquet;
SELECT number FROM numbers(3) FORMAT Arrow;
SELECT number FROM numbers(3) FORMAT Native;
SELECT number FROM numbers(1000) FORMAT Null;

-- ============================================================================
-- 21. EXPLAIN / SHOW / system tables / settings
-- ============================================================================
EXPLAIN AST SELECT number FROM numbers(10) WHERE number > 5;
EXPLAIN SYNTAX SELECT number FROM numbers(10) WHERE number > 5 ORDER BY number;
EXPLAIN PLAN SELECT number % 3 AS k, count() FROM numbers(100) GROUP BY k;
EXPLAIN PIPELINE SELECT number % 3 AS k, count() FROM numbers(100) GROUP BY k;
EXPLAIN QUERY TREE SELECT sum(number) FROM numbers(10);
EXPLAIN ESTIMATE SELECT count() FROM corpus.mt;
SHOW DATABASES;
SHOW PROCESSLIST;
SELECT name, engine FROM system.tables WHERE database = 'corpus' ORDER BY name;
SELECT name, type FROM system.columns WHERE database = 'corpus' AND table = 'mt' ORDER BY position;
SELECT count() FROM system.functions;
SELECT count() FROM system.table_functions;
SELECT count() FROM system.table_engines;
SELECT count() FROM system.formats;
SELECT count() FROM system.data_type_families;
SELECT name, value FROM system.settings WHERE changed ORDER BY name LIMIT 20;
SELECT name, value FROM system.build_options WHERE name IN ('VERSION_FULL', 'SYSTEM') ORDER BY name;
SELECT * FROM system.one;
SELECT number FROM system.numbers LIMIT 5;
SET max_threads = 2;
SELECT sum(number) FROM numbers(100000);
SET max_threads = 1;

-- ============================================================================
-- 22. Deliberate error paths (keeps exception formatting/reporting hot)
-- ============================================================================
SELECT * FROM this_table_does_not_exist;
SELECT no_such_function(42);
SELECT toInt32('definitely not a number');
SELECT intDiv(1, 0);
SELECT arrayMap(x -> y, [1]);
SELECT FROM WHERE;
SELECT count() FROM file('/corpus/missing_file.csv', CSV, 'a Int32');
CREATE TABLE corpus.mem (id UInt32) ENGINE = Memory;
SELECT [1, 2] + 3;

-- ============================================================================
-- 23. url() via local fixture HTTP server (WasmHTTPBridge path)
-- ============================================================================
SELECT * FROM url('{HTTP}/people_names.csv', CSVWithNames) ORDER BY id;
SELECT count() FROM url('{HTTP}/people.jsonl', JSONEachRow);
DESCRIBE url('{HTTP}/people_names.csv');

-- ============================================================================
-- 24. Data lake: DataLakeCatalog + icebergS3 + deltaLake + Unity
--     (runner brings up moto S3, pyiceberg/delta-rs fixtures, mock catalogs)
-- ============================================================================
CREATE DATABASE lake ENGINE = DataLakeCatalog('{CATALOG}/v1', 'testing', 'testing') SETTINGS catalog_type = 'rest', warehouse = 'warehouse', storage_endpoint = '{S3}', vended_credentials = false;
SHOW TABLES FROM lake;
DESCRIBE TABLE lake.`lakehouse.events`;
SELECT * FROM lake.`lakehouse.events` ORDER BY id;
SELECT count(), sum(id), round(avg(score), 2) FROM lake.`lakehouse.events`;
SELECT city, count(), sum(amount) FROM lake.`lakehouse.city_events` GROUP BY city ORDER BY city;
SELECT * FROM lake.`lakehouse.deleted_events` ORDER BY id;
SELECT name, score FROM lake.`lakehouse.events` WHERE id > 3 ORDER BY score DESC LIMIT 3;
DROP DATABASE lake;
DESCRIBE icebergS3('{S3}/lakebucket/warehouse/lakehouse/events', 'testing', 'testing');
SELECT count(), max(id) FROM icebergS3('{S3}/lakebucket/warehouse/lakehouse/events', 'testing', 'testing');
SELECT id, name FROM icebergS3('{S3}/lakebucket/warehouse/lakehouse/events', 'testing', 'testing') WHERE score > 3 ORDER BY id;
CREATE DATABASE unity ENGINE = DataLakeCatalog('{UNITY}', 'testing', 'testing') SETTINGS catalog_type = 'unity', warehouse = 'unity', storage_endpoint = '{S3}', vended_credentials = false;
SHOW TABLES FROM unity;
SELECT id, item, price FROM unity.`lakeschema.sales` ORDER BY id;
SELECT count(), max(price) FROM unity.`lakeschema.sales`;
DROP DATABASE unity;
DESCRIBE deltaLake('{S3}/deltabucket/unity/sales', 'testing', 'testing');
SELECT count(), round(sum(price), 2) FROM deltaLake('{S3}/deltabucket/unity/sales', 'testing', 'testing');
SELECT id, item FROM deltaLake('{S3}/deltabucket/unity/sales', 'testing', 'testing') WHERE price > 10 ORDER BY id;

-- ============================================================================
-- 26. Probes for functions NOT registered in the wasm build (each fails alone,
--     keeping UNKNOWN_FUNCTION handling hot without poisoning other lines).
--     If a future build adds one, it graduates into the sections above.
-- ============================================================================
SELECT toInt128(5), toInt256(6);
SELECT toUInt128(5), toUInt256(6);
SELECT toDecimal128(9.87, 2);
SELECT intDivOrZero(7, 0);
SELECT bitShiftLeft(1, 4), bitShiftRight(256, 4), bitRotateLeft(1, 33);
SELECT avgWeighted(number, number % 3 + 1) FROM numbers(100);
SELECT anyHeavy(number % 5) FROM numbers(100);
SELECT uniqUpTo(5)(number % 100) FROM numbers(1000);
SELECT quantileTDigest(0.95)(number) FROM numbers(1000);
SELECT groupArraySorted(5)(10 - number) FROM numbers(10);
SELECT groupArrayMovingSum(3)(number), groupArrayMovingAvg(3)(number) FROM numbers(10);
SELECT groupBitAnd(number), groupBitOr(number), groupBitXor(number) FROM numbers(1, 100);
SELECT groupBitmap(toUInt32(number % 50)) FROM numbers(1000);
SELECT entropy(number % 10) FROM numbers(100);
SELECT histogram(5)(toFloat64(number % 100)) FROM numbers(1000);
SELECT simpleLinearRegression(toFloat64(number), toFloat64(2 * number + 1)) FROM numbers(100);
SELECT mannWhitneyUTest(toFloat64(number % 17), number % 2) FROM numbers(200);
SELECT studentTTest(toFloat64(number % 13), number % 2) FROM numbers(200);
SELECT rankCorr(toFloat64(number % 7), toFloat64(number % 11)) FROM numbers(200);
SELECT cramersV(number % 3, number % 5) FROM numbers(1000);
SELECT sequenceMatch('(?1)(?2)')(toDateTime(number), number % 3 = 0, number % 3 = 1) FROM numbers(100);
SELECT retention(number % 7 = 0, number % 7 = 1) FROM numbers(49);
SELECT exponentialMovingAverage(5)(toFloat64(number), number) FROM numbers(50);
SELECT ascii('A');
SELECT lowerUTF8('ÄÖÜ'), upperUTF8('äöü');
SELECT soundex('Robert');
SELECT formatReadableQuantity(123456789), formatReadableDecimalSize(123456789);
SELECT base64Encode('chdb'), base64Decode('Y2hkYg==');
SELECT parseDateTimeBestEffortOrNull('garbage');
SELECT gcd(12, 18), lcm(4, 6);
SELECT expm1(0.5);
SELECT arrayCumSum([1, 2, 3]);
SELECT arrayShuffle([1, 2, 3]);
SELECT arrayRotateLeft([1, 2, 3, 4], 1), arrayShiftRight([1, 2, 3, 4], 1, 0);
SELECT arrayFold((acc, x) -> acc + x, [1, 2, 3], toInt64(0));
SELECT arrayAUC([0.1, 0.4, 0.35, 0.8], [0, 0, 1, 1]);
SELECT mapApply((k, v) -> (upper(k), v * 2), map('a', 1));
SELECT JSONExtractKeys('{"a": 1, "b": 2}');
SELECT hex(sipHash128('chdb')), hex(murmurHash3_128('chdb'));
SELECT xxHash32('chdb');
SELECT murmurHash2_64('chdb'), murmurHash3_32('chdb');
SELECT pointInPolygon((1.5, 1.5), [(0, 0), (3, 0), (3, 3), (0, 3)]);
SELECT tupleToNameValuePairs(CAST((42, 3.5), 'Tuple(i Int32, f Float64)'));

-- ============================================================================
-- 25. Session/stream API coverage happens in the runner itself (streaming
--     fetch, cancel, progress) — no SQL needed here.
-- ============================================================================
SELECT 'corpus done';
