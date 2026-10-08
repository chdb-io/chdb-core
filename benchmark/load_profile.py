#!/usr/bin/env python3
"""Profile how a bulk load into a chDB MergeTree table turns into parts and merges.

Runs CREATE + one load statement (optionally OPTIMIZE ... FINAL) on a fresh
database path with part_log and query_log enabled, then reports:

  * width of the INSERT pipeline (parallel MergeTree sinks);
  * parts written by each INSERT: count, rows and compressed size distribution;
  * every background merge: start, duration, inputs, output, bytes read and
    written, CPU time, compression time;
  * active / outdated parts per partition after load, after merges settle,
    after FINAL, right before close and after reopening;
  * where INSERT and FINAL spent CPU: sorting, compression, everything else;
  * on-disk size of the database directory after close;
  * the engine's shutdown summary of abandoned merge work.

Example (ClickBench):

  python benchmark/load_profile.py --path /tmp/cb --create create.sql \\
      --load "INSERT INTO hits SELECT * FROM file('hits.parquet')" \\
      --table hits --final --out cb-profile.json
"""

import argparse
import json
import os
import shutil
import statistics
import sys
import tempfile
import time
import uuid

from chdb.state.sqlitelike import connect

CONFIG = """<clickhouse>
    <logger>
        <log>{log_path}</log>
        <level>information</level>
        <async>false</async>
    </logger>
    <part_log>
        <database>system</database>
        <table>part_log</table>
        <flush_interval_milliseconds>1000</flush_interval_milliseconds>
    </part_log>
    <query_log>
        <database>system</database>
        <table>query_log</table>
        <flush_interval_milliseconds>1000</flush_interval_milliseconds>
    </query_log>
</clickhouse>
"""

# ProfileEvents that split INSERT / merge time into sorting, compression and the rest.
EVENTS = [
    "RealTimeMicroseconds",
    "UserTimeMicroseconds",
    "SystemTimeMicroseconds",
    "OSReadBytes",
    "OSWriteBytes",
    "MergeTreeDataWriterBlocks",
    "MergeTreeDataWriterRows",
    "MergeTreeDataWriterUncompressedBytes",
    "MergeTreeDataWriterCompressedBytes",
    "MergeTreeDataWriterSortingBlocksMicroseconds",
    "MergeTreeDataWriterMergingBlocksMicroseconds",
    "MergeTreeDataWriterStatisticsCalculationMicroseconds",
    "MergeTreeDataWriterSkipIndicesCalculationMicroseconds",
    "CompressedWriteBufferBytes",
    "CompressedWriteBufferCompressMicroseconds",
    "MergeHorizontalStageExecuteMilliseconds",
    "MergeVerticalStageExecuteMilliseconds",
    "MergeTotalMilliseconds",
]


def events_expr(column="ProfileEvents"):
    names = ", ".join(f"'{e}'" for e in EVENTS)
    return f"mapFilter((k, v) -> k IN ({names}), {column})"


def query_json(conn, sql):
    out = str(conn.query(sql, "JSONEachRow")).strip()
    return [json.loads(line) for line in out.splitlines()] if out else []


def distribution(values):
    if not values:
        return {"count": 0}
    values = sorted(values)
    return {
        "count": len(values),
        "min": values[0],
        "median": statistics.median(values),
        "max": values[-1],
        "sum": sum(values),
    }


def parts_snapshot(conn, table_filter):
    """Active and outdated parts, overall and per partition."""
    parts = query_json(
        conn,
        f"SELECT database, table, partition_id, name, active, rows, bytes_on_disk, level "
        f"FROM system.parts WHERE {table_filter} ORDER BY database, table, partition_id, name",
    )
    active = [p for p in parts if p["active"]]
    per_partition = {}
    for p in active:
        key = f"{p['database']}.{p['table']}/{p['partition_id']}"
        per_partition.setdefault(key, []).append(int(p["bytes_on_disk"]))
    return {
        "active_parts": len(active),
        "inactive_parts": len(parts) - len(active),
        "active_bytes": distribution([int(p["bytes_on_disk"]) for p in active]),
        "active_rows": distribution([int(p["rows"]) for p in active]),
        "active_levels": distribution([int(p["level"]) for p in active]),
        "partitions": {k: distribution(v) for k, v in sorted(per_partition.items())},
    }


def pipeline_sinks(conn, load_sql):
    """Number of parallel MergeTree sinks in the INSERT pipeline (EXPLAIN PIPELINE)."""
    plan = str(conn.query(f"EXPLAIN PIPELINE {load_sql}", "TSVRaw"))
    width = 0
    for line in plan.splitlines():
        line = line.strip()
        if "MergeTreeSink" not in line:
            continue
        parts = line.split("×")
        width += int(parts[1].split()[0]) if len(parts) > 1 else 1
    return {"merge_tree_sinks": width, "explain_pipeline": plan}


def wait_for_merges(conn, table_filter, timeout):
    start = time.monotonic()
    while time.monotonic() - start < timeout:
        running = query_json(conn, f"SELECT count() AS n FROM system.merges WHERE {table_filter}")[0]["n"]
        if int(running) == 0:
            break
        time.sleep(0.5)
    return round(time.monotonic() - start, 3)


def directory_bytes(path):
    total = 0
    for root, _, files in os.walk(path):
        for name in files:
            try:
                total += os.lstat(os.path.join(root, name)).st_size
            except FileNotFoundError:
                pass
    return total


def collect_logs(conn, table_filter, query_ids):
    ids = ", ".join(f"'{q}'" for q in query_ids.values())
    queries = query_json(
        conn,
        f"SELECT query_id, query_duration_ms, memory_usage, written_rows, written_bytes, "
        f"{events_expr()} AS events "
        f"FROM system.query_log WHERE type = 'QueryFinish' AND query_id IN ({ids})",
    )
    by_id = {q["query_id"]: q for q in queries}

    new_parts = query_json(
        conn,
        f"SELECT query_id, partition_id, rows, size_in_bytes, bytes_uncompressed "
        f"FROM system.part_log WHERE event_type = 'NewPart' AND {table_filter} "
        f"ORDER BY event_time_microseconds",
    )
    inserts = {}
    for p in new_parts:
        inserts.setdefault(p["query_id"], []).append(p)

    merges = query_json(
        conn,
        f"SELECT toString(event_time_microseconds - toIntervalMillisecond(duration_ms)) AS start, "
        f"duration_ms, merge_reason, merge_algorithm, partition_id, part_name, "
        f"length(merged_from) AS inputs, rows, size_in_bytes, bytes_uncompressed, read_rows, read_bytes, "
        f"peak_memory_usage, error, exception, {events_expr()} AS events "
        f"FROM system.part_log WHERE event_type = 'MergeParts' AND {table_filter} "
        f"ORDER BY event_time_microseconds",
    )
    return by_id, inserts, merges


def summarize_events(rows):
    total = {}
    for row in rows:
        for k, v in row["events"].items():
            total[k] = total.get(k, 0) + int(v)
    return total


def run(args):
    workdir = tempfile.mkdtemp(prefix="chdb-load-profile-")
    log_path = os.path.join(workdir, "chdb.log")
    config_path = os.path.join(workdir, "config.xml")
    with open(config_path, "w") as f:
        f.write(CONFIG.format(log_path=log_path))

    if os.path.exists(args.path):
        if not args.overwrite:
            sys.exit(f"{args.path} exists; pass --overwrite to replace it")
        shutil.rmtree(args.path)

    table_filter = "database NOT IN ('system', 'INFORMATION_SCHEMA', 'information_schema')"
    if args.table:
        table_filter += f" AND table = '{args.table}'"

    report = {"path": args.path, "load": args.load, "settings": args.settings, "stages": {}, "timings_s": {}}
    query_ids = {}
    suffix = f" SETTINGS {args.settings}" if args.settings else ""

    conn = connect(f"{args.path}?config-file={config_path}")
    try:
        with open(args.create) as f:
            for statement in filter(str.strip, f.read().split(";")):
                conn.query(statement)

        report["pipeline"] = pipeline_sinks(conn, args.load + suffix)

        query_ids["load"] = f"load_profile_load_{uuid.uuid4().hex}"
        start = time.monotonic()
        conn.query(f"/* {query_ids['load']} */ {args.load}{suffix}")
        report["timings_s"]["load"] = round(time.monotonic() - start, 3)
        report["stages"]["after_load"] = parts_snapshot(conn, table_filter)

        if args.settle_seconds > 0:
            report["timings_s"]["settle"] = wait_for_merges(conn, table_filter, args.settle_seconds)
            report["stages"]["after_settle"] = parts_snapshot(conn, table_filter)

        if args.final:
            if not args.table:
                sys.exit("--final needs --table")
            query_ids["final"] = f"load_profile_final_{uuid.uuid4().hex}"
            start = time.monotonic()
            conn.query(f"/* {query_ids['final']} */ OPTIMIZE TABLE {args.table} FINAL")
            report["timings_s"]["final"] = round(time.monotonic() - start, 3)
            report["stages"]["after_final"] = parts_snapshot(conn, table_filter)

        conn.query("SYSTEM FLUSH LOGS")
        # Queries are matched by the comment carrying their marker; resolve the real query_id.
        resolved = {}
        for stage, marker in query_ids.items():
            hit = query_json(
                conn,
                f"SELECT query_id FROM system.query_log WHERE type = 'QueryFinish' "
                f"AND query LIKE '%{marker}%' AND query NOT LIKE '%system.query_log%' LIMIT 1",
            )
            if hit:
                resolved[stage] = hit[0]["query_id"]

        queries, inserts, merges = collect_logs(conn, table_filter, resolved)
        report["queries"] = {stage: queries.get(qid) for stage, qid in resolved.items()}
        report["inserts"] = {
            stage: {
                "parts": len(inserts.get(qid, [])),
                "rows": distribution([int(p["rows"]) for p in inserts.get(qid, [])]),
                "bytes": distribution([int(p["size_in_bytes"]) for p in inserts.get(qid, [])]),
                "partitions": len({p["partition_id"] for p in inserts.get(qid, [])}),
            }
            for stage, qid in resolved.items()
            if stage == "load"
        }
        report["merges"] = {
            "count": len(merges),
            "failed": sum(1 for m in merges if int(m["error"]) != 0),
            "input_parts": sum(int(m["inputs"]) for m in merges),
            "read_bytes": sum(int(m["read_bytes"]) for m in merges),
            "written_bytes": sum(int(m["size_in_bytes"]) for m in merges),
            "events": summarize_events(merges),
            "timeline": merges,
        }
        report["stages"]["before_close"] = parts_snapshot(conn, table_filter)
    finally:
        conn.close()

    report["data_bytes_after_close"] = directory_bytes(args.path)
    with open(log_path) as f:
        report["shutdown_log"] = [line.rstrip() for line in f if "Shutting down" in line and "active parts" in line]

    conn = connect(args.path)
    try:
        report["stages"]["after_reopen"] = parts_snapshot(conn, table_filter)
    finally:
        conn.close()
    report["data_bytes_after_reopen"] = directory_bytes(args.path)

    shutil.rmtree(workdir, ignore_errors=True)
    return report


def print_summary(report):
    print(f"load: {report['timings_s'].get('load')} s, final: {report['timings_s'].get('final', '-')} s")
    print(f"INSERT pipeline MergeTree sinks: {report['pipeline']['merge_tree_sinks']}")
    for stage, ins in report["inserts"].items():
        print(f"{stage}: {ins['parts']} new parts in {ins['partitions']} partitions, "
              f"bytes {ins['bytes']}")
    m = report["merges"]
    print(f"merges: {m['count']} ({m['failed']} failed), {m['input_parts']} input parts, "
          f"read {m['read_bytes']} B, wrote {m['written_bytes']} B")
    for stage, snap in report["stages"].items():
        print(f"{stage:>13}: {snap['active_parts']} active / {snap['inactive_parts']} inactive parts")
    for stage, q in report["queries"].items():
        if q:
            ev = q["events"]
            print(f"{stage} CPU: user {ev.get('UserTimeMicroseconds', 0)} us, "
                  f"sort {ev.get('MergeTreeDataWriterSortingBlocksMicroseconds', 0)} us, "
                  f"compress {ev.get('CompressedWriteBufferCompressMicroseconds', 0)} us")
    print(f"data bytes after close: {report['data_bytes_after_close']}, after reopen: {report['data_bytes_after_reopen']}")
    for line in report["shutdown_log"]:
        print(line)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--path", required=True, help="database directory to create")
    parser.add_argument("--create", required=True, help="SQL file with the CREATE statements")
    parser.add_argument("--load", required=True, help="load statement, e.g. INSERT INTO t SELECT ...")
    parser.add_argument("--table", help="restrict the report to this table; required by --final")
    parser.add_argument("--settings", default="", help="SETTINGS appended to the load statement")
    parser.add_argument("--final", action="store_true", help="run OPTIMIZE TABLE ... FINAL after the load")
    parser.add_argument("--settle-seconds", type=float, default=0,
                        help="after the load, wait up to this long for running merges to finish")
    parser.add_argument("--overwrite", action="store_true", help="remove --path first if it exists")
    parser.add_argument("--out", help="write the full report as JSON")
    args = parser.parse_args()

    report = run(args)
    print_summary(report)
    if args.out:
        with open(args.out, "w") as f:
            json.dump(report, f, indent=2)


if __name__ == "__main__":
    main()
