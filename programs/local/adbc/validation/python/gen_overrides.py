#!/usr/bin/env python3
"""Generate chdb-dialect txtcase overrides for the validation suite.

For each framework-default case that fails on chDB purely for dialect
reasons, this script rewrites the setup DDL into ClickHouse syntax
(Nullable columns — ClickHouse columns are non-nullable by default),
replays the case through the driver, and records the actual round-trip
schema/values as the override's expected parts. Run from this directory:

    CHDB_ADBC_DRIVER=/path/to/libchdb.so python gen_overrides.py
"""

import base64
import math
import os
import pathlib
import re
import sys

import pyarrow as pa
import adbc_driver_manager.dbapi as dbapi
from adbc_drivers_validation import arrowjson

HERE = pathlib.Path(__file__).parent
import adbc_drivers_validation

SITE_QUERIES = pathlib.Path(adbc_drivers_validation.__file__).parent / "queries"
OUT_QUERIES = HERE / "queries"

BIND_CASES = [
    "binary", "binary_view", "boolean", "date", "decimal", "fixed_size_binary",
    "float16", "float32", "float64", "int16", "int32", "int64",
    "large_binary", "large_string", "string", "string_view",
    "time_ms", "time_ns", "time_s", "time_us",
    "timestamp_ms", "timestamp_ns", "timestamp_s", "timestamp_us",
    "timestamptz_ms", "timestamptz_ns", "timestamptz_s", "timestamptz_us",
]
INGEST_CASES = [
    "binary", "binary_view", "decimal_scale_negative", "fixed_size_binary",
    "large_binary",
    "time_ms", "time_ns", "time_s", "time_us",
    "timestamp_ms", "timestamp_ns", "timestamp_s", "timestamp_us",
    "timestamptz_ms", "timestamptz_ns", "timestamptz_s", "timestamptz_us",
]

# SQL-standard DDL types -> ClickHouse (wrapped in Nullable by the caller).
# Longest-match first.
DDL_TYPES = [
    (re.compile(r"TIMESTAMP\((\d+)\) WITH TIME ZONE"), r"DateTime64(\1, 'UTC')"),
    (re.compile(r"TIMESTAMP\((\d+)\)"), r"DateTime64(\1)"),
    (re.compile(r"TIME\((\d+)\)"), r"DateTime64(\1)"),
    (re.compile(r"DECIMAL\((\d+),\s*(-?\d+)\)"), r"Decimal(\1, \2)"),
    (re.compile(r"DOUBLE PRECISION"), "Float64"),
    (re.compile(r"\bDOUBLE\b"), "Float64"),
    (re.compile(r"\bREAL\b"), "Float32"),
    (re.compile(r"\bFLOAT\b"), "Float64"),
    (re.compile(r"\bHALF_FLOAT\b"), "Float32"),
    (re.compile(r"\bBIGINT\b"), "Int64"),
    (re.compile(r"\bSMALLINT\b"), "Int16"),
    (re.compile(r"\bINTEGER\b"), "Int32"),
    (re.compile(r"\bINT\b"), "Int32"),
    (re.compile(r"\bBOOLEAN\b"), "Bool"),
    (re.compile(r"\bDATE\b"), "Date32"),
    (re.compile(r"VARBINARY\((\d+)\)"), "String"),
    (re.compile(r"BINARY\((\d+)\)"), r"FixedString(\1)"),
    (re.compile(r"\bVARBINARY\b"), "String"),
    (re.compile(r"\bBINARY\b"), "String"),
    (re.compile(r"\bBLOB\b"), "String"),
    (re.compile(r"VARCHAR\((\d+)\)"), "String"),
    (re.compile(r"\bVARCHAR\b"), "String"),
    (re.compile(r"\bTEXT\b"), "String"),
]


def parse_parts(text):
    header, order, parts, cur = [], [], {}, None
    for line in text.splitlines():
        m = re.match(r"^// part: (\w+)\s*$", line)
        if m:
            cur = m.group(1)
            parts[cur] = []
            order.append(cur)
        elif cur is None:
            header.append(line)
        else:
            parts[cur].append(line)
    return (
        "\n".join(header).strip("\n"),
        order,
        {k: "\n".join(v).strip("\n") for k, v in parts.items()},
    )


def render(header, order, parts):
    out = [header, ""]
    for name in order:
        out.append(f"// part: {name}")
        out.append("")
        out.append(parts[name])
        out.append("")
    return "\n".join(out).rstrip("\n") + "\n"


def translate_ddl(ddl):
    # Column lines look like "    res TYPE," — wrap the mapped type in
    # Nullable and keep everything else. Applies per line, columns only.
    out_lines = []
    for line in ddl.splitlines():
        new = line
        for rx, repl in DDL_TYPES:
            m = rx.search(new)
            if m:
                mapped = rx.sub(repl, m.group(0))
                new = new[: m.start()] + "Nullable(" + mapped + ")" + new[m.end():]
                break
        out_lines.append(new)
    ddl = "\n".join(out_lines)
    if re.match(r"\s*CREATE TABLE", ddl) and "ENGINE" not in ddl:
        ddl = ddl.rstrip().rstrip(";") + ";"
    return ddl


FMT = None


def fmt_of(t):
    if pa.types.is_int64(t):
        return "l"
    if pa.types.is_int32(t):
        return "i"
    if pa.types.is_int16(t):
        return "s"
    if pa.types.is_int8(t):
        return "c"
    if pa.types.is_uint32(t):
        return "I"
    if pa.types.is_uint64(t):
        return "L"
    if pa.types.is_uint16(t):
        return "S"
    if pa.types.is_uint8(t):
        return "C"
    if pa.types.is_boolean(t):
        return "b"
    if pa.types.is_float16(t):
        return "e"
    if pa.types.is_float32(t):
        return "f"
    if pa.types.is_float64(t):
        return "g"
    if pa.types.is_string(t):
        return "u"
    if pa.types.is_large_string(t):
        return "U"
    if pa.types.is_binary(t):
        return "z"
    if pa.types.is_large_binary(t):
        return "Z"
    if pa.types.is_fixed_size_binary(t):
        return f"w:{t.byte_width}"
    if pa.types.is_date32(t):
        return "tdD"
    if pa.types.is_date64(t):
        return "tdm"
    if pa.types.is_time32(t):
        return "tts" if t.unit == "s" else "ttm"
    if pa.types.is_time64(t):
        return "ttu" if t.unit == "us" else "ttn"
    if pa.types.is_timestamp(t):
        unit = {"s": "s", "ms": "m", "us": "u", "ns": "n"}[t.unit]
        return f"ts{unit}:{t.tz or ''}"
    if pa.types.is_decimal(t):
        return f"d:{t.precision},{t.scale}"
    raise NotImplementedError(f"no format string for {t}")


def schema_json(schema):
    children = []
    for f in schema:
        entry = f'        {{\n            "name": "{f.name}",\n            "format": "{fmt_of(f.type)}"'
        if f.nullable:
            entry += ',\n            "flags": ["nullable"]'
        entry += "\n        }"
        children.append(entry)
    return '{\n    "format": "+s",\n    "children": [\n' + ",\n".join(children) + "\n    ]\n}"


def cell_json(value, t):
    import json

    if value is None:
        return "null"
    if pa.types.is_binary(t) or pa.types.is_large_binary(t) or pa.types.is_fixed_size_binary(t) or pa.types.is_binary_view(t):
        return json.dumps(base64.b64encode(value).decode())
    if pa.types.is_decimal(t):
        return json.dumps(str(value))
    if pa.types.is_timestamp(t) or pa.types.is_time32(t) or pa.types.is_time64(t) or pa.types.is_date32(t) or pa.types.is_date64(t):
        return "null" if value is None else str(value)
    if pa.types.is_float32(t) or pa.types.is_float64(t):
        if math.isnan(value):
            return '"NaN"'
        if math.isinf(value):
            return '"Inf"' if value > 0 else '"-Inf"'
        return json.dumps(value)
    return json.dumps(value)


def table_jsonlines(table):
    lines = []
    cols = []
    for i, f in enumerate(table.schema):
        col = table.column(i)
        if pa.types.is_date32(f.type) or pa.types.is_time32(f.type):
            vals = col.cast(pa.int32()).to_pylist()
        elif pa.types.is_timestamp(f.type) or pa.types.is_time64(f.type) or pa.types.is_date64(f.type):
            vals = col.cast(pa.int64()).to_pylist()
        else:
            vals = col.to_pylist()
        cols.append(vals)
    for r in range(table.num_rows):
        cells = []
        for i, f in enumerate(table.schema):
            cells.append(f'"{f.name}": {cell_json(cols[i][r], f.type)}')
        lines.append("{" + ", ".join(cells) + "}")
    return "\n".join(lines)


def sanitize_binary(table):
    """chDB stores Arrow binary as String and reads it back as utf8, so the
    payloads must be UTF-8-representable to round-trip through the case
    format. Non-UTF-8 bytes are swapped for same-length ASCII markers."""
    changed = False
    cols = []
    for i, f in enumerate(table.schema):
        col = table.column(i)
        if pa.types.is_binary(f.type) or pa.types.is_large_binary(f.type) \
                or pa.types.is_fixed_size_binary(f.type) or pa.types.is_binary_view(f.type):
            vals = []
            for r, v in enumerate(col.to_pylist()):
                if v is None:
                    vals.append(None)
                    continue
                try:
                    v.decode("utf-8")
                    vals.append(v)
                except UnicodeDecodeError:
                    marker = f"bin-{r:02d}-".encode()
                    width = len(v)
                    payload = (marker * (width // len(marker) + 1))[:width]
                    vals.append(payload)
                    changed = True
            cols.append(pa.chunked_array([pa.array(vals, type=f.type)]))
        else:
            cols.append(col)
    if not changed:
        return table, False
    return pa.Table.from_arrays(cols, schema=table.schema), True


def sanitize_dates(table):
    """ClickHouse Date32 covers 1900-01-01..2299-12-31 (days -25567..120529);
    out-of-range case values are clamped to the boundaries."""
    changed = False
    cols = []
    for i, f in enumerate(table.schema):
        col = table.column(i)
        if pa.types.is_date32(f.type):
            vals = []
            for v in col.cast(pa.int32()).to_pylist():
                if v is None:
                    vals.append(None)
                elif v < -25567:
                    vals.append(-25567)
                    changed = True
                elif v > 120529:
                    vals.append(120529)
                    changed = True
                else:
                    vals.append(v)
            cols.append(pa.chunked_array([pa.array(vals, pa.int32()).cast(f.type)]))
        else:
            cols.append(col)
    if not changed:
        return table, False
    return pa.Table.from_arrays(cols, schema=table.schema), True


def read_all(cursor):
    handle, _ = cursor.adbc_statement.execute_query()
    return pa.RecordBatchReader._import_from_c(handle.address).read_all()


def run_sql(cursor, sql):
    cursor.adbc_statement.set_sql_query(sql)
    cursor.adbc_statement.execute_update()


def main():
    lib = (
        os.environ.get("CHDB_ADBC_DRIVER")
        or os.environ.get("CHDB_LIB_PATH")
        or "libchdb.so"
    )
    conn = dbapi.connect(driver=lib, autocommit=True)
    report = []

    for name in BIND_CASES:
        src = SITE_QUERIES / "type" / "bind" / f"{name}.txtcase"
        header, order, parts = parse_parts(src.read_text())
        drop = re.search(r'drop\s*=\s*"([^"]+)"', parts.get("metadata", ""))
        table_name = drop.group(1) if drop else None
        new_ddl = translate_ddl(parts["setup_query"])
        parts["setup_query"] = new_ddl
        try:
            with conn.cursor() as cur:
                if table_name:
                    run_sql(cur, f"DROP TABLE IF EXISTS `{table_name}`")
                for stmt in [s.strip() for s in new_ddl.split(";") if s.strip()]:
                    run_sql(cur, stmt)
                bind_schema = arrowjson.loads_schema(parts["bind_schema"])
                data = arrowjson.loads_table(parts["bind"], bind_schema)
                data, changed = sanitize_binary(data)
                data, changed_dates = sanitize_dates(data)
                if changed or changed_dates:
                    parts["bind"] = table_jsonlines(data)
                batch = data.combine_chunks().to_batches()[0]
                bq = re.sub(r"\$(\d+)", "?", parts["bind_query"].strip())
                cur.adbc_statement.set_sql_query(bq)
                cur.adbc_statement.bind(batch)
                cur.adbc_statement.execute_update()
                cur.adbc_statement.set_sql_query(parts["query"].strip())
                actual = read_all(cur)
        except Exception as e:  # noqa: BLE001
            report.append(f"bind/{name}: ERROR {e}")
            continue

        expected_schema = arrowjson.loads_schema(parts["expected_schema"]) if "expected_schema" in parts else bind_schema
        if actual.schema != expected_schema:
            parts["expected_schema"] = schema_json(actual.schema)
            if "expected_schema" not in order:
                order.append("expected_schema")
        parts["expected"] = table_jsonlines(actual)
        if "expected" not in order:
            order.append("expected")
        dst = OUT_QUERIES / "type" / "bind" / f"{name}.txtcase"
        dst.parent.mkdir(parents=True, exist_ok=True)
        dst.write_text(render(header, order, parts))
        report.append(f"bind/{name}: OK schema={actual.schema.types}")

    for name in INGEST_CASES:
        src = SITE_QUERIES / "ingest" / f"{name}.txtcase"
        header, order, parts = parse_parts(src.read_text())
        try:
            input_schema = arrowjson.loads_schema(parts["input_schema"])
            data = arrowjson.loads_table(parts["input"], input_schema)
            data, changed = sanitize_binary(data)
            if changed:
                parts["input"] = table_jsonlines(data)
            tname = f"gen_ingest_{name}"
            with conn.cursor() as cur:
                run_sql(cur, f"DROP TABLE IF EXISTS `{tname}`")
                cur.adbc_ingest(tname, data, mode="create")
                cur.adbc_statement.set_sql_query(
                    f"SELECT * FROM `{tname}` ORDER BY idx"
                )
                actual = read_all(cur)
        except Exception as e:  # noqa: BLE001
            report.append(f"ingest/{name}: ERROR {e}")
            continue

        parts["expected_schema"] = schema_json(actual.schema)
        parts["expected"] = table_jsonlines(actual)
        for extra in ("expected_schema", "expected"):
            if extra not in order:
                order.append(extra)
        dst = OUT_QUERIES / "ingest" / f"{name}.txtcase"
        dst.parent.mkdir(parents=True, exist_ok=True)
        dst.write_text(render(header, order, parts))
        report.append(f"ingest/{name}: OK schema={actual.schema.types}")

    conn.close()
    print("\n".join(report))


if __name__ == "__main__":
    sys.exit(main())
