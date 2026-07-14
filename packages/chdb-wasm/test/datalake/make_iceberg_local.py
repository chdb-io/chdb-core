#!/usr/bin/env python3
"""Create a real Iceberg table on the local filesystem with pyiceberg.

Usage: make_iceberg_local.py <warehouse_dir>

Writes namespace `localns`, table `items` (2 snapshots) under <warehouse_dir>
via a local SqlCatalog (sqlite), then prints {"table_dir": ...} on stdout —
the table directory to mirror into the wasm MEMFS for icebergLocal().
"""
import json
import sys
import tempfile

import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog


def main() -> None:
    warehouse_dir = sys.argv[1]

    tmp = tempfile.mkdtemp(prefix="iceberg-sqlcat-")
    catalog = SqlCatalog(
        "local",
        uri=f"sqlite:///{tmp}/catalog.db",
        warehouse=f"file://{warehouse_dir}",
    )

    catalog.create_namespace("localns")
    data = pa.table(
        {
            "k": pa.array([10, 20, 30], type=pa.int64()),
            "v": pa.array(["ten", "twenty", "thirty"]),
        }
    )
    table = catalog.create_table("localns.items", schema=data.schema)
    table.append(data)
    table.append(pa.table({"k": pa.array([40], type=pa.int64()), "v": pa.array(["forty"])}))

    # file:///path/... -> /path/...
    table_dir = table.location()
    if table_dir.startswith("file://"):
        table_dir = table_dir[len("file://") :]

    print(json.dumps({"table_dir": table_dir}))


if __name__ == "__main__":
    main()
