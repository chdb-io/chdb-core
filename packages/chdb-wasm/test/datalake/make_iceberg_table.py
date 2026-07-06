#!/usr/bin/env python3
"""Create a real Iceberg table on an S3-compatible endpoint (moto) with pyiceberg.

Usage: make_iceberg_table.py <s3_endpoint> <bucket>

Writes namespace `lakehouse`, table `events` (3 columns, 5 rows) under
s3://<bucket>/warehouse/ via a local SqlCatalog (sqlite), then prints a JSON
descriptor {"metadata_location": ..., "namespace": ..., "table": ...} on stdout
for the mock Iceberg REST catalog to serve.
"""
import json
import sys
import tempfile

import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog


def main() -> None:
    endpoint, bucket = sys.argv[1], sys.argv[2]

    tmp = tempfile.mkdtemp(prefix="iceberg-sqlcat-")
    catalog = SqlCatalog(
        "local",
        uri=f"sqlite:///{tmp}/catalog.db",
        warehouse=f"s3://{bucket}/warehouse",
        **{
            "s3.endpoint": endpoint,
            "s3.access-key-id": "testing",
            "s3.secret-access-key": "testing",
            "s3.region": "us-east-1",
        },
    )

    catalog.create_namespace("lakehouse")
    data = pa.table(
        {
            "id": pa.array([1, 2, 3, 4, 5], type=pa.int64()),
            "name": pa.array(["alpha", "beta", "gamma", "delta", "epsilon"]),
            "score": pa.array([1.5, 2.5, 3.5, 4.5, 5.5], type=pa.float64()),
        }
    )
    table = catalog.create_table("lakehouse.events", schema=data.schema)
    table.append(data)

    # Second snapshot so manifest-list handling beyond the trivial case is exercised.
    more = pa.table(
        {
            "id": pa.array([6, 7], type=pa.int64()),
            "name": pa.array(["zeta", "eta"]),
            "score": pa.array([6.5, 7.5], type=pa.float64()),
        }
    )
    table.append(more)

    # The exact on-disk metadata JSON, for the mock REST catalog's LoadTableResult.
    with table.io.new_input(table.metadata_location).open() as f:
        metadata_json = json.loads(f.read().decode("utf-8"))

    print(
        json.dumps(
            {
                "metadata_location": table.metadata_location,
                "namespace": "lakehouse",
                "table": "events",
                "location": table.location(),
                "metadata": metadata_json,
            }
        )
    )


if __name__ == "__main__":
    main()
