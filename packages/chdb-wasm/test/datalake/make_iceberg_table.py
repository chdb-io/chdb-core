#!/usr/bin/env python3
"""Create real Iceberg tables on an S3-compatible endpoint with pyiceberg.

Usage: make_iceberg_table.py <s3_endpoint> <bucket> [access_key secret_key]

Writes namespace `lakehouse` with three tables under s3://<bucket>/warehouse/:
  - events:        3 columns, 2 snapshots (5 + 2 rows) — the basic read path
  - city_events:   identity-partitioned by `city` with values containing spaces
                   ("New York") — object keys with characters needing URL encoding
  - deleted_events: format-version 2, rows deleted after the initial append
                   (5 rows written, 2 deleted). NOTE: pyiceberg falls back to
                   copy-on-write ("Merge on read is not yet supported"), so this
                   covers delete-via-overwrite snapshots; positional-delete files
                   need a Spark writer and are not covered here.

Prints a JSON descriptor {"namespace", "tables": [{"name", "metadata_location",
"location", "metadata"}...]} on stdout for the mock Iceberg REST catalog.
"""
import json
import sys
import tempfile

import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog


def describe(table):
    with table.io.new_input(table.metadata_location).open() as f:
        metadata_json = json.loads(f.read().decode("utf-8"))
    return {
        "name": table.name()[-1],
        "metadata_location": table.metadata_location,
        "location": table.location(),
        "metadata": metadata_json,
    }


def main() -> None:
    endpoint, bucket = sys.argv[1], sys.argv[2]
    access_key = sys.argv[3] if len(sys.argv) > 3 else "testing"
    secret_key = sys.argv[4] if len(sys.argv) > 4 else "testing"

    tmp = tempfile.mkdtemp(prefix="iceberg-sqlcat-")
    catalog = SqlCatalog(
        "local",
        uri=f"sqlite:///{tmp}/catalog.db",
        warehouse=f"s3://{bucket}/warehouse",
        **{
            "s3.endpoint": endpoint,
            "s3.access-key-id": access_key,
            "s3.secret-access-key": secret_key,
            "s3.region": "us-east-1",
            # Generous timeouts: on a busy CI runner moto can respond slowly and
            # the AWS SDK's low-speed watchdog (curl 28) kills uploads otherwise.
            "s3.connect-timeout": "60",
            "s3.request-timeout": "60",
        },
    )

    catalog.create_namespace("lakehouse")
    tables = []

    # --- events: plain table, two snapshots ---
    data = pa.table(
        {
            "id": pa.array([1, 2, 3, 4, 5], type=pa.int64()),
            "name": pa.array(["alpha", "beta", "gamma", "delta", "epsilon"]),
            "score": pa.array([1.5, 2.5, 3.5, 4.5, 5.5], type=pa.float64()),
        }
    )
    events = catalog.create_table("lakehouse.events", schema=data.schema)
    events.append(data)
    events.append(
        pa.table(
            {
                "id": pa.array([6, 7], type=pa.int64()),
                "name": pa.array(["zeta", "eta"]),
                "score": pa.array([6.5, 7.5], type=pa.float64()),
            }
        )
    )
    tables.append(describe(events))

    # --- city_events: identity partition on `city`, values with spaces ->
    #     data file keys like .../city=New%20York/... (URL-encoding coverage) ---
    from pyiceberg.partitioning import PartitionField, PartitionSpec
    from pyiceberg.transforms import IdentityTransform

    city_data = pa.table(
        {
            "id": pa.array([1, 2, 3, 4], type=pa.int64()),
            "city": pa.array(["New York", "San Francisco", "New York", "Los Angeles"]),
            "amount": pa.array([10.0, 20.0, 30.0, 40.0], type=pa.float64()),
        }
    )
    city_events = catalog.create_table("lakehouse.city_events", schema=city_data.schema)
    with city_events.update_spec() as update:
        update.add_field("city", IdentityTransform(), "city")
    city_events = catalog.load_table("lakehouse.city_events")
    city_events.append(city_data)
    tables.append(describe(city_events))

    # --- deleted_events: delete -> copy-on-write overwrite snapshot ---
    del_data = pa.table(
        {
            "id": pa.array([1, 2, 3, 4, 5], type=pa.int64()),
            "v": pa.array(["a", "b", "c", "d", "e"]),
        }
    )
    deleted = catalog.create_table(
        "lakehouse.deleted_events",
        schema=del_data.schema,
        properties={"format-version": "2"},
    )
    deleted.append(del_data)
    deleted.delete(delete_filter="id = 2 or id = 4")
    deleted = catalog.load_table("lakehouse.deleted_events")
    tables.append(describe(deleted))

    print(json.dumps({"namespace": "lakehouse", "tables": tables}))


if __name__ == "__main__":
    main()
