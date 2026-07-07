#!/usr/bin/env python3
"""Create a real Delta Lake table on an S3-compatible endpoint (moto) with delta-rs.

Usage: make_delta_table.py <s3_endpoint> <bucket>

Writes s3://<bucket>/unity/sales (two commits -> two _delta_log entries) and
prints a JSON descriptor for the mock Unity catalog on stdout.
"""
import json
import sys

import pyarrow as pa
from deltalake import write_deltalake


def main() -> None:
    endpoint, bucket = sys.argv[1], sys.argv[2]
    location = f"s3://{bucket}/unity/sales"

    storage_options = {
        "AWS_ENDPOINT_URL": endpoint,
        "AWS_ACCESS_KEY_ID": "testing",
        "AWS_SECRET_ACCESS_KEY": "testing",
        "AWS_REGION": "us-east-1",
        "AWS_ALLOW_HTTP": "true",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
        # Generous timeout for slow/busy CI runners (object_store default is 30s
        # total per request, which a loaded moto can exceed).
        "timeout": "120s",
    }

    data = pa.table(
        {
            "id": pa.array([1, 2, 3], type=pa.int64()),
            "item": pa.array(["book", "pen", "desk"]),
            "price": pa.array([10.5, 1.25, 99.0], type=pa.float64()),
        }
    )
    write_deltalake(location, data, storage_options=storage_options)

    # Second commit so the log-replay path (00000...0.json + 00000...1.json) is exercised.
    more = pa.table(
        {
            "id": pa.array([4], type=pa.int64()),
            "item": pa.array(["lamp"]),
            "price": pa.array([25.0], type=pa.float64()),
        }
    )
    write_deltalake(location, more, mode="append", storage_options=storage_options)

    # Columns in the shape UnityCatalog.cpp parses: type_json is either a quoted
    # simple Delta type name (the OSS Unity quirk) or a full JSON field object.
    print(
        json.dumps(
            {
                "catalog": "unity",
                "schema": "lakeschema",
                "table": "sales",
                "location": location,
                "columns": [
                    {"name": "id", "nullable": True, "type_json": '"long"'},
                    {"name": "item", "nullable": True, "type_json": '"string"'},
                    {"name": "price", "nullable": True, "type_json": '"double"'},
                ],
            }
        )
    )


if __name__ == "__main__":
    main()
