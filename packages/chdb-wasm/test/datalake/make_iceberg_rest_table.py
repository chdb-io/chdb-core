#!/usr/bin/env python3
"""Create an Iceberg table through a REAL Iceberg REST catalog (pyiceberg RestCatalog).

Usage: make_iceberg_rest_table.py <catalog_uri> <s3_endpoint> <access_key> <secret_key>

Creates namespace `smoke`, table `readings` (2 snapshots) via the REST protocol —
the catalog service (e.g. apache/iceberg-rest-fixture) manages the metadata and
writes to its configured warehouse. Prints {"namespace","table"} on stdout.
"""
import json
import sys

import pyarrow as pa
from pyiceberg.catalog import load_catalog


def main() -> None:
    catalog_uri, s3_endpoint, access_key, secret_key = sys.argv[1:5]

    catalog = load_catalog(
        "real-rest",
        **{
            "type": "rest",
            "uri": catalog_uri,
            "s3.endpoint": s3_endpoint,
            "s3.access-key-id": access_key,
            "s3.secret-access-key": secret_key,
            "s3.region": "us-east-1",
            # fsspec (s3fs) instead of pyarrow S3FileIO: the aws-sdk-cpp multipart
            # upload wedges against moto on loaded CI runners (curl 28 stalls);
            # fsspec+moto is the combination pyiceberg's own CI uses.
            "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO",
            # Generous timeouts for slow/busy CI runners (curl 28 low-speed watchdog).
            "s3.connect-timeout": "60",
            "s3.request-timeout": "60",
        },
    )

    catalog.create_namespace("smoke")
    data = pa.table(
        {
            "sensor": pa.array(["a", "b", "a", "c"]),
            "value": pa.array([10, 20, 30, 40], type=pa.int64()),
        }
    )
    table = catalog.create_table("smoke.readings", schema=data.schema)
    table.append(data)
    table.append(pa.table({"sensor": pa.array(["b"]), "value": pa.array([50], type=pa.int64())}))

    print(json.dumps({"namespace": "smoke", "table": "readings"}))


if __name__ == "__main__":
    main()
