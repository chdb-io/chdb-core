#!/usr/bin/env python3
"""Smoke the built native wheel against a real local Iceberg table.

PyIceberg writes the table and a tiny local HTTP server exposes the subset of
the Iceberg REST catalog that DataLakeCatalog needs.  This keeps the release
gate self-contained while exercising catalog discovery, Iceberg metadata,
manifest and Parquet reads through the installed chdb-core wheel.
"""

from __future__ import annotations

import json
import sys
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import pyarrow as pa
from chdb import session
from pyiceberg.catalog.sql import SqlCatalog


def _warehouse_uri(path: Path) -> str:
    # PyIceberg's local file IO turns file:///C:/... into the path /C:/..., which Windows rejects, so
    # spell a Windows warehouse file:C:/... (RFC 8089's form for drive letters), which it reads as C:/...
    if sys.platform == "win32":
        return f"file:{path.as_posix()}"
    return f"file://{path}"


def _make_table(root: Path) -> dict:
    catalog = SqlCatalog(
        "fixture",
        uri=f"sqlite:///{root / 'catalog.db'}",
        warehouse=_warehouse_uri(root / "warehouse"),
    )
    catalog.create_namespace("smoke")
    table = catalog.create_table(
        "smoke.items",
        schema=pa.schema(
            [
                pa.field("id", pa.int64(), nullable=False),
                pa.field("name", pa.string(), nullable=False),
            ]
        ),
    )
    table.append(
        pa.table(
            {
                "id": pa.array([1, 2, 3], type=pa.int64()),
                "name": pa.array(["one", "two", "three"]),
            },
            schema=table.schema().as_arrow(),
        )
    )
    table.append(
        pa.table(
            {"id": pa.array([4], type=pa.int64()), "name": pa.array(["four"])},
            schema=table.schema().as_arrow(),
        )
    )
    table = catalog.load_table("smoke.items")
    with table.io.new_input(table.metadata_location).open() as stream:
        metadata = json.loads(stream.read().decode("utf-8"))
    # Close the SQLite catalog: Windows cannot delete the temporary directory while it is open.
    catalog.engine.dispose()
    return {
        "metadata-location": table.metadata_location,
        "metadata": metadata,
        "config": {},
    }


def _handler(table: dict):
    class CatalogHandler(BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802 - BaseHTTPRequestHandler API
            request = urlsplit(self.path)
            path = request.path.rstrip("/")
            if path == "/v1/config":
                self._send({"defaults": {}, "overrides": {}})
            elif path == "/v1/namespaces":
                parent = parse_qs(request.query).get("parent")
                self._send({"namespaces": [] if parent else [["smoke"]]})
            elif path == "/v1/namespaces/smoke/tables":
                self._send({"identifiers": [{"namespace": ["smoke"], "name": "items"}]})
            elif path == "/v1/namespaces/smoke/tables/items":
                self._send(table)
            else:
                self._send(
                    {
                        "error": {
                            "message": f"not found: {self.path}",
                            "type": "NoSuchTableException",
                            "code": 404,
                        }
                    },
                    404,
                )

        def _send(self, payload: dict, status: int = 200) -> None:
            body = json.dumps(payload).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, _format: str, *_args) -> None:
            pass

    return CatalogHandler


class TestNativeIcebergCatalog(unittest.TestCase):
    @unittest.skipIf(
        sys.platform == "win32",
        "DataLakeCatalog does not parse a Windows table location (file:C:/...) yet",
    )
    def test_rest_catalog_reads_real_local_iceberg_table(self):
        # chDB restricts local-file access to the process working directory. Keep
        # the Iceberg warehouse below it so this exercises the installed wheel
        # without weakening that sandbox.
        with tempfile.TemporaryDirectory(prefix="chdb-iceberg-", dir=Path.cwd()) as tmp:
            table = _make_table(Path(tmp))
            server = ThreadingHTTPServer(("127.0.0.1", 0), _handler(table))
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()

            sess = session.Session()
            try:
                endpoint = f"http://127.0.0.1:{server.server_port}/v1"
                sess.query("SET allow_database_iceberg = 1")
                sess.query(
                    "CREATE DATABASE lake "
                    f"ENGINE = DataLakeCatalog('{endpoint}') "
                    "SETTINGS catalog_type = 'rest', warehouse = 'warehouse', "
                    "vended_credentials = false, aws_access_key_id = 'testing', "
                    "aws_secret_access_key = 'testing'"
                )

                tables = str(sess.query("SHOW TABLES FROM lake", "CSV")).strip()
                self.assertEqual(tables, '"smoke.items"')

                aggregate = str(
                    sess.query(
                        "SELECT count(), sum(id) FROM lake.`smoke.items`",
                        "CSV",
                    )
                ).strip()
                self.assertEqual(aggregate, "4,10")

                filtered = str(
                    sess.query(
                        "SELECT id, name FROM lake.`smoke.items` "
                        "WHERE id >= 3 ORDER BY id",
                        "CSV",
                    )
                ).strip()
                self.assertEqual(filtered, '3,"three"\n4,"four"')
            finally:
                sess.close()
                server.shutdown()
                server.server_close()
                thread.join(timeout=5)

    def test_iceberg_local_reads_real_local_iceberg_table(self):
        # The same table, read straight from its directory: Iceberg metadata, Avro manifests and
        # Parquet data files, without the catalog. Kept below the working directory, as above.
        with tempfile.TemporaryDirectory(prefix="chdb-iceberg-", dir=Path.cwd()) as tmp:
            _make_table(Path(tmp))
            table_dir = (Path(tmp) / "warehouse" / "smoke" / "items").as_posix()
            sess = session.Session()
            try:
                aggregate = str(
                    sess.query(f"SELECT count(), sum(id) FROM icebergLocal('{table_dir}')", "CSV")
                ).strip()
                self.assertEqual(aggregate, "4,10")

                filtered = str(
                    sess.query(
                        f"SELECT id, name FROM icebergLocal('{table_dir}') WHERE id >= 3 ORDER BY id",
                        "CSV",
                    )
                ).strip()
                self.assertEqual(filtered, '3,"three"\n4,"four"')
            finally:
                sess.close()


if __name__ == "__main__":
    unittest.main()
