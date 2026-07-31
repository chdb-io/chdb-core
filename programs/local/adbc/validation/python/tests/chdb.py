# Quirks definition for validating the chDB ADBC driver with
# adbc-drivers/validation. chDB speaks the same SQL dialect as ClickHouse,
# so the query set mirrors the ClickHouse driver's (Apache-2.0, headers kept).
import os
import re
from pathlib import Path

from adbc_drivers_validation import model

from .engine_version import SHORT_VERSION


class ChdbQuirks(model.DriverQuirks):
    name = "chdb"
    driver = "chdb"
    driver_name = "ADBC chDB Driver"
    vendor_name = "ClickHouse"
    vendor_version = re.compile(rf"^{re.escape(SHORT_VERSION)}(\.\d+)*$")
    short_version = SHORT_VERSION
    features = model.DriverFeatures(
        connection_get_table_schema=True,
        get_objects=True,
        metadata_type_name=False,
        statement_bind=True,
        statement_bulk_ingest=True,
        statement_bulk_ingest_schema=True,
        statement_get_parameter_schema=True,
        statement_prepare=True,
        statement_rows_affected=True,
        current_schema="default",
        secondary_schema="validation2",
    )
    setup = model.DriverSetup(database={}, connection={}, statement={})

    @property
    def queries_paths(self) -> tuple[Path]:
        return (Path(__file__).parent.parent / "queries",)

    def is_table_not_found(self, table_name: str, error: Exception) -> bool:
        return "UNKNOWN_TABLE" in str(error) or (
            "Unknown table" in str(error) and table_name in str(error)
        )

    def quote_one_identifier(self, identifier: str) -> str:
        return f"`{identifier}`"

    def query_override(self, context: str, default: str) -> str:
        # ClickHouse columns are non-nullable by default, so the suite's
        # `(id INT, value VARCHAR)` sample table would coerce an inserted NULL
        # to '' instead of storing NULL. Declare the columns Nullable so the
        # typed-NULL parameter case round-trips.
        if context == "TestStatement.sample_table":
            return default.replace("id INT", "id Nullable(Int32)").replace(
                "value VARCHAR", "value Nullable(String)"
            )
        return default


QUIRKS = [ChdbQuirks()]
