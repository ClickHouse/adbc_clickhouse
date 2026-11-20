# Copyright (c) 2026 ADBC Drivers Contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import re
from pathlib import Path

from adbc_drivers_validation import model, quirks


class ClickHouseQuirks(model.DriverQuirks):
    name = "clickhouse"
    driver = "adbc_driver_clickhouse"
    driver_name = "ADBC Driver Foundry Driver for ClickHouse"
    vendor_name = "ClickHouse"
    # CI runs against the `latest` image, so don't pin a release
    vendor_version = re.compile(r"\d+\.\d+\.\d+\.\d+")
    short_version = "latest"
    features = model.DriverFeatures(
        connection_get_table_schema=True,
        connection_set_current_schema=True,
        statement_bind=True,
        # The driver binds one row per execution, by field name
        statement_bind_test_mode="select",
        statement_bulk_ingest=True,
        statement_prepare=True,
        current_catalog="",
        current_schema="default",
        supported_xdbc_fields=[],
    )
    setup = model.DriverSetup(
        database={
            "uri": os.environ.get("ADBC_CLICKHOUSE_TEST_URI", "http://localhost:8123/")
        },
        connection={"adbc.connection.db_schema": "default"},
        statement={},
    )

    @property
    def queries_paths(self) -> tuple[Path]:
        return (Path(__file__).parent.parent / "queries",)

    def is_table_not_found(self, table_name: str, error: Exception) -> bool:
        return "UNKNOWN_TABLE" in str(error) and table_name in str(error)

    def quote_one_identifier(self, identifier: str) -> str:
        return f"`{identifier}`"

    def split_statement(self, statement: str) -> list[str]:
        return quirks.split_statement(statement)


QUIRKS = [ClickHouseQuirks()]
