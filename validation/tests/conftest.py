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
import sys
from pathlib import Path

import adbc_drivers_validation.model
import pytest
from adbc_drivers_validation.tests.conftest import (  # noqa: F401
    conn,
    conn_factory,
    db_kwargs,
    manual_test,
    pytest_addoption,
    pytest_collection_modifyitems,
)

from .clickhouse import ClickHouseQuirks


@pytest.fixture(scope="session")
def driver(request) -> adbc_drivers_validation.model.DriverQuirks:
    driver = request.param
    assert driver.startswith("clickhouse")
    return ClickHouseQuirks()


@pytest.fixture(scope="session")
def driver_path() -> str:
    if path := os.environ.get("ADBC_CLICKHOUSE_DRIVER_PATH"):
        return path

    prefix, ext = {
        "win32": ("", "dll"),
        "darwin": ("lib", "dylib"),
    }.get(sys.platform, ("lib", "so"))
    return str(
        Path(__file__).parents[2] / f"target/debug/{prefix}adbc_clickhouse.{ext}"
    )
