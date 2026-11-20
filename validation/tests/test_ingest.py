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

import adbc_drivers_validation.tests.ingest as ingest_tests

from . import clickhouse
from .marks import xfail

Base = ingest_tests.TestIngest


def pytest_generate_tests(metafunc) -> None:
    return ingest_tests.generate_tests(clickhouse.QUIRKS, metafunc)


# Only `adbc.ingest.mode.append` is implemented, and most tests start with `create`
# https://github.com/ClickHouse/adbc_clickhouse/issues/59
def needs_ingest_modes(test):
    return xfail(test, "ingest modes other than append are not implemented")


class TestIngest(Base):
    test_create = needs_ingest_modes(Base.test_create)
    test_create_dictionary = needs_ingest_modes(Base.test_create_dictionary)
    test_append = needs_ingest_modes(Base.test_append)
    test_createappend = needs_ingest_modes(Base.test_createappend)
    test_create_conflict = needs_ingest_modes(Base.test_create_conflict)
    test_append_schema_mismatch = needs_ingest_modes(Base.test_append_schema_mismatch)
    test_createappend_schema_mismatch = needs_ingest_modes(
        Base.test_createappend_schema_mismatch
    )
    test_replace = needs_ingest_modes(Base.test_replace)
    test_replace_noop = needs_ingest_modes(Base.test_replace_noop)
    test_create_multiple_batches = needs_ingest_modes(Base.test_create_multiple_batches)
    test_create_large_batch = needs_ingest_modes(Base.test_create_large_batch)
    test_create_long_values = needs_ingest_modes(Base.test_create_long_values)
    test_many_columns = needs_ingest_modes(Base.test_many_columns)
    test_ingest_then_query = needs_ingest_modes(Base.test_ingest_then_query)
