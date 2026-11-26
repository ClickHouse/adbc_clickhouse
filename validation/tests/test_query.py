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

import adbc_drivers_validation.tests.query as query_tests
import pytest

from . import clickhouse
from .marks import xfail

Base = query_tests.TestQuery


def pytest_generate_tests(metafunc) -> None:
    return query_tests.generate_tests(clickhouse.QUIRKS, metafunc)


class _UncheckedTypeNameQuirks(clickhouse.ClickHouseQuirks):
    @property
    def field_metadata_prefix(self) -> str:
        return "CLICKHOUSE:unchecked"


_GET_TABLE_SCHEMA_XFAILS = {
    **{
        f"type/select/timestamp{p}": "get_table_schema() omits the server time zone "
        "that query results carry for `DateTime64` without a time zone"
        for p in range(10)
    },
    "type/select/time": "get_table_schema() doesn't map `Time64` to Arrow",
}


class TestQuery(Base):
    def test_get_table_schema(self, request, driver, conn, query, query_setup) -> None:
        if reason := _GET_TABLE_SCHEMA_XFAILS.get(query.name):
            request.applymarker(pytest.mark.xfail(reason=reason, strict=True))
        # `get_table_schema()` sets `CLICKHOUSE:type`, but query results don't, and the
        # suite has one switch (`metadata_type_name`) for both. Don't check the type name.
        super().test_get_table_schema(
            _UncheckedTypeNameQuirks(), conn, query, query_setup
        )

    test_query_bind_dictionary = xfail(
        Base.test_query_bind_dictionary,
        "dictionary-encoded parameters are not supported",
    )
    test_query_bind_stream = xfail(
        Base.test_query_bind_stream,
        "a query (not an insert) accepts only one bound row per execution",
    )
