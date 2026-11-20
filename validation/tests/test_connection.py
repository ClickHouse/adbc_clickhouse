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

import adbc_drivers_validation.tests.connection as connection_tests

from . import clickhouse
from .marks import xfail

Base = connection_tests.TestConnection


def pytest_generate_tests(metafunc) -> None:
    return connection_tests.generate_tests(clickhouse.QUIRKS, metafunc)


class TestConnection(Base):
    # https://github.com/ClickHouse/adbc_clickhouse/issues/9
    test_get_info = xfail(Base.test_get_info, "get_info() is not implemented")
    test_get_info_arrow_version = xfail(
        Base.test_get_info_arrow_version, "get_info() is not implemented"
    )

    test_get_table_schema_not_found = xfail(
        Base.test_get_table_schema_not_found,
        "returns INTERNAL instead of NOT_FOUND for a table that doesn't exist",
    )

    # The failures are in subtests, which pytest reports apart from the test itself,
    # so this XPASSes. https://github.com/ClickHouse/adbc_clickhouse/issues/100
    test_unknown_option = xfail(
        Base.test_unknown_option,
        "an unknown option fails with INVALID_ARGUMENT or NOT_IMPLEMENTED, not NOT_FOUND",
        strict=False,
    )
