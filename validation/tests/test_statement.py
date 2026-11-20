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

import adbc_driver_manager
import adbc_drivers_validation.tests.statement as statement_tests

from . import clickhouse
from .marks import xfail


def pytest_generate_tests(metafunc) -> None:
    return statement_tests.generate_tests(clickhouse.QUIRKS, metafunc)


Base = statement_tests.TestStatement


# These bind fields named "0", "1", ..., but a ClickHouse query parameter name must be
# an identifier. https://github.com/ClickHouse/adbc_clickhouse/issues/81
def needs_positional_params(test):
    return xfail(test, "positional parameters are not supported")


class TestStatement(Base):
    test_parameter_execute = needs_positional_params(Base.test_parameter_execute)
    test_parameter_null_typed = needs_positional_params(Base.test_parameter_null_typed)

    def test_rows_affected(self, driver, conn) -> None:
        # ClickHouse doesn't like UPDATE, so truncate the upstream test
        table_name = "test_rows_affected"
        with conn.cursor() as cursor:
            cursor.adbc_statement.set_sql_query(
                driver.drop_table(table_name="test_rows_affected")
            )
            try:
                cursor.adbc_statement.execute_update()
            except adbc_driver_manager.Error as e:
                # Some databases have no way to do DROP IF EXISTS
                if not driver.is_table_not_found(table_name=table_name, error=e):
                    raise

            quoted_name = driver.quote_identifier(table_name)
            cursor.adbc_statement.set_sql_query(
                f"CREATE TABLE {quoted_name} (id INT) ENGINE = MergeTree ORDER BY id"
            )
            rows_affected = cursor.adbc_statement.execute_update()

            if (
                driver.features.statement_rows_affected
                and driver.features.statement_rows_affected_ddl
            ):
                assert rows_affected == 0
            else:
                assert rows_affected == -1

            cursor.adbc_statement.set_sql_query(
                f"INSERT INTO {quoted_name} (id) VALUES (1)"
            )
            rows_affected = cursor.adbc_statement.execute_update()
            if driver.features.statement_rows_affected:
                assert rows_affected == 1
            else:
                assert rows_affected == -1
