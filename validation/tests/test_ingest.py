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
import adbc_drivers_validation.tests.ingest as ingest_tests
import pyarrow
import pytest
from adbc_drivers_validation import compare, model
from adbc_drivers_validation.utils import execute_query_without_prepare

from . import clickhouse
from .marks import xfail

Base = ingest_tests.TestIngest


def pytest_generate_tests(metafunc) -> None:
    if metafunc.definition.name != "test_append_existing_table":
        return ingest_tests.generate_tests(clickhouse.QUIRKS, metafunc)

    quirks = clickhouse.QUIRKS[0]
    driver = f"{quirks.name}:{quirks.short_version}"
    metafunc.parametrize(
        "driver,query",
        [
            pytest.param(driver, q, id=f"{driver}:{q.name}", marks=q.pytest_marks)
            for q in quirks.query_set.queries.values()
            if isinstance(q.query, model.IngestQuery)
        ],
        scope="module",
        indirect=["driver"],
    )


_CLICKHOUSE_TYPES = {
    pyarrow.int16(): "Int16",
    pyarrow.int32(): "Int32",
    pyarrow.int64(): "Int64",
    pyarrow.float32(): "Float32",
    pyarrow.float64(): "Float64",
    pyarrow.bool_(): "Bool",
    pyarrow.date32(): "Date32",
}


def _clickhouse_type(ty: pyarrow.DataType) -> str:
    if name := _CLICKHOUSE_TYPES.get(ty):
        return name
    if pyarrow.types.is_fixed_size_binary(ty):
        return f"FixedString({ty.byte_width})"
    if (
        pyarrow.types.is_string(ty)
        or pyarrow.types.is_large_string(ty)
        or pyarrow.types.is_string_view(ty)
        or pyarrow.types.is_binary(ty)
        or pyarrow.types.is_large_binary(ty)
        or pyarrow.types.is_binary_view(ty)
    ):
        return "String"
    if pyarrow.types.is_decimal(ty):
        return f"Decimal({ty.precision}, {ty.scale})"
    precision = {"s": 0, "ms": 3, "us": 6, "ns": 9}
    if pyarrow.types.is_time(ty):
        return f"Time64({precision[ty.unit]})"
    if pyarrow.types.is_timestamp(ty):
        tz = f", '{ty.tz}'" if ty.tz else ""
        return f"DateTime64({precision[ty.unit]}{tz})"
    raise NotImplementedError(f"no ClickHouse type for {ty}")


def _create_table(cursor, driver, table_name: str, schema: pyarrow.Schema) -> None:
    columns = []
    for field in schema:
        ty = _clickhouse_type(field.type)
        if field.nullable:
            ty = f"Nullable({ty})"
        columns.append(f"{driver.quote_identifier(field.name)} {ty}")
    cursor.adbc_statement.set_sql_query(
        f"CREATE TABLE {driver.quote_identifier(table_name)} ({', '.join(columns)}) "
        "ENGINE = MergeTree ORDER BY tuple()"
    )
    cursor.adbc_statement.execute_update()


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

    def test_append_existing_table(self, driver, conn, query) -> None:
        """`test_append` on a table created with SQL, for every type."""
        subquery = query.query
        table_name = ingest_tests.make_table_name("test_ingest_append_existing", query)
        data = subquery.input()
        data2 = data.set_column(
            0, data.schema[0], pyarrow.array(range(len(data), len(data) * 2))
        )

        with conn.cursor() as cursor:
            driver.try_drop_table(cursor, table_name=table_name)
            _create_table(cursor, driver, table_name, data.schema)
            with driver.setup_statement(query, cursor):
                assert cursor.adbc_ingest(table_name, data, mode="append") == -1
                assert cursor.adbc_ingest(table_name, data2, mode="append") == -1

        columns = ", ".join(driver.quote_identifier(f.name) for f in data.schema)
        with conn.cursor() as cursor:
            with driver.setup_statement(query, cursor):
                result = execute_query_without_prepare(
                    cursor,
                    f"SELECT {columns} FROM {driver.quote_identifier(table_name)}",
                )

        expected = subquery.expected()
        expected2 = expected.set_column(
            0,
            expected.schema[0],
            pyarrow.array(range(len(expected), len(expected) * 2)),
        )
        compare.compare_tables(
            ingest_tests._sort_table_by_first_column(
                pyarrow.concat_tables([expected, expected2])
            ),
            ingest_tests._sort_table_by_first_column(result),
            query.metadata(),
        )

    @pytest.mark.xfail(
        reason="appending a mismatched schema fails with INTERNAL, not ALREADY_EXISTS"
    )
    def test_append_existing_table_schema_mismatch(self, driver, conn, query) -> None:
        """`test_append_schema_mismatch` on a table created with SQL."""
        table_name = ingest_tests.make_table_name(
            "test_ingest_append_existing_mismatch", query
        )
        data = query.query.input()
        mismatched = data.append_column(
            "adbc_extra_column", pyarrow.array([0] * len(data), type=pyarrow.int64())
        )

        with conn.cursor() as cursor:
            driver.try_drop_table(cursor, table_name=table_name)
            _create_table(cursor, driver, table_name, data.schema)
            with driver.setup_statement(query, cursor):
                with pytest.raises(adbc_driver_manager.dbapi.Error) as excinfo:
                    cursor.adbc_ingest(table_name, mismatched, mode="append")

        assert (
            excinfo.value.status_code
            == adbc_driver_manager.AdbcStatusCode.ALREADY_EXISTS
        )
