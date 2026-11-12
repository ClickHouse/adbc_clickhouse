//! Tests for `Statement::execute_schema()`.

use crate::test_database;
use adbc_core::error::Status;
use adbc_core::options::OptionConnection;
use adbc_core::{Connection, Database, Statement};
use arrow_array::{RecordBatch, create_array};
use arrow_schema::{DataType, Field, Schema};

/// Columns of several types, including nullable, decimal and low-cardinality ones.
const QUERY: &str = "SELECT number, toString(number) AS s, toNullable(toInt32(number)) AS n, \
    toDecimal64(number, 2) AS d64, toDecimal128(number, 4) AS d128, toDate('2024-01-01') AS d, toDate32('2024-01-01') AS d32, \
    toDateTime64('2024-01-01 00:00:00', 3, 'UTC') AS ts, toFloat64(number) / 3 AS f, \
    toLowCardinality(toString(number % 3)) AS lc, [number, number + 1] AS arr \
    FROM system.numbers LIMIT 3";

/// `execute_schema()` annotates each field with its ClickHouse type (`CLICKHOUSE:type` metadata),
/// as `get_table_schema()` does; the Arrow stream from `execute()` carries no field metadata.
fn without_metadata(schema: &Schema) -> Schema {
    Schema::new(
        schema
            .fields()
            .iter()
            .map(|f| f.as_ref().clone().with_metadata(Default::default()))
            .collect::<Vec<_>>(),
    )
}

#[test]
fn execute_schema_matches_execute() {
    let db = test_database();
    let mut conn = db.new_connection().unwrap();

    let mut statement = conn.new_statement().unwrap();
    statement.set_sql_query(QUERY).unwrap();
    let described = statement.execute_schema().unwrap();

    let mut statement = conn.new_statement().unwrap();
    statement.set_sql_query(QUERY).unwrap();
    let executed = statement.execute().unwrap().schema();

    assert_eq!(without_metadata(&described), *executed);
    assert_eq!(
        described.field_with_name("lc").unwrap().metadata()["CLICKHOUSE:type"],
        "LowCardinality(String)"
    );
}

#[test]
fn execute_after_execute_schema() {
    let db = test_database();
    let mut conn = db.new_connection().unwrap();
    let mut statement = conn.new_statement().unwrap();
    statement.set_sql_query(QUERY).unwrap();

    // The data query reuses the statement's query ID; the schema requests must not collide with it.
    for _ in 0..3 {
        let described = statement.execute_schema().unwrap();
        let reader = statement.execute().unwrap();
        assert_eq!(without_metadata(&described), *reader.schema());
        let rows: usize = reader.map(|batch| batch.unwrap().num_rows()).sum();
        assert_eq!(rows, 3);
    }
}

#[test]
fn execute_schema_with_bound_parameters() {
    let db = test_database();
    let mut conn = db.new_connection().unwrap();
    let mut statement = conn.new_statement().unwrap();
    statement
        .set_sql_query("SELECT {x:Int32} AS x, {s:String} AS s")
        .unwrap();

    let params = RecordBatch::try_new(
        Schema::new(vec![
            Field::new("x", DataType::Int32, false),
            Field::new("s", DataType::Utf8, false),
        ])
        .into(),
        vec![create_array!(Int32, [7]), create_array!(Utf8, ["seven"])],
    )
    .unwrap();
    statement.bind(params).unwrap();

    assert_eq!(
        without_metadata(&statement.execute_schema().unwrap()),
        Schema::new(vec![
            Field::new("x", DataType::Int32, false),
            Field::new("s", DataType::Utf8, false),
        ])
    );
}

#[test]
fn execute_schema_without_query() {
    let db = test_database();
    let mut conn = db.new_connection().unwrap();
    let mut statement = conn.new_statement().unwrap();
    let err = statement.execute_schema().unwrap_err();
    assert_eq!(err.status, Status::InvalidState);
}

#[test]
fn execute_schema_reports_server_errors() {
    let db = test_database();
    let mut conn = db.new_connection().unwrap();
    let mut statement = conn.new_statement().unwrap();
    statement
        .set_sql_query("SELECT * FROM adbc_no_such_table_for_execute_schema")
        .unwrap();
    let err = statement.execute_schema().unwrap_err();
    assert!(
        err.message.contains("UNKNOWN_TABLE") || err.message.contains("doesn't exist"),
        "unexpected error: {}",
        err.message
    );
}

#[test]
fn execute_schema_date_follows_date_as_uint16() {
    let db = test_database();
    for (setting, expected) in [("0", DataType::Date32), ("1", DataType::UInt16)] {
        let mut conn = db
            .new_connection_with_opts([(
                OptionConnection::Other(
                    "clickhouse.setting.output_format_arrow_date_as_uint16".into(),
                ),
                setting.into(),
            )])
            .unwrap();
        let mut statement = conn.new_statement().unwrap();
        statement
            .set_sql_query("SELECT toDate('2024-01-02') AS d")
            .unwrap();

        let described = statement.execute_schema().unwrap();
        let executed = statement.execute().unwrap().schema();
        assert_eq!(
            described.field(0).data_type(),
            &expected,
            "setting {setting}"
        );
        assert_eq!(
            executed.field(0).data_type(),
            &expected,
            "setting {setting}"
        );
    }
}
