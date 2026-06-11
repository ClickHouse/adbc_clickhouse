use crate::test_database;
use adbc_core::error::Status;
use adbc_core::options::{IngestMode, OptionStatement};
use adbc_core::{Connection, Database, Optionable, Statement};
use arrow_array::{RecordBatch, RecordBatchIterator, RecordBatchReader, create_array};
use arrow_schema::{DataType, Field, Schema};

#[test]
fn unqualified_table() {
    let db = test_database();
    let mut conn = db.new_connection().unwrap();

    let mut create_table = conn.new_statement().unwrap();

    create_table
        .set_sql_query(
            "CREATE TEMPORARY TABLE foo(bar Int32, baz String) ENGINE = MergeTree ORDER BY bar",
        )
        .unwrap();

    create_table.execute_update().unwrap();

    let mut insert = conn.new_statement().unwrap();

    insert
        .set_option(OptionStatement::TargetTable, "foo".into())
        .unwrap();

    // `record_batch!()` sets all columns to be nullable
    let record_batch = RecordBatch::try_new(
        Schema::new(vec![
            Field::new("bar", DataType::Int32, false),
            Field::new("baz", DataType::Utf8, false),
        ])
        .into(),
        vec![
            create_array!(Int32, [1, 2, 3, 4, 5]),
            create_array!(Utf8, ["Lorem", "ipsum", "dolor", "sit", "amet"]),
        ],
    )
    .unwrap();

    let schema = record_batch.schema();

    insert
        .bind_stream(Box::new(RecordBatchIterator::new(
            [Ok(record_batch.clone())],
            schema,
        )))
        .unwrap();

    insert.execute_update().unwrap();

    let mut select = conn.new_statement().unwrap();

    select
        .set_sql_query("SELECT bar, baz FROM foo ORDER BY bar")
        .unwrap();

    let reader = select.execute().unwrap();

    let schema = reader.schema();

    let records = reader.collect::<Result<Vec<_>, _>>().unwrap();

    let merged = arrow::compute::concat_batches(&schema, &records).unwrap();

    assert_eq!(record_batch, merged);
}

#[test]
fn qualified_table() {
    let db = test_database();
    let mut conn = db.new_connection().unwrap();

    let mut create_db = conn.new_statement().unwrap();

    create_db
        .set_sql_query("CREATE DATABASE IF NOT EXISTS adbc_clickhouse_test")
        .unwrap();

    create_db.execute().unwrap();

    let mut create_table = conn.new_statement().unwrap();

    create_table
        .set_sql_query(
            // tables inside a database cannot be temporary
            "CREATE OR REPLACE TABLE adbc_clickhouse_test.qualified_table_test(bar Int32, baz String) ENGINE = MergeTree ORDER BY bar",
        )
        .unwrap();

    create_table.execute_update().unwrap();

    let mut insert = conn.new_statement().unwrap();

    insert
        .set_option(
            OptionStatement::TargetDbSchema,
            "adbc_clickhouse_test".into(),
        )
        .unwrap();

    insert
        .set_option(OptionStatement::TargetTable, "qualified_table_test".into())
        .unwrap();

    // `record_batch!()` sets all columns to be nullable
    let record_batch = RecordBatch::try_new(
        Schema::new(vec![
            Field::new("bar", DataType::Int32, false),
            Field::new("baz", DataType::Utf8, false),
        ])
        .into(),
        vec![
            create_array!(Int32, [1, 2, 3, 4, 5]),
            create_array!(Utf8, ["Lorem", "ipsum", "dolor", "sit", "amet"]),
        ],
    )
    .unwrap();

    let schema = record_batch.schema();

    insert
        .bind_stream(Box::new(RecordBatchIterator::new(
            [Ok(record_batch.clone())],
            schema,
        )))
        .unwrap();

    insert.execute_update().unwrap();

    let mut select = conn.new_statement().unwrap();

    select
        .set_sql_query(
            "SELECT bar, baz FROM adbc_clickhouse_test.qualified_table_test ORDER BY bar",
        )
        .unwrap();

    let reader = select.execute().unwrap();

    let schema = reader.schema();

    let records = reader.collect::<Result<Vec<_>, _>>().unwrap();

    let merged = arrow::compute::concat_batches(&schema, &records).unwrap();

    assert_eq!(record_batch, merged);
}

#[test]
fn errors_without_table() {
    let db = test_database();
    let mut conn = db.new_connection().unwrap();

    let mut statement = conn.new_statement().unwrap();

    statement
        .set_option(OptionStatement::TargetDbSchema, "foo".into())
        .unwrap();

    statement
        .bind_stream(Box::new(RecordBatchIterator::new(
            [],
            Schema::empty().into(),
        )))
        .unwrap();

    let err = statement.execute_update().unwrap_err();

    assert_eq!(err.status, Status::InvalidState);

    statement
        .set_option(OptionStatement::IngestMode, IngestMode::Append.into())
        .unwrap();

    let err = statement.execute_update().unwrap_err();

    assert_eq!(err.status, Status::InvalidState);
}

#[test]
fn unsupported_options() {
    let db = test_database();
    let mut conn = db.new_connection().unwrap();

    let mut statement = conn.new_statement().unwrap();

    // Canary test for options that are currently explicitly unsupported
    // If support gets added, a new test should be written
    for mode in [
        IngestMode::Create,
        IngestMode::Replace,
        IngestMode::CreateAppend,
    ] {
        assert_eq!(
            statement
                .set_option(OptionStatement::IngestMode, mode.into())
                .unwrap_err()
                .status,
            Status::NotImplemented,
            "expected {mode:?} to return NotImplemented"
        );
    }

    assert_eq!(
        statement
            .set_option(OptionStatement::TargetCatalog, "foo".into())
            .unwrap_err()
            .status,
        Status::InvalidArguments,
        "ClickHouse does not support catalogs"
    );

    assert_eq!(
        statement
            .set_option(OptionStatement::Temporary, 1.into())
            .unwrap_err()
            .status,
        Status::NotImplemented,
        "temporary table ingest not yet implemented"
    );
}
