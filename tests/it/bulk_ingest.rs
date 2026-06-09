use crate::test_database;
use adbc_core::error::Status;
use adbc_core::options::{IngestMode, OptionStatement};
use adbc_core::{Connection, Database, Optionable, Statement};
use arrow_array::{RecordBatchIterator, record_batch};
use arrow_schema::Schema;

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

    let record_batch = record_batch!(
        ("bar", Int32, [1, 2, 3, 4, 5]),
        ("baz", Utf8, ["Lorem", "ipsum", "dolor", "sit", "amet"])
    )
    .unwrap();

    let schema = record_batch.schema();

    insert
        .bind_stream(Box::new(RecordBatchIterator::new(
            [Ok(record_batch)],
            schema,
        )))
        .unwrap();

    insert.execute_update().unwrap();
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
