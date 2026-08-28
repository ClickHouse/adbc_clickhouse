//! Tests for configuring the default database of a connection.
//!
//! See: <https://github.com/ClickHouse/adbc_clickhouse/issues/67>

use crate::test_driver;
use adbc_core::options::{OptionConnection, OptionDatabase, OptionStatement};
use adbc_core::{Connection, Database, Driver, Optionable, Statement};
use arrow_array::cast::AsArray;
use arrow_array::{RecordBatch, RecordBatchIterator};
use arrow_schema::{DataType, Field, Schema};
use std::sync::Arc;

/// Execute `SELECT currentDatabase()` on the given statement and return the result.
fn current_database(mut statement: impl Statement<Option = OptionStatement>) -> String {
    statement
        .set_sql_query("SELECT currentDatabase() AS db")
        .unwrap();

    let mut records = statement.execute().unwrap();

    let record = records
        .next()
        .expect("expected one RecordBatch, got none")
        .unwrap();

    record.column(0).as_string::<i32>().value(0).into()
}

#[test]
fn database_from_url_param() {
    let mut driver = test_driver();

    let db_name = format!("adbc_test_db_{}", rand::random::<u64>());

    // Create the test database through a connection without a configured database.
    let setup_db = driver
        .new_database_with_opts([(OptionDatabase::Uri, "http://localhost:8123/".into())])
        .unwrap();

    let mut setup_conn = setup_db.new_connection().unwrap();

    let mut setup = setup_conn.new_statement().unwrap();
    setup
        .set_sql_query(format!("CREATE DATABASE {db_name}"))
        .unwrap();
    setup.execute_update().unwrap();

    // The `database` URL parameter sets the default database for all connections.
    let db = driver
        .new_database_with_opts([(
            OptionDatabase::Uri,
            format!("http://localhost:8123/?database={db_name}").into(),
        )])
        .unwrap();

    let mut conn = db.new_connection().unwrap();

    assert_eq!(
        conn.get_option_string(OptionConnection::CurrentSchema)
            .unwrap(),
        db_name
    );
    assert_eq!(current_database(conn.new_statement().unwrap()), db_name);

    // Unqualified DDL resolves to the configured database (query path).
    let mut create_table = conn.new_statement().unwrap();
    create_table
        .set_sql_query("CREATE TABLE foo(bar Int32) ENGINE = MergeTree ORDER BY bar")
        .unwrap();
    create_table.execute_update().unwrap();

    // Unqualified streaming inserts also resolve to the configured database (insert path).
    let schema = Arc::new(Schema::new(vec![Field::new("bar", DataType::Int32, false)]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(arrow_array::Int32Array::from(vec![1, 2, 3]))],
    )
    .unwrap();

    let mut insert = conn.new_statement().unwrap();
    insert
        .set_sql_query("INSERT INTO foo(bar) FORMAT ArrowStream")
        .unwrap();
    insert
        .bind_stream(Box::new(RecordBatchIterator::new([Ok(batch)], schema)))
        .unwrap();
    insert.execute_update().unwrap();

    // Confirm through the unconfigured connection that everything landed in `db_name`.
    let mut check = setup_conn.new_statement().unwrap();
    check
        .set_sql_query(format!("SELECT sum(bar) AS total FROM {db_name}.foo"))
        .unwrap();

    let mut records = check.execute().unwrap();
    let record = records
        .next()
        .expect("expected one RecordBatch, got none")
        .unwrap();
    assert_eq!(
        record
            .column(0)
            .as_primitive::<arrow_array::types::Int64Type>()
            .value(0),
        6
    );
    drop(records);

    // `OptionConnection::CurrentSchema` overrides the URL parameter per connection...
    let mut override_conn = db
        .new_connection_with_opts([(OptionConnection::CurrentSchema, "default".into())])
        .unwrap();

    assert_eq!(
        override_conn
            .get_option_string(OptionConnection::CurrentSchema)
            .unwrap(),
        "default"
    );
    assert_eq!(
        current_database(override_conn.new_statement().unwrap()),
        "default"
    );

    // ...and may also be set after the connection is created.
    override_conn
        .set_option(OptionConnection::CurrentSchema, db_name.clone().into())
        .unwrap();
    assert_eq!(
        current_database(override_conn.new_statement().unwrap()),
        db_name
    );

    // Clean up.
    let mut cleanup = setup_conn.new_statement().unwrap();
    cleanup
        .set_sql_query(format!("DROP DATABASE {db_name}"))
        .unwrap();
    cleanup.execute_update().unwrap();
}
