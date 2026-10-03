use crate::test_database;
use adbc_core::error::Status;
use adbc_core::{Connection, Database, Optionable, Statement};

/// A query that sleeps longer than the configured bound is abandoned with a `Timeout` status.
#[test]
fn query_times_out() {
    let db = test_database();
    let mut conn = db.new_connection().unwrap();

    let mut statement = conn.new_statement().unwrap();
    statement
        .set_option(adbc_clickhouse::options::QUERY_TIMEOUT.into(), "1".into())
        .unwrap();
    statement.set_sql_query("SELECT sleep(3)").unwrap();

    let err = match statement.execute() {
        Ok(_) => panic!("query should have timed out"),
        Err(err) => err,
    };
    assert_eq!(err.status, Status::Timeout);
}

/// The configured timeout is read back verbatim.
#[test]
fn query_timeout_read_back() {
    let db = test_database();
    let mut conn = db.new_connection().unwrap();

    conn.set_option(adbc_clickhouse::options::QUERY_TIMEOUT.into(), "5".into())
        .unwrap();

    // A statement inherits the connection-level timeout.
    let statement = conn.new_statement().unwrap();
    assert_eq!(
        statement
            .get_option_string(adbc_clickhouse::options::QUERY_TIMEOUT.into())
            .unwrap(),
        "5"
    );
}

/// A fast query is unaffected by the timeout bound.
#[test]
fn query_within_timeout_succeeds() {
    let db = test_database();
    let mut conn = db.new_connection().unwrap();

    let mut statement = conn.new_statement().unwrap();
    statement
        .set_option(adbc_clickhouse::options::QUERY_TIMEOUT.into(), "30".into())
        .unwrap();
    statement.set_sql_query("SELECT 1").unwrap();

    let mut records = statement.execute().unwrap();
    assert!(records.next().unwrap().is_ok());
}
