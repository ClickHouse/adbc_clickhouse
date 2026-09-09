//! Integration test for JWT access token authentication.
//!
//! JWT authentication is only supported by ClickHouse Cloud (the local docker server
//! cannot validate a JWT), so this test is skipped unless a real Cloud service is
//! configured via `ADBC_CLICKHOUSE_TEST_JWT_URL` and `ADBC_CLICKHOUSE_TEST_JWT_TOKEN`.

use crate::test_driver;
use adbc_clickhouse::options;
use adbc_core::options::OptionDatabase;
use adbc_core::{Connection, Database, Driver, Optionable, Statement};
use arrow_array::cast::AsArray;

#[test]
fn jwt_access_token() {
    let (Ok(url), Ok(token)) = (
        std::env::var("ADBC_CLICKHOUSE_TEST_JWT_URL"),
        std::env::var("ADBC_CLICKHOUSE_TEST_JWT_TOKEN"),
    ) else {
        eprintln!(
            "skipping jwt_access_token: set ADBC_CLICKHOUSE_TEST_JWT_URL and \
             ADBC_CLICKHOUSE_TEST_JWT_TOKEN to run against a ClickHouse Cloud service"
        );
        return;
    };

    let mut driver = test_driver();

    let db = driver
        .new_database_with_opts([(OptionDatabase::Uri, url.into())])
        .unwrap();

    let mut conn = db
        .new_connection_with_opts([(options::ACCESS_TOKEN.into(), token.into())])
        .unwrap();

    let mut statement = conn.new_statement().unwrap();
    statement.set_sql_query("SELECT currentUser()").unwrap();

    let mut records = statement.execute().unwrap();
    let record = records
        .next()
        .expect("expected one RecordBatch, got none")
        .unwrap();

    let current_user = record.column(0).as_string::<i32>().value(0);
    assert!(
        !current_user.is_empty(),
        "expected currentUser() to be non-empty"
    );
    drop(records);

    // Rotate to a garbage token; new statements on this connection should now fail auth.
    conn.set_option(options::ACCESS_TOKEN.into(), "not_a_valid_token".into())
        .unwrap();

    let mut statement = conn.new_statement().unwrap();
    statement.set_sql_query("SELECT 1").unwrap();
    assert!(statement.execute().is_err());
}

/// Cheap local sanity check that doesn't require ClickHouse Cloud: the local docker server
/// can't validate a JWT, but it should still reject the `Bearer` scheme outright rather than
/// silently falling back to an anonymous login. This proves the header is actually sent.
#[test]
fn bearer_header_is_rejected_by_local_server() {
    let mut driver = test_driver();

    let db = driver
        .new_database_with_opts([(OptionDatabase::Uri, "http://localhost:8123/".into())])
        .unwrap();

    let mut conn = db
        .new_connection_with_opts([(options::ACCESS_TOKEN.into(), "not_a_valid_token".into())])
        .unwrap();

    let mut statement = conn.new_statement().unwrap();
    statement.set_sql_query("SELECT 1").unwrap();

    let err = match statement.execute() {
        Ok(_) => panic!("expected an authentication error, got Ok"),
        Err(e) => e,
    };
    assert!(
        err.message.contains("AUTHENTICATION_FAILED") || err.message.contains("authentication"),
        "expected an authentication error, got: {:?}",
        err.message
    );
}
