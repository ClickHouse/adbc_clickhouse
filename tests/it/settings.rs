use crate::test_driver;
use adbc_core::options::{OptionDatabase, OptionStatement};
use adbc_core::{Connection, Database, Driver, Optionable, Statement};
use arrow_array::cast::AsArray;

/// Query the server-effective value of `mutations_sync` through the given statement.
fn query_setting(mut statement: impl Statement<Option = OptionStatement>) -> String {
    statement
        .set_sql_query("SELECT toString(getSetting('mutations_sync')) AS v")
        .unwrap();

    let mut records = statement.execute().unwrap();

    let record = records
        .next()
        .expect("expected one RecordBatch, got none")
        .unwrap();

    record
        .column_by_name("v")
        .expect("expected column `v`")
        .as_string::<i32>()
        .value(0)
        .into()
}

#[test]
fn query_with_settings() {
    let mut driver = test_driver();

    let db = driver
        .new_database_with_opts([
            (OptionDatabase::Uri, "http://localhost:8123/".into()),
            ("clickhouse.setting.mutations_sync".into(), "1".into()),
        ])
        .unwrap();

    // The database-level setting should be inherited by the connection
    let mut conn = db.new_connection().unwrap();
    assert_eq!(query_setting(conn.new_statement().unwrap()), "1");
    assert_eq!(
        conn.get_option_string("clickhouse.setting.mutations_sync".into())
            .unwrap(),
        "1"
    );

    // The connection can override the setting
    conn.set_option("clickhouse.setting.mutations_sync".into(), "2".into())
        .unwrap();
    assert_eq!(query_setting(conn.new_statement().unwrap()), "2");
    assert_eq!(
        conn.get_option_string("clickhouse.setting.mutations_sync".into())
            .unwrap(),
        "2"
    );

    // A sibling connection should still inherit the database-level value
    let mut conn2 = db.new_connection().unwrap();
    assert_eq!(query_setting(conn2.new_statement().unwrap()), "1");

    // A statement can override the setting
    let mut statement = conn.new_statement().unwrap();
    statement
        .set_option("clickhouse.setting.mutations_sync".into(), "0".into())
        .unwrap();
    assert_eq!(
        statement
            .get_option_string("clickhouse.setting.mutations_sync".into())
            .unwrap(),
        "0"
    );
    assert_eq!(query_setting(statement), "0");

    // A fresh statement on the same connection still sees the connection-level value
    assert_eq!(query_setting(conn.new_statement().unwrap()), "2");
}
