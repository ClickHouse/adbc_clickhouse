//! Requests on one connection share a ClickHouse session, which runs one request at a time.

use crate::test_database;
use adbc_core::{Connection, Database, Statement};
use std::time::{Duration, Instant};

#[test]
fn query_after_dropping_an_unfinished_result() {
    let db = test_database();
    let mut conn = db.new_connection().unwrap();

    for _ in 0..5 {
        let mut statement = conn.new_statement().unwrap();
        statement
            .set_sql_query("SELECT number, toString(number) FROM system.numbers LIMIT 50000000")
            .unwrap();
        let mut reader = statement.execute().unwrap();
        reader.next().unwrap().unwrap();
        drop(reader);

        // The server may still hold the session for the abandoned query.
        let started = Instant::now();
        let mut statement = conn.new_statement().unwrap();
        statement.set_sql_query("SELECT 1 AS one").unwrap();
        let schema = statement.execute_schema().unwrap();
        assert_eq!(schema.field(0).name(), "one");
        let rows: usize = statement
            .execute()
            .unwrap()
            .map(|batch| batch.unwrap().num_rows())
            .sum();
        assert_eq!(rows, 1);
        println!("follow-up queries took {:?}", started.elapsed());
        assert!(started.elapsed() < Duration::from_secs(5));
    }
}
