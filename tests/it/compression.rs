//! Tests for the `clickhouse.client.compression` database option.

use crate::test_driver;
use adbc_clickhouse::options;
use adbc_core::error::Status;
use adbc_core::options::OptionDatabase;
use adbc_core::{Connection, Database, Driver, Optionable, Statement};
use arrow_array::RecordBatch;
use arrow_array::cast::AsArray;
use arrow_array::types::UInt64Type;

/// Sum `number` over enough rows to span several response blocks.
fn sum_numbers(compression: Option<&str>) -> u64 {
    let mut opts = vec![(OptionDatabase::Uri, "http://localhost:8123/".into())];
    if let Some(compression) = compression {
        opts.push((options::COMPRESSION.into(), compression.into()));
    }
    let db = test_driver().new_database_with_opts(opts).unwrap();
    let mut conn = db.new_connection().unwrap();
    let mut statement = conn.new_statement().unwrap();
    statement
        .set_sql_query("SELECT number FROM system.numbers LIMIT 1000000")
        .unwrap();

    statement
        .execute()
        .unwrap()
        .collect::<Result<Vec<RecordBatch>, _>>()
        .unwrap()
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_primitive::<UInt64Type>()
                .values()
                .to_vec()
        })
        .sum()
}

#[test]
fn compression_none_and_lz4_return_the_same_data() {
    let expected = 999_999 * 1_000_000 / 2;
    assert_eq!(sum_numbers(None), expected);
    assert_eq!(sum_numbers(Some("lz4")), expected);
    assert_eq!(sum_numbers(Some("none")), expected);
    assert_eq!(sum_numbers(Some("NONE")), expected);
}

#[test]
fn compression_read_back() {
    let mut driver = test_driver();

    let db = driver.new_database().unwrap();
    assert_eq!(
        db.get_option_string(options::COMPRESSION.into()).unwrap(),
        "lz4"
    );

    let db = driver
        .new_database_with_opts([(options::COMPRESSION.into(), "none".into())])
        .unwrap();
    assert_eq!(
        db.get_option_string(options::COMPRESSION.into()).unwrap(),
        "none"
    );
}

#[test]
fn compression_rejects_unknown_values() {
    let err = test_driver()
        .new_database_with_opts([(options::COMPRESSION.into(), "zstd".into())])
        .err()
        .expect("expected an error for an unknown compression method");
    assert_eq!(err.status, Status::InvalidArguments);
}
