use adbc_clickhouse::ClickhouseDriver;
use adbc_core::options::OptionDatabase;
use adbc_core::{Connection, Database, Driver, Optionable, Statement};
use arrow_array::{RecordBatch, RecordBatchReader, create_array};
use arrow_schema::{DataType, Field, Schema};

mod get_table_schema;

#[test]
fn basic_query() {
    let mut driver = test_driver();

    let mut db = driver.new_database().unwrap();
    db.set_option(OptionDatabase::Uri, "http://localhost:8123/".into())
        .unwrap();

    let mut conn = db.new_connection().unwrap();

    let mut statement = conn.new_statement().unwrap();

    statement
        .set_sql_query("SELECT number, 'test_' || number as name FROM system.numbers LIMIT 10")
        .unwrap();

    let reader = statement.execute().unwrap();

    let schema = reader.schema();

    let batches = reader.collect::<Result<Vec<_>, _>>().unwrap();

    let joined = arrow::compute::concat_batches(&schema, &batches).unwrap();

    // `record_batch!()` sets all columns to be nullable
    let expected = RecordBatch::try_new(
        Schema::new(vec![
            Field::new("number", DataType::UInt64, false),
            Field::new("name", DataType::Utf8, false),
        ])
        .into(),
        vec![
            create_array!(UInt64, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]),
            create_array!(
                Utf8,
                [
                    "test_0", "test_1", "test_2", "test_3", "test_4", "test_5", "test_6", "test_7",
                    "test_8", "test_9"
                ]
            ),
        ],
    )
    .unwrap();

    assert_eq!(joined, expected);
}

fn test_driver() -> ClickhouseDriver {
    let rt = tokio::runtime::Builder::new_multi_thread()
        // We don't want to spawn `num_cpus` threads for every test.
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();

    ClickhouseDriver::init_with(rt)
}
