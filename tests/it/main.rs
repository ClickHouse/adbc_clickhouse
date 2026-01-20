use adbc_clickhouse::ClickhouseDriver;
use adbc_core::options::OptionDatabase;
use adbc_core::{Connection, Database, Driver, Optionable, Statement};
use arrow_array::types::Int32Type;
use arrow_array::{
    PrimitiveArray, RecordBatch, RecordBatchIterator, RecordBatchReader, StringArray, create_array,
};
use arrow_schema::{DataType, Field, Schema};
use std::sync::Arc;

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

#[test]
fn streaming_insert() {
    let mut driver = test_driver();

    let mut db = driver.new_database().unwrap();
    db.set_option(OptionDatabase::Uri, "http://localhost:8123/".into())
        .unwrap();

    let mut conn = db.new_connection().unwrap();

    let mut create_table = conn.new_statement().unwrap();
    create_table
        .set_sql_query("CREATE TEMPORARY TABLE foo(bar Int32, baz String) ORDER BY bar")
        .unwrap();

    create_table.execute_update().unwrap();

    let batch_size = 5;
    let num_batches = 10;
    let mut next_id = 1..;

    let mut batches = Vec::new();
    let schema = Arc::new(Schema::new(vec![
        Field::new("bar", DataType::Int32, false),
        Field::new("baz", DataType::Utf8, false),
    ]));

    for batch in 0..num_batches {
        let bars: PrimitiveArray<Int32Type> = (0..batch_size)
            .zip(&mut next_id)
            .map(|(_, id)| id)
            .collect();

        let bazzes: StringArray = bars
            .iter()
            .filter_map(|bar| {
                let bar = bar?;
                Some(format!("batch_{batch}_bar_{bar}"))
            })
            .collect::<Vec<String>>()
            .into();

        batches.push(RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(bars), Arc::new(bazzes)],
        ));
    }

    let mut insert = conn.new_statement().unwrap();
    insert
        .set_sql_query("INSERT INTO foo(bar, baz) FORMAT ArrowStream")
        .unwrap();

    let batches = RecordBatchIterator::new(batches, schema.clone());

    insert.bind_stream(Box::new(batches)).unwrap();

    insert.execute_update().unwrap();
}

pub(crate) fn test_driver() -> ClickhouseDriver {
    let rt = tokio::runtime::Builder::new_multi_thread()
        // We don't want to spawn `num_cpus` threads for every test.
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();

    ClickhouseDriver::init_with(rt)
}
