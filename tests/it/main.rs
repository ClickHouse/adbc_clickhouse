use adbc_clickhouse::options;
use adbc_core::options::{OptionDatabase, OptionStatement};
use adbc_core::{Connection, Database, Driver, Optionable, Statement};
use arrow_array::cast::AsArray;
use arrow_array::types::Int32Type;
use arrow_array::{
    PrimitiveArray, RecordBatch, RecordBatchIterator, RecordBatchReader, StringArray,
    TimestampMillisecondArray, create_array, record_batch,
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use std::sync::Arc;

mod get_table_schema;

// NOTE: tests run with the `current-thread` runtime by default.
// Set `ADBC_CLICKHOUSE_TEST_MULTI_THREAD=1` to test with the `multi-thread` runtime.
//
// Tests using `test_driver()` use the FFI when the `ffi` feature is enabled.
// By default in this mode, the driver is statically linked.
// To test with dynamic linking, set `ADBC_CLICKHOUSE_TEST_LOAD_DYNAMIC=1`
#[test]
fn basic_query() {
    let mut driver = test_driver();

    let db = driver
        .new_database_with_opts([
            (OptionDatabase::Uri, "http://localhost:8123/".into()),
            (
                options::PRODUCT_INFO.into(),
                format!("adbc_clickhouse_test/{}", env!("CARGO_PKG_VERSION")).into(),
            ),
        ])
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
fn query_with_bind_params() {
    let mut driver = test_driver();

    let db = driver
        .new_database_with_opts([
            (OptionDatabase::Uri, "http://localhost:8123/".into()),
            (
                options::PRODUCT_INFO.into(),
                format!("adbc_clickhouse_test/{}", env!("CARGO_PKG_VERSION")).into(),
            ),
        ])
        .unwrap();

    let mut conn = db.new_connection().unwrap();

    let mut statement = conn.new_statement().unwrap();

    // Don't want to try to cover literally everything, we can let the validation suite handle that
    // https://github.com/ClickHouse/adbc_clickhouse/issues/5
    statement
        .set_sql_query(
            "SELECT {i32:Int32} AS i32, {u64:UInt64} AS u64, {string:String} AS string, \
        {nullable_string:Nullable(String)} AS nullable_string, {f32:Float32} AS f32, \
        {datetime:DateTime64} AS datetime",
        )
        .unwrap();

    // `record_batch!()` sets all columns to be nullable
    let params = RecordBatch::try_new(
        Schema::new(vec![
            Field::new("i32", DataType::Int32, false),
            Field::new("u64", DataType::UInt64, false),
            Field::new("string", DataType::Utf8, false),
            Field::new("nullable_string", DataType::Utf8, true),
            Field::new("f32", DataType::Float32, false),
            Field::new(
                "datetime",
                DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                false,
            ),
        ])
        .into(),
        vec![
            create_array!(Int32, [-1234]),
            create_array!(UInt64, [12345678]),
            create_array!(Utf8, ["foobar"]),
            // FIXME: `None` has incorrect serialization: https://github.com/ClickHouse/clickhouse-rs/issues/384
            create_array!(Utf8, [Some("foobar")]),
            create_array!(Float32, [std::f32::consts::PI]),
            // `record_batch!()` also doesn't support `Timestamp`
            // because it expects an `ident` for the type
            Arc::new(
                TimestampMillisecondArray::new(vec![1769818068000].into(), None)
                    .with_timezone("UTC"),
            ),
        ],
    )
    .unwrap();

    statement.bind(params.clone()).unwrap();

    let mut records = statement.execute().unwrap();

    let record = records
        .next()
        .expect("expected one RecordBatch, got none")
        .unwrap();

    assert_eq!(params, record);
}

#[test]
fn streaming_insert() {
    let mut driver = test_driver();

    let db = driver
        .new_database_with_opts([
            (OptionDatabase::Uri, "http://localhost:8123/".into()),
            (
                options::PRODUCT_INFO.into(),
                format!("adbc_clickhouse_test/{}", env!("CARGO_PKG_VERSION")).into(),
            ),
        ])
        .unwrap();

    let mut conn = db.new_connection().unwrap();

    let mut create_table = conn.new_statement().unwrap();
    create_table
        .set_sql_query(
            "CREATE TEMPORARY TABLE foo(bar Int32, baz String) ENGINE = MergeTree ORDER BY bar",
        )
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

    let mut select = conn.new_statement().unwrap();
    select
        .set_sql_query(
            "SELECT \
         count(*) AS row_count, \
         first_value(bar) AS min_bar, \
         first_value(baz) AS min_baz,
         last_value(bar) AS max_bar, \
         last_value(baz) AS max_baz \
         FROM (SELECT * FROM foo ORDER BY bar)",
        )
        .unwrap();

    let mut reader = select.execute().unwrap();

    let batch = reader.next().expect("expected one record").unwrap();

    assert!(reader.next().is_none(), "expected only one record");
    assert_eq!(batch.num_rows(), 1);

    let expected_count = batch_size * num_batches;

    let expected = RecordBatch::try_new(
        Schema::new(vec![
            Field::new("row_count", DataType::UInt64, false),
            Field::new("min_bar", DataType::Int32, false),
            Field::new("min_baz", DataType::Utf8, false),
            Field::new("max_bar", DataType::Int32, false),
            Field::new("max_baz", DataType::Utf8, false),
        ])
        .into(),
        vec![
            create_array!(UInt64, [expected_count]),
            create_array!(Int32, [1]),
            create_array!(Utf8, ["batch_0_bar_1"]),
            create_array!(Int32, [expected_count as i32]),
            create_array!(Utf8, ["batch_9_bar_50"]),
        ],
    )
    .unwrap();

    assert_eq!(batch, expected);
}

#[test]
fn query_with_product_info() {
    // Note: may be `ManagedStatement` if using the `ffi` feature.
    fn query_user_agent_string(mut statement: impl Statement<Option = OptionStatement>) -> String {
        // Unique query string we can search for
        let query = "SELECT 'adbc_clickhouse/tests/query_with_product_info'";
        statement.set_sql_query(query).unwrap();

        let mut records = statement.execute().unwrap();

        let record = records
            .next()
            .expect("BUG: expected one `RecordBatch`, got none")
            .unwrap();

        assert_eq!(
            record.column(0).as_string::<i32>().value(0),
            "adbc_clickhouse/tests/query_with_product_info"
        );

        drop(records);

        let query_id = statement
            .get_option_string(options::QUERY_ID.into())
            .unwrap();

        // Overwrite the query ID to avoid confusion.
        statement
            .set_option(
                options::QUERY_ID.into(),
                format!("adbc_clickhouse_test_query_{}", rand::random::<u64>()).into(),
            )
            .unwrap();

        // Flush the query logs or else it won't appear
        statement
            .set_sql_query("SYSTEM FLUSH LOGS query_log")
            .unwrap();
        statement.execute_update().unwrap();

        statement
            .set_sql_query(
                "SELECT query, http_user_agent \
             FROM system.query_log \
             WHERE query = {query:String} AND query_id = {query_id:String} \
             ORDER BY event_time DESC",
            )
            .unwrap();

        statement
            .bind(record_batch!(("query", Utf8, [query]), ("query_id", Utf8, [query_id])).unwrap())
            .unwrap();

        let mut records = statement.execute().unwrap();

        let record = records
            .next()
            .expect("BUG: expected one `RecordBatch`, got none")
            .unwrap();

        record
            .column_by_name("http_user_agent")
            .unwrap()
            .as_string::<i32>()
            .value(0)
            .into()
    }

    let mut driver = test_driver();

    let db_product_info = format!("adbc_clickhouse_test/{}", env!("CARGO_PKG_VERSION"));

    let db = driver
        .new_database_with_opts([
            (OptionDatabase::Uri, "http://localhost:8123/".into()),
            (options::PRODUCT_INFO.into(), db_product_info.clone().into()),
        ])
        .unwrap();

    let mut conn = db.new_connection().unwrap();

    // Product info should be inherited by the connection
    let user_agent = query_user_agent_string(conn.new_statement().unwrap());
    assert!(
        user_agent.starts_with(&db_product_info),
        "expected User-Agent string {user_agent:?} to contain {db_product_info:?}"
    );

    let conn_product_info = format!("test_conn/0.0.0 {db_product_info}");
    conn.set_option(
        options::PRODUCT_INFO.into(),
        conn_product_info.clone().into(),
    )
    .unwrap();

    // The connection should have its own product_info, of course.
    let user_agent = query_user_agent_string(conn.new_statement().unwrap());
    assert!(
        user_agent.starts_with(&conn_product_info),
        "expected User-Agent string {user_agent:?} to contain {conn_product_info:?}"
    );

    // A different connection should inherit the product_info of the database
    let user_agent = query_user_agent_string(db.new_connection().unwrap().new_statement().unwrap());
    assert!(
        user_agent.starts_with(&db_product_info),
        "expected User-Agent string {user_agent:?} to contain {db_product_info:?}"
    );

    let statement_product_info = format!("test_statement/0.0.1-alpha.1 {conn_product_info}");

    let mut statement = conn.new_statement().unwrap();
    // FIXME: https://github.com/apache/arrow-adbc/issues/3913
    statement
        .set_option(
            options::PRODUCT_INFO.into(),
            statement_product_info.clone().into(),
        )
        .unwrap();

    let user_agent = query_user_agent_string(statement);
    assert!(
        user_agent.starts_with(&statement_product_info),
        "expected User-Agent string {user_agent:?} to contain {statement_product_info:?}"
    );

    // A different statement should inherit the product_info of the connection
    let user_agent = query_user_agent_string(conn.new_statement().unwrap());
    assert!(
        user_agent.starts_with(&conn_product_info),
        "expected User-Agent string {user_agent:?} to contain {conn_product_info:?}"
    );
}

#[test]
fn query_with_session_id() {
    /// Execute `SELECT {foo:String} AS foo` on the given statement and return the result.
    fn get_foo(mut statement: impl Statement<Option = OptionStatement>) -> String {
        statement
            .set_sql_query("SELECT {foo:String} AS foo")
            .unwrap();

        let mut records = statement.execute().unwrap();

        let record = records
            .next()
            .expect("expected one RecordBatch, got none")
            .unwrap();

        record
            .column_by_name("foo")
            .expect("expected column `foo`")
            .as_string::<i32>()
            .value(0)
            .into()
    }

    let mut driver = test_driver();

    let db = driver
        .new_database_with_opts([
            (OptionDatabase::Uri, "http://localhost:8123/".into()),
            (
                options::PRODUCT_INFO.into(),
                format!("adbc_clickhouse_test/{}", env!("CARGO_PKG_VERSION")).into(),
            ),
        ])
        .unwrap();

    let mut conn = db.new_connection().unwrap();
    let mut conn2 = db.new_connection().unwrap();

    let session_id3 = format!("adbc_clickhouse_test_{}", rand::random::<u64>());

    // In 3 different sessions, set some session-local state that the other queries will rely on
    let mut set_statement = conn.new_statement().unwrap();
    set_statement
        .set_sql_query("SET param_foo = 'foobar'")
        .unwrap();
    set_statement.execute_update().unwrap();

    let mut set_statement2 = conn2.new_statement().unwrap();
    set_statement2
        .set_sql_query("SET param_foo = 'foobar2'")
        .unwrap();
    set_statement2.execute_update().unwrap();

    // Same connection, different session
    let mut set_statement3 = conn2.new_statement().unwrap();
    set_statement3
        .set_option(options::SESSION_ID.into(), session_id3.clone().into())
        .unwrap();
    set_statement3
        .set_sql_query("SET param_foo = 'foobar3'")
        .unwrap();
    set_statement3.execute_update().unwrap();

    assert_eq!(get_foo(conn.new_statement().unwrap()), "foobar");

    assert_eq!(get_foo(conn2.new_statement().unwrap()), "foobar2");

    let mut statement3 = conn2.new_statement().unwrap();
    statement3
        .set_option(options::SESSION_ID.into(), session_id3.into())
        .unwrap();

    assert_eq!(get_foo(statement3), "foobar3");
}

#[cfg(not(feature = "ffi"))]
pub(crate) fn test_driver() -> adbc_clickhouse::ClickhouseDriver {
    if let Ok(s) = std::env::var("ADBC_CLICKHOUSE_TEST_MULTI_THREAD")
        && s.eq_ignore_ascii_case("1")
    {
        let rt = tokio::runtime::Builder::new_multi_thread()
            // We don't want to spawn `num_cpus` threads for every test.
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();

        adbc_clickhouse::ClickhouseDriver::init_with(rt.into())
    } else {
        adbc_clickhouse::ClickhouseDriver::init()
    }
}

#[cfg(feature = "ffi")]
pub(crate) fn test_driver() -> adbc_driver_manager::ManagedDriver {
    use adbc_core::options::AdbcVersion;

    if let Ok(s) = std::env::var("ADBC_CLICKHOUSE_TEST_LOAD_DYNAMIC")
        && s.eq_ignore_ascii_case("1")
    {
        adbc_driver_manager::ManagedDriver::load_dynamic_from_name(
            "adbc_clickhouse",
            None,
            AdbcVersion::V110,
        )
        .unwrap()
    } else {
        // Explicit coercion required to make this happy
        let init: adbc_ffi::FFI_AdbcDriverInitFunc = adbc_clickhouse::AdbcClickhouseInit;
        adbc_driver_manager::ManagedDriver::load_static(&init, AdbcVersion::V110).unwrap()
    }
}
