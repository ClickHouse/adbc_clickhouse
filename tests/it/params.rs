use crate::test_database;
use adbc_core::{Connection, Database, Statement};
use arrow_array::{FixedSizeBinaryArray, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use std::sync::Arc;
use uuid::Uuid;

#[test]
fn params_round_trip_correctly() {
    let params_schema = Arc::new(Schema::new(vec![
        Field::new("field_uuid", DataType::FixedSizeBinary(16), false)
            // ClickHouse Server returns extra metadata over
            // what's specified by the canonical extension type.
            //
            // `.with_extension_type(arrow_schema::extensions::Uuid)`
            // deletes the `ARROW:extension:metadata` key so equality wouldn't work.
            .with_metadata(
                [
                    ("ARROW:extension:name".to_string(), "arrow.uuid".to_string()),
                    ("ARROW:extension:metadata".to_string(), "".to_string()),
                    ("PARQUET:logical_type".to_string(), "UUID".to_string()),
                ]
                .into(),
            ),
    ]));

    let params = RecordBatch::try_new(
        params_schema,
        vec![Arc::new(
            FixedSizeBinaryArray::try_from_iter(
                ["1ad437ba-9298-44b0-9f80-774f0515efb4"
                    .parse::<Uuid>()
                    .unwrap()
                    .into_bytes()]
                .into_iter(),
            )
            .unwrap(),
        )],
    )
    .unwrap();

    let db = test_database();
    let mut conn = db.new_connection().unwrap();
    let mut statement = conn.new_statement().unwrap();

    statement
        .set_sql_query("SELECT {field_uuid:UUID} AS field_uuid")
        .unwrap();

    statement.bind(params.clone()).unwrap();

    let mut records = statement.execute().unwrap();

    let record_batch = records.next().unwrap().unwrap();

    assert_eq!(params, record_batch);
}
