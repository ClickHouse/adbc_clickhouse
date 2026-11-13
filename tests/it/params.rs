use crate::test_database;
use adbc_core::options::OptionStatement;
use adbc_core::{Connection, Database, Optionable, Statement};
use arrow_array::{FixedSizeBinaryArray, RecordBatch, create_array};
use arrow_schema::{DataType, Field, Schema};
use std::sync::Arc;
use uuid::Uuid;

#[test]
fn binary_strings_round_trip() {
    let params_schema = Arc::new(Schema::new(vec![
        Field::new("field_uuid", DataType::FixedSizeBinary(16), false)
            // Match the canonical extension metadata ClickHouse returns.
            // Do not include server-version-specific keys such as
            // `PARQUET:logical_type`.
            //
            // `.with_extension_type(arrow_schema::extensions::Uuid)`
            // deletes the `ARROW:extension:metadata` key so equality wouldn't work.
            .with_metadata(
                [
                    ("ARROW:extension:name".to_string(), "arrow.uuid".to_string()),
                    ("ARROW:extension:metadata".to_string(), "".to_string()),
                ]
                .into(),
            ),
        Field::new("field_binary", DataType::Binary, false),
        Field::new(
            "field_fixed_size_binary",
            DataType::FixedSizeBinary(64),
            false,
        ),
    ]));

    let params = RecordBatch::try_new(
        params_schema,
        vec![
            Arc::new(
                FixedSizeBinaryArray::try_from_iter(
                    ["1ad437ba-9298-44b0-9f80-774f0515efb4"
                        .parse::<Uuid>()
                        .unwrap()
                        .into_bytes()]
                    .into_iter(),
                )
                .unwrap(),
            ),
            create_array!(Binary, [b"Lorem ipsum dolor sit emet"]),
            Arc::new(FixedSizeBinaryArray::try_from_iter([[0xFF; 64]].into_iter()).unwrap()),
        ],
    )
    .unwrap();

    let db = test_database();
    let mut conn = db.new_connection().unwrap();
    let mut statement = conn.new_statement().unwrap();

    statement
        .set_sql_query(
            "SELECT \
             {field_uuid:UUID} AS field_uuid, \
             {field_binary:String} AS field_binary, \
             {field_fixed_size_binary:FixedString(64)} AS field_fixed_size_binary",
        )
        .unwrap();

    statement
        // Otherwise binary gets returned as `String`
        // since ClickHouse doesn't have a separate binary type
        .set_option(
            OptionStatement::Other(adbc_clickhouse::options::OUTPUT_STRING_AS_STRING.into()),
            // ADBC convention
            "false".into(),
        )
        .unwrap();

    statement.bind(params.clone()).unwrap();

    let mut records = statement.execute().unwrap();

    let record_batch = records.next().unwrap().unwrap();

    // Some server versions add `PARQUET:logical_type` metadata to UUID columns;
    // drop it so the comparison doesn't depend on the server version.
    let normalized_schema = Schema::new_with_metadata(
        record_batch
            .schema()
            .fields()
            .iter()
            .map(|field| {
                let mut metadata = field.metadata().clone();
                metadata.remove("PARQUET:logical_type");
                field.as_ref().clone().with_metadata(metadata)
            })
            .collect::<Vec<_>>(),
        record_batch.schema().metadata().clone(),
    );
    let record_batch =
        RecordBatch::try_new(normalized_schema.into(), record_batch.columns().to_vec()).unwrap();

    assert_eq!(params, record_batch);
}
