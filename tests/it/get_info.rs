use adbc_core::constants::{
    ADBC_INFO_DRIVER_ADBC_VERSION, ADBC_INFO_DRIVER_ARROW_VERSION, ADBC_INFO_DRIVER_NAME,
    ADBC_INFO_DRIVER_VERSION, ADBC_INFO_VENDOR_NAME, ADBC_INFO_VENDOR_SQL,
    ADBC_INFO_VENDOR_SUBSTRAIT, ADBC_INFO_VENDOR_VERSION, ADBC_VERSION_1_1_0,
};
use adbc_core::options::InfoCode;
use adbc_core::schemas::GET_INFO_SCHEMA;
use adbc_core::{Connection, Database};
use arrow_array::cast::AsArray;
use arrow_array::types::UInt32Type;
use arrow_array::{BooleanArray, Int64Array, StringArray, UnionArray};
use std::collections::{HashMap, HashSet};

use crate::test_database;

#[test]
fn get_info_all() {
    let db = test_database();
    let conn = db.new_connection().unwrap();

    let reader = conn.get_info(None).unwrap();
    assert_eq!(reader.schema().as_ref(), GET_INFO_SCHEMA.as_ref());

    let batches = reader.collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(batches.len(), 1);
    let batch = &batches[0];

    let names = batch.column(0).as_primitive::<UInt32Type>();
    let values = batch
        .column(1)
        .as_any()
        .downcast_ref::<UnionArray>()
        .expect("info_value should be a UnionArray");

    let mut by_code = HashMap::new();
    for i in 0..batch.num_rows() {
        by_code.insert(names.value(i), i);
    }

    assert_eq!(
        string_value(values, by_code[&ADBC_INFO_VENDOR_NAME]),
        "ClickHouse"
    );

    let vendor_version = string_value(values, by_code[&ADBC_INFO_VENDOR_VERSION]);
    assert!(
        !vendor_version.is_empty(),
        "expected non-empty vendor version, got {vendor_version:?}"
    );

    assert!(bool_value(values, by_code[&ADBC_INFO_VENDOR_SQL]));
    assert!(!bool_value(values, by_code[&ADBC_INFO_VENDOR_SUBSTRAIT]));

    assert_eq!(
        string_value(values, by_code[&ADBC_INFO_DRIVER_NAME]),
        "ADBC ClickHouse Driver"
    );
    assert_eq!(
        string_value(values, by_code[&ADBC_INFO_DRIVER_VERSION]),
        env!("CARGO_PKG_VERSION")
    );
    assert_eq!(
        string_value(values, by_code[&ADBC_INFO_DRIVER_ARROW_VERSION]),
        "58.3.0"
    );
    assert_eq!(
        i64_value(values, by_code[&ADBC_INFO_DRIVER_ADBC_VERSION]),
        ADBC_VERSION_1_1_0 as i64
    );
}

#[test]
fn get_info_filtered() {
    let db = test_database();
    let conn = db.new_connection().unwrap();

    let reader = conn
        .get_info(Some(HashSet::from([
            InfoCode::DriverName,
            InfoCode::VendorSql,
        ])))
        .unwrap();

    let batches = reader.collect::<Result<Vec<_>, _>>().unwrap();
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 2);

    let names = batch.column(0).as_primitive::<UInt32Type>();
    let values = batch
        .column(1)
        .as_any()
        .downcast_ref::<UnionArray>()
        .expect("info_value should be a UnionArray");

    let mut codes = Vec::new();
    for i in 0..batch.num_rows() {
        codes.push(names.value(i));
        match names.value(i) {
            ADBC_INFO_DRIVER_NAME => {
                assert_eq!(string_value(values, i), "ADBC ClickHouse Driver");
            }
            ADBC_INFO_VENDOR_SQL => {
                assert!(bool_value(values, i));
            }
            other => panic!("unexpected info code {other}"),
        }
    }

    codes.sort_unstable();
    assert_eq!(
        codes,
        vec![ADBC_INFO_VENDOR_SQL, ADBC_INFO_DRIVER_NAME]
    );
}

fn string_value(values: &UnionArray, row: usize) -> String {
    assert_eq!(values.type_id(row), 0, "expected string_value type id");
    values
        .value(row)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("string child")
        .value(0)
        .to_string()
}

fn bool_value(values: &UnionArray, row: usize) -> bool {
    assert_eq!(values.type_id(row), 1, "expected bool_value type id");
    values
        .value(row)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .expect("bool child")
        .value(0)
}

fn i64_value(values: &UnionArray, row: usize) -> i64 {
    assert_eq!(values.type_id(row), 2, "expected int64_value type id");
    values
        .value(row)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("int64 child")
        .value(0)
}
