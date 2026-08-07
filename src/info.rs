//! Helpers for building [`Connection::get_info`][adbc_core::Connection::get_info] results.
//!
//! The result schema is defined by [`adbc_core::schemas::GET_INFO_SCHEMA`].

use adbc_core::error::{Error, Status};
use adbc_core::options::InfoCode;
use adbc_core::schemas::GET_INFO_SCHEMA;
use arrow_array::builder::{
    ArrayBuilder, BooleanBuilder, Int32Builder, Int64Builder, ListBuilder, MapBuilder,
    MapFieldNames, StringBuilder, UInt32Builder,
};
use arrow_array::{ArrayRef, RecordBatch, RecordBatchIterator, RecordBatchReader, UnionArray};
use arrow_buffer::ScalarBuffer;
use arrow_schema::{DataType, UnionMode};
use std::collections::HashSet;
use std::sync::Arc;

type Result<T, E = Error> = std::result::Result<T, E>;

/// Builds a dense-union `get_info` result matching [`GET_INFO_SCHEMA`].
///
/// Pattern adapted from the ADBC Driver Foundry [`InfoBuilder`][info-builder].
///
/// [info-builder]: https://github.com/adbc-drivers/driverbase-rs/blob/main/driverbase/src/lib.rs
pub(crate) struct InfoBuilder {
    info_name: UInt32Builder,
    type_id: Vec<i8>,
    offset: Vec<i32>,
    string_value: StringBuilder,
    bool_value: BooleanBuilder,
    int64_value: Int64Builder,
    int32_bitmask: Int32Builder,
    string_list: ListBuilder<StringBuilder>,
    int32_to_int32_list_map: MapBuilder<Int32Builder, ListBuilder<Int32Builder>>,
}

impl InfoBuilder {
    const CODE_STRING: i8 = 0;
    const CODE_BOOL: i8 = 1;
    const CODE_INT64: i8 = 2;

    pub(crate) fn new() -> Self {
        Self {
            info_name: UInt32Builder::new(),
            type_id: Vec::new(),
            offset: Vec::new(),
            string_value: StringBuilder::new(),
            bool_value: BooleanBuilder::new(),
            int64_value: Int64Builder::new(),
            int32_bitmask: Int32Builder::new(),
            string_list: ListBuilder::new(StringBuilder::new()),
            // Match `GET_INFO_SCHEMA` field names (`key`/`value`, not MapBuilder defaults).
            int32_to_int32_list_map: MapBuilder::new(
                Some(MapFieldNames {
                    entry: "entries".to_string(),
                    key: "key".to_string(),
                    value: "value".to_string(),
                }),
                Int32Builder::new(),
                ListBuilder::new(Int32Builder::new()),
            ),
        }
    }

    pub(crate) fn add_string(&mut self, code: InfoCode, value: impl AsRef<str>) {
        self.info_name.append_value(u32::from(&code));
        self.type_id.push(Self::CODE_STRING);
        self.offset.push(self.string_value.len() as i32);
        self.string_value.append_value(value);
    }

    pub(crate) fn add_bool(&mut self, code: InfoCode, value: bool) {
        self.info_name.append_value(u32::from(&code));
        self.type_id.push(Self::CODE_BOOL);
        self.offset.push(self.bool_value.len() as i32);
        self.bool_value.append_value(value);
    }

    pub(crate) fn add_i64(&mut self, code: InfoCode, value: i64) {
        self.info_name.append_value(u32::from(&code));
        self.type_id.push(Self::CODE_INT64);
        self.offset.push(self.int64_value.len() as i32);
        self.int64_value.append_value(value);
    }

    pub(crate) fn finish(mut self) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        let num_rows = self.type_id.len();
        let info_name = self.info_name.finish();

        let type_ids = ScalarBuffer::from(self.type_id);
        let offsets = ScalarBuffer::from(self.offset);

        let children: Vec<ArrayRef> = vec![
            Arc::new(self.string_value.finish()),
            Arc::new(self.bool_value.finish()),
            Arc::new(self.int64_value.finish()),
            Arc::new(self.int32_bitmask.finish()),
            Arc::new(self.string_list.finish()),
            Arc::new(self.int32_to_int32_list_map.finish()),
        ];

        let schema = GET_INFO_SCHEMA.clone();
        let DataType::Union(union_fields, UnionMode::Dense) = schema.field(1).data_type() else {
            return Err(Error::with_message_and_status(
                "GET_INFO_SCHEMA.info_value is not a dense union",
                Status::Internal,
            ));
        };

        let info_value = UnionArray::try_new(
            union_fields.clone(),
            type_ids,
            Some(offsets),
            children,
        )
        .map_err(|e| {
            Error::with_message_and_status(
                format!("error building get_info union array: {e}"),
                Status::Internal,
            )
        })?;

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(info_name), Arc::new(info_value)],
        )
        .map_err(|e| {
            Error::with_message_and_status(
                format!("error building get_info record batch: {e}"),
                Status::Internal,
            )
        })?;

        debug_assert_eq!(batch.num_rows(), num_rows);

        Ok(Box::new(RecordBatchIterator::new(
            std::iter::once(Ok(batch)),
            schema,
        )))
    }
}

/// Whether `codes` requests `code` (or requests everything when `None`).
#[inline]
pub(crate) fn wants(codes: Option<&HashSet<InfoCode>>, code: InfoCode) -> bool {
    codes.is_none_or(|set| set.contains(&code))
}
