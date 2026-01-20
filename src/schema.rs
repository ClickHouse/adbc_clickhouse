use adbc_core::error::{Error, Status};
use arrow_schema::{DataType, Field, Fields, Schema, TimeUnit};
use clickhouse::Client;
use clickhouse_types::DataTypeNode;
use clickhouse_types::data_types::{DateTimePrecision, DecimalType, EnumType};
use serde::{Deserialize, Deserializer};

type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(clickhouse::Row, serde::Deserialize)]
#[allow(dead_code)]
struct Column {
    name: String,
    r#type: String,
    // FIXME: `clickhouse::Row` derive doesn't allow missing columns
    default_type: String,
    default_expression: String,
    comment: String,
    codec_expression: String,
    ttl_expression: String,
}

#[derive(clickhouse::Row, serde::Deserialize)]
#[serde(default)]
struct Settings {
    /// <https://clickhouse.com/docs/operations/settings/formats#output_format_arrow_string_as_string>
    #[serde(deserialize_with = "deserialize_bool_setting")]
    string_as_string: bool,
    /// <https://clickhouse.com/docs/operations/settings/formats#output_format_arrow_fixed_string_as_fixed_byte_array>
    #[serde(deserialize_with = "deserialize_bool_setting")]
    fixed_string_as_fixed_byte_array: bool,
    /// <https://clickhouse.com/docs/operations/settings/formats#output_format_arrow_low_cardinality_as_dictionary>
    #[serde(deserialize_with = "deserialize_bool_setting")]
    low_cardinality_as_dictionary: bool,
    /// <https://clickhouse.com/docs/operations/settings/formats#output_format_arrow_use_signed_indexes_for_dictionary>
    #[serde(deserialize_with = "deserialize_bool_setting")]
    use_signed_indexes_for_dictionary: bool,
    /// <https://clickhouse.com/docs/operations/settings/formats#output_format_arrow_use_64_bit_indexes_for_dictionary>
    #[serde(deserialize_with = "deserialize_bool_setting")]
    use_64_bit_indexes_for_dictionary: bool,
}

impl Default for Settings {
    fn default() -> Self {
        // Note: docs don't match!
        // https://github.com/ClickHouse/ClickHouse/blob/3196ab525aa6f1fef8d367db7610b765f7737f01/src/Processors/Formats/Impl/CHColumnToArrowColumn.h#L19-L34
        // This doesn't actually mean the docs are wrong,
        // because the defaults in-code don't actually match the defaults in the system catalog.
        // We should get the correct values in `Settings::query()`.
        //
        // The changes are documented in this file:
        // https://github.com/ClickHouse/ClickHouse/blob/7b9c8d9190d8b5cfcd92283277afaa119f25dfc8/src/Core/SettingsChangesHistory.cpp#L729
        Settings {
            string_as_string: false,
            fixed_string_as_fixed_byte_array: true,
            low_cardinality_as_dictionary: false,
            use_signed_indexes_for_dictionary: false,
            use_64_bit_indexes_for_dictionary: false,
        }
    }
}

impl Settings {
    async fn query(client: &Client) -> Result<Self> {
        use std::fmt::Write;

        let settings = [
            "string_as_string",
            "fixed_string_as_fixed_byte_array",
            "low_cardinality_as_dictionary",
            "use_signed_indexes_for_dictionary",
            "use_64_bit_indexes_for_dictionary",
        ];

        let mut sql = "SELECT ".to_string();

        let mut pushed = false;
        for setting in settings {
            if pushed {
                sql.push_str(", ");
            }

            write!(
                sql,
                // Only non-default settings appear in the `system.settings` view
                // `anyOrNull` lets us define the default ourselves, because it's not always `false`
                "(SELECT anyOrNull(value) FROM system.settings WHERE name = 'output_format_arrow_{setting}') AS {setting}"
            ).expect("writing to a `String` should be infallible");

            pushed = true;
        }

        client.query(&sql).fetch_one::<Self>().await.map_err(|e| {
            Error::with_message_and_status(
                format!("error fetching Arrow format settings: {e}"),
                Status::Internal,
            )
        })
    }
}

pub async fn of_table(
    client: &Client,
    db: Option<&str>,
    table: &str,
) -> adbc_core::error::Result<Schema> {
    let query = if let Some(db) = db {
        client
            .query("DESCRIBE TABLE {db:Identifier}.{table:Identifier}")
            .param("db", db)
            .param("table", table)
    } else {
        client
            .query("DESCRIBE TABLE {table:Identifier}")
            .param("table", table)
    };

    let mut columns = query.fetch::<Column>().map_err(|e| {
        Error::with_message_and_status(
            format!("error beginning fetch of table schema: {e}"),
            Status::Internal,
        )
    })?;

    // Some type mappings can be affected by dynamic settings,
    // so we have to query those as well.
    //
    // If `get_table_schema()` turns out to be performance-sensitive,
    // we could issue this query at the same time as the `DESCRIBE TABLE`.
    let settings = Settings::query(client).await?;

    let mut fields = Vec::new();

    while let Some(column) = columns.next().await.map_err(|e| {
        Error::with_message_and_status(
            if let Some(db) = db {
                format!("error retrieving schema of table {db:?}.{table:?}: {e}")
            } else {
                format!("error retrieving schema of table {table:?}: {e}")
            },
            Status::Internal,
        )
    })? {
        let field = column_to_arrow(&column, &settings).map_err(|e| Error {
            message: if let Some(db) = db {
                format!(
                    "error mapping column {:?} of table {db:?}.{table:?}: {}",
                    column.name, e.message
                )
            } else {
                format!(
                    "error mapping column {:?} of table {table:?}: {}",
                    column.name, e.message
                )
            },
            ..e
        })?;

        fields.push(field);
    }

    Ok(Schema::new(fields))
}

fn column_to_arrow(column: &Column, settings: &Settings) -> Result<Field> {
    let ch_type = DataTypeNode::new(&column.r#type).map_err(|e| {
        Error::with_message_and_status(
            format!("error parsing type {:?}: {e}", column.r#type),
            Status::Internal,
        )
    })?;

    // CH defaults to not-null
    let mut nullable = false;

    let ty = ch_type_to_arrow(settings, &ch_type, &mut nullable)?;

    let mut field = Field::new(&column.name, ty, nullable);

    field
        .metadata_mut()
        .insert("CLICKHOUSE:type".to_string(), column.r#type.to_string());

    Ok(field)
}

// https://github.com/ClickHouse/ClickHouse/blob/3196ab525aa6f1fef8d367db7610b765f7737f01/src/Processors/Formats/Impl/CHColumnToArrowColumn.cpp#L939
fn ch_type_to_arrow(
    settings: &Settings,
    ch_type: &DataTypeNode,
    nullable: &mut bool,
) -> Result<DataType> {
    match ch_type {
        DataTypeNode::Bool => Ok(DataType::Boolean),
        DataTypeNode::UInt8 => Ok(DataType::UInt8),
        DataTypeNode::UInt16 => Ok(DataType::UInt16),
        DataTypeNode::UInt32 => Ok(DataType::UInt32),
        DataTypeNode::UInt64 => Ok(DataType::UInt64),
        // https://github.com/ClickHouse/ClickHouse/blob/3196ab525aa6f1fef8d367db7610b765f7737f01/src/Processors/Formats/Impl/CHColumnToArrowColumn.cpp#L93-L96
        DataTypeNode::UInt128 => Ok(DataType::FixedSizeBinary(16)),
        DataTypeNode::UInt256 => Ok(DataType::FixedSizeBinary(32)),
        DataTypeNode::Int8 => Ok(DataType::Int8),
        DataTypeNode::Int16 => Ok(DataType::Int16),
        DataTypeNode::Int32 => Ok(DataType::Int32),
        DataTypeNode::Int64 => Ok(DataType::Int64),
        // https://github.com/ClickHouse/ClickHouse/blob/3196ab525aa6f1fef8d367db7610b765f7737f01/src/Processors/Formats/Impl/CHColumnToArrowColumn.cpp#L93-L96
        DataTypeNode::Int128 => Ok(DataType::FixedSizeBinary(16)),
        DataTypeNode::Int256 => Ok(DataType::FixedSizeBinary(32)),
        DataTypeNode::Float32 => Ok(DataType::Float32),
        DataTypeNode::Float64 => Ok(DataType::Float64),
        // Note: not the same as IEE-754 standard half-precision floating point.
        // https://clickhouse.com/docs/sql-reference/data-types/float#bfloat16
        // There doesn't appear to be a supported conversion in CH
        // DataTypeNode::BFloat16 => {}
        DataTypeNode::Decimal(scale, precision, kind) => arrow_decimal(scale, precision, kind),
        // https://github.com/ClickHouse/ClickHouse/blob/3196ab525aa6f1fef8d367db7610b765f7737f01/src/Processors/Formats/Impl/CHColumnToArrowColumn.cpp#L1035-L1039
        DataTypeNode::FixedString(len) if settings.fixed_string_as_fixed_byte_array => {
            let len = i32::try_from(*len).map_err(|_| {
                Error::with_message_and_status(
                    format!("FixedString({len}) size out of range for Arrow fixed-size binary"),
                    Status::InvalidState,
                )
            })?;

            Ok(DataType::FixedSizeBinary(len))
        }
        // https://github.com/ClickHouse/ClickHouse/blob/3196ab525aa6f1fef8d367db7610b765f7737f01/src/Processors/Formats/Impl/CHColumnToArrowColumn.cpp#L1041-L1042
        DataTypeNode::String | DataTypeNode::FixedString(_) => {
            if settings.string_as_string {
                Ok(DataType::Utf8)
            } else {
                Ok(DataType::Binary)
            }
        }
        // Surprisingly, I don't see a mapping for UUID in the CH code despite it being supported as
        // a canonical extension type in Arrow: https://arrow.apache.org/docs/format/CanonicalExtensions.html#uuid
        // DataTypeNode::UUID => {}
        // https://github.com/ClickHouse/ClickHouse/blob/3196ab525aa6f1fef8d367db7610b765f7737f01/src/Processors/Formats/Impl/CHColumnToArrowColumn.cpp#L86
        DataTypeNode::Date => Ok(DataType::UInt16),
        DataTypeNode::Date32 => Ok(DataType::Date32),
        // https://github.com/ClickHouse/ClickHouse/blob/3196ab525aa6f1fef8d367db7610b765f7737f01/src/Processors/Formats/Impl/CHColumnToArrowColumn.cpp#L87C1-L87C114
        DataTypeNode::DateTime(_) => Ok(DataType::UInt32),
        DataTypeNode::DateTime64(precision, tz) => arrow_timestamp(precision, tz),
        // No mappings in CHColumnToArrowColumn.cpp
        // DataTypeNode::Time => {}
        // DataTypeNode::Time64(_) => {}
        // DataTypeNode::Interval(_) => {}
        // https://github.com/ClickHouse/ClickHouse/blob/3196ab525aa6f1fef8d367db7610b765f7737f01/src/Processors/Formats/Impl/CHColumnToArrowColumn.cpp#L1050
        DataTypeNode::IPv4 => Ok(DataType::UInt32),
        // https://github.com/ClickHouse/ClickHouse/blob/3196ab525aa6f1fef8d367db7610b765f7737f01/src/Processors/Formats/Impl/CHColumnToArrowColumn.cpp#L1047
        DataTypeNode::IPv6 => Ok(DataType::FixedSizeBinary(16)),
        DataTypeNode::Nullable(inner) => {
            *nullable = true;
            ch_type_to_arrow(settings, inner, &mut false)
        }
        // Note: LowCardinality is not necessarily as simple as unwrapping the type
        DataTypeNode::LowCardinality(inner) => low_cardinality_to_arrow(settings, inner, nullable),
        // https://github.com/ClickHouse/ClickHouse/blob/3196ab525aa6f1fef8d367db7610b765f7737f01/src/Processors/Formats/Impl/CHColumnToArrowColumn.cpp#L976-L983
        DataTypeNode::Array(inner) => {
            let mut nullable = false;
            let item_ty = ch_type_to_arrow(settings, inner, &mut nullable)?;

            Ok(DataType::List(Field::new("item", item_ty, nullable).into()))
        }
        DataTypeNode::Tuple(types) => tuple_to_struct(settings, types),
        DataTypeNode::Enum(enum_ty, _) => match enum_ty {
            // https://github.com/ClickHouse/ClickHouse/blob/3196ab525aa6f1fef8d367db7610b765f7737f01/src/Processors/Formats/Impl/CHColumnToArrowColumn.cpp#L75
            EnumType::Enum8 => Ok(DataType::Int8),
            // https://github.com/ClickHouse/ClickHouse/blob/3196ab525aa6f1fef8d367db7610b765f7737f01/src/Processors/Formats/Impl/CHColumnToArrowColumn.cpp#L78
            EnumType::Enum16 => Ok(DataType::Int16),
        },
        // https://github.com/ClickHouse/ClickHouse/blob/3196ab525aa6f1fef8d367db7610b765f7737f01/src/Processors/Formats/Impl/CHColumnToArrowColumn.cpp#L1012-L1027
        DataTypeNode::Map([key_ty, val_ty]) => arrow_map(settings, key_ty, val_ty),
        // Not supported
        // DataTypeNode::AggregateFunction(_, _) => {}
        // DataTypeNode::Variant(_) => {}
        // DataTypeNode::Dynamic => {}
        // As a subset of `Dynamic`, it appears `JSON` is not supported even though
        // it could just be encoded as a string
        // DataTypeNode::JSON => {}
        // Geo types are not supported
        // DataTypeNode::Point => {}
        // DataTypeNode::Ring => {}
        // DataTypeNode::LineString => {}
        // DataTypeNode::MultiLineString => {}
        // DataTypeNode::Polygon => {}
        // DataTypeNode::MultiPolygon => {}
        _ => Err(Error::with_message_and_status(
            format!(
                "conversion of ClickHouse type {ch_type:?} to Arrow is not currently supported"
            ),
            Status::NotImplemented,
        )),
    }
}

fn arrow_decimal(scale: &u8, precision: &u8, kind: &DecimalType) -> Result<DataType> {
    // CH defers the validation/clamping of `scale` and `precision` to `arrow-cpp`:
    // https://github.com/ClickHouse/ClickHouse/blob/3196ab525aa6f1fef8d367db7610b765f7737f01/src/Processors/Formats/Impl/CHColumnToArrowColumn.cpp#L965

    // Arrow format allows negative precision which right-pads the decimal representation
    // A naive cast would produce an invalid value
    //
    // Max precision for `Decimal256` is 76 so an overflow here likely indicates data corruption:
    // https://clickhouse.com/docs/sql-reference/data-types/decimal#decimal-value-ranges
    let precision = i8::try_from(*precision).map_err(|_| {
        Error::with_message_and_status(
            format!("{kind} precision out of range: {precision}"),
            Status::InvalidData,
        )
    })?;

    match kind {
        DecimalType::Decimal32 => Ok(DataType::Decimal32(*scale, precision)),
        DecimalType::Decimal64 => Ok(DataType::Decimal64(*scale, precision)),
        DecimalType::Decimal128 => Ok(DataType::Decimal128(*scale, precision)),
        DecimalType::Decimal256 => Ok(DataType::Decimal256(*scale, precision)),
    }
}

fn arrow_timestamp(precision: &DateTimePrecision, tz: &Option<String>) -> Result<DataType> {
    // https://github.com/ClickHouse/ClickHouse/blob/3196ab525aa6f1fef8d367db7610b765f7737f01/src/Processors/Formats/Impl/CHColumnToArrowColumn.cpp#L927
    let time_unit = match precision {
        DateTimePrecision::Precision0 => TimeUnit::Second,
        DateTimePrecision::Precision1
        | DateTimePrecision::Precision2
        | DateTimePrecision::Precision3 => TimeUnit::Millisecond,
        DateTimePrecision::Precision4
        | DateTimePrecision::Precision5
        | DateTimePrecision::Precision6 => TimeUnit::Microsecond,
        _ => TimeUnit::Nanosecond,
    };

    Ok(DataType::Timestamp(
        time_unit,
        tz.as_deref().map(Into::into),
    ))
}

fn arrow_map(
    settings: &Settings,
    key_ty: &DataTypeNode,
    val_ty: &DataTypeNode,
) -> Result<DataType> {
    let key_ty = ch_type_to_arrow(settings, key_ty, &mut false)?;

    let mut val_nullable = false;
    let val_ty = ch_type_to_arrow(settings, val_ty, &mut val_nullable)?;

    // `arrow::map()` in C++, defined here (`keys_sorted` defaults to `false`):
    // https://github.com/apache/arrow/blob/07e784ca5032f5e17fe648100a9863e7114cb620/cpp/src/arrow/type.cc#L3217-L3221
    // ultimately calls the `MapType` constructors here:
    // https://github.com/apache/arrow/blob/07e784ca5032f5e17fe648100a9863e7114cb620/cpp/src/arrow/type.cc#L1028-L1043
    let struct_fields = vec![
        Field::new("key", key_ty, false),
        Field::new("value", val_ty, val_nullable),
    ];

    Ok(DataType::Map(
        Field::new("entries", DataType::Struct(struct_fields.into()), false).into(),
        false, // is_sorted
    ))
}

// https://github.com/ClickHouse/ClickHouse/blob/3196ab525aa6f1fef8d367db7610b765f7737f01/src/Processors/Formats/Impl/CHColumnToArrowColumn.cpp#L985-L999
fn tuple_to_struct(settings: &Settings, types: &[DataTypeNode]) -> Result<DataType> {
    // FIXME: `clickhouse-types` doesn't support tuples with named fields
    let fields = types
        .iter()
        .enumerate()
        .map(|(i, ty)| {
            let mut nullable = false;
            let arrow_ty = ch_type_to_arrow(settings, ty, &mut nullable).map_err(|e| Error {
                message: format!(
                    "error mapping field {i} of tuple type {types:?}: {}",
                    e.message
                ),
                ..e
            })?;

            Ok(Field::new(i.to_string(), arrow_ty, nullable))
        })
        .collect::<Result<Fields>>()?;

    Ok(DataType::Struct(fields))
}

// https://github.com/ClickHouse/ClickHouse/blob/3196ab525aa6f1fef8d367db7610b765f7737f01/src/Processors/Formats/Impl/CHColumnToArrowColumn.cpp#L1001-L1010
fn low_cardinality_to_arrow(
    settings: &Settings,
    inner_ty: &DataTypeNode,
    nullable: &mut bool,
) -> Result<DataType> {
    let inner_ty = ch_type_to_arrow(settings, inner_ty, nullable)?;

    if !settings.low_cardinality_as_dictionary {
        return Ok(inner_ty);
    }

    // CH can be configured to encode LowCardinality as an Arrow dictionary,
    // but the index type used actually sorta depends on the data, as well as a couple other settings:
    // https://github.com/ClickHouse/ClickHouse/blob/3196ab525aa6f1fef8d367db7610b765f7737f01/src/Processors/Formats/Impl/CHColumnToArrowColumn.cpp#L900-L913
    let index_ty = match (
        settings.use_signed_indexes_for_dictionary,
        settings.use_64_bit_indexes_for_dictionary,
    ) {
        // Based on the documented defaults and the comments in the above source link,
        // this appears to be the most likely case.
        //
        // Unfortunately, I don't think there's actually any way to externally detect the case
        // where CH has chosen dynamically to use 64-bit indices. However, having more than
        // 2^31 unique values sort of defeats the purpose of `LowCardinality` anyway, so this seems
        // like a very unlikely edge case to encounter.
        //
        // Especially because, for getting this wrong to break something, it requires the caller
        // to rely on the exact type being returned here and ignoring what's actually returned
        // from a query, which also seems unlikely.
        (true, false) => DataType::Int32,
        (true, true) => DataType::Int64,
        (false, false) => DataType::UInt32,
        (false, true) => DataType::UInt64,
    };

    Ok(DataType::Dictionary(index_ty.into(), inner_ty.into()))
}

fn deserialize_bool_setting<'de, D>(deserializer: D) -> Result<bool, D::Error>
where
    D: Deserializer<'de>,
{
    use serde::de::Error;

    let Some(s) = Option::<String>::deserialize(deserializer)? else {
        return Ok(false);
    };

    match &*s {
        "0" => Ok(false),
        "1" => Ok(true),
        _ => Err(Error::custom(format!("expected \"0\" or \"1\", got {s:?}"))),
    }
}
