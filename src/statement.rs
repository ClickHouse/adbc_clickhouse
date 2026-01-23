use crate::reader::ArrowStreamReader;
use adbc_core::error::{Error, Status};
use adbc_core::options::{OptionStatement, OptionValue};
use adbc_core::{Optionable, PartitionedResult, Statement};
use arrow_array::cast::{AsArray, as_largestring_array, as_string_array};
use arrow_array::types::{
    Date32Type, Date64Type, Decimal32Type, Decimal64Type, Decimal128Type, Decimal256Type,
    DurationMicrosecondType, DurationMillisecondType, DurationNanosecondType, DurationSecondType,
    Float32Type, Float64Type, Int8Type, Int16Type, Int32Type, Int64Type, Time32MillisecondType,
    Time32SecondType, Time64MicrosecondType, Time64NanosecondType, TimestampMicrosecondType,
    TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType, UInt8Type, UInt16Type,
    UInt32Type, UInt64Type,
};
use arrow_array::{Array, RecordBatch, RecordBatchIterator, RecordBatchReader};
use arrow_schema::{DataType, Schema, TimeUnit};
use clickhouse::Client;
use clickhouse::query::Query;

use crate::writer::ArrowStreamWriter;
use crate::{AugmentedClient, Result, TokioContext, options};

pub struct ClickhouseStatement {
    client: AugmentedClient,
    tokio: TokioContext,
    sql_query: Option<String>,
    bind: Option<BindType>,
}

enum BindType {
    Single(RecordBatch),
    Stream(Box<dyn RecordBatchReader + Send>),
}

impl ClickhouseStatement {
    pub(crate) fn new(client: AugmentedClient, tokio: TokioContext) -> Self {
        Self {
            client,
            tokio,
            sql_query: None,
            bind: None,
        }
    }
}

impl Statement for ClickhouseStatement {
    fn bind(&mut self, batch: RecordBatch) -> adbc_core::error::Result<()> {
        self.bind = Some(BindType::Single(batch));
        Ok(())
    }

    fn bind_stream(
        &mut self,
        reader: Box<dyn RecordBatchReader + Send>,
    ) -> adbc_core::error::Result<()> {
        self.bind = Some(BindType::Stream(reader));
        Ok(())
    }

    fn execute(&mut self) -> adbc_core::error::Result<impl RecordBatchReader + Send> {
        let sql = expect_sql_query(&self.sql_query)?;

        if is_streaming_insert(sql) {
            // There isn't a sensible thing to return here since bulk inserts aren't generally
            // expected to return a result set; we could return an empty reader, but that would
            // require adding special casing to `ArrowStreamReader`, or dynamic dispatch.
            // https://arrow.apache.org/adbc/current/format/specification.html#bulk-data-ingestion
            return Err(Error::with_message_and_status(
                "streaming inserts may only be used with `Statement::execute_update()`",
                Status::InvalidState,
            ));
        }

        fetch_blocking(&self.client, &self.tokio, sql, self.bind.take())
    }

    fn execute_update(&mut self) -> adbc_core::error::Result<Option<i64>> {
        let sql = expect_sql_query(&self.sql_query)?;

        if is_streaming_insert(sql) {
            let stream = match self.bind.take() {
                Some(BindType::Stream(stream)) => stream,
                Some(BindType::Single(batch)) => {
                    let schema = batch.schema();
                    // Converting to a trait object to reduce monomorphization overhead.
                    Box::new(RecordBatchIterator::new([Ok(batch)], schema))
                }
                None => {
                    return Err(Error::with_message_and_status(
                        "streaming insert requires `Statement::bind()` or `Statement::bind_stream`",
                        Status::InvalidState,
                    ));
                }
            };

            return execute_streaming_insert(&self.client, &self.tokio, sql, stream);
        }

        // FIXME: implement proper support for `X-ClickHouse-Summary` header
        execute_blocking(&self.client, &self.tokio, sql, self.bind.take())?;
        Ok(None)
    }

    fn execute_schema(&mut self) -> adbc_core::error::Result<Schema> {
        err_unimplemented!("ClickhouseStatement::execute_schema()")
    }

    fn execute_partitions(&mut self) -> adbc_core::error::Result<PartitionedResult> {
        err_unimplemented!("ClickhouseStatement::execute_partitions()")
    }

    fn get_parameter_schema(&self) -> adbc_core::error::Result<Schema> {
        err_unimplemented!("ClickhouseStatement::get_parameter_schema()")
    }

    fn prepare(&mut self) -> adbc_core::error::Result<()> {
        err_unimplemented!("ClickhouseStatement::prepare()")
    }

    fn set_sql_query(&mut self, query: impl AsRef<str>) -> adbc_core::error::Result<()> {
        self.sql_query = Some(query.as_ref().to_string());
        Ok(())
    }

    fn set_substrait_plan(&mut self, _plan: impl AsRef<[u8]>) -> adbc_core::error::Result<()> {
        err_unimplemented!("ClickhouseStatement::set_substrait_plan()")
    }

    fn cancel(&mut self) -> adbc_core::error::Result<()> {
        err_unimplemented!("ClickhouseStatement::cancel()")
    }
}

impl Optionable for ClickhouseStatement {
    type Option = OptionStatement;

    fn set_option(
        &mut self,
        key: Self::Option,
        value: OptionValue,
    ) -> adbc_core::error::Result<()> {
        match key {
            // OptionStatement::IngestMode => {}
            // OptionStatement::TargetTable => {}
            // OptionStatement::TargetCatalog => {}
            // OptionStatement::TargetDbSchema => {}
            // OptionStatement::Temporary => {}
            // OptionStatement::Incremental => {}
            // OptionStatement::Progress => {}
            // OptionStatement::MaxProgress => {}
            OptionStatement::Other(s) if s == options::PRODUCT_INFO => {
                self.client.set_product_info(&value.try_into()?);
            }
            other => {
                return Err(Error::with_message_and_status(
                    format!("unimplemented connection option: {:?}", other.as_ref()),
                    Status::NotImplemented,
                ));
            }
        }

        Ok(())
    }

    fn get_option_string(&self, key: Self::Option) -> adbc_core::error::Result<String> {
        err_unimplemented!("ClickhouseStatement::get_option_string({key:?})")
    }

    fn get_option_bytes(&self, key: Self::Option) -> adbc_core::error::Result<Vec<u8>> {
        err_unimplemented!("ClickhouseStatement::get_option_bytes({key:?})")
    }

    fn get_option_int(&self, key: Self::Option) -> adbc_core::error::Result<i64> {
        err_unimplemented!("ClickhouseStatement::get_option_int({key:?})")
    }

    fn get_option_double(&self, key: Self::Option) -> adbc_core::error::Result<f64> {
        err_unimplemented!("ClickhouseStatement::get_option_double({key:?})")
    }
}

#[inline(always)]
fn expect_sql_query(sql_query: &Option<String>) -> Result<&str, Error> {
    sql_query.as_deref().ok_or_else(|| {
        Error::with_message_and_status(
            "set_sql_query() must be called before bind() or execute()",
            Status::InvalidState,
        )
    })
}

fn bind_query(mut query: Query, bind: Option<BindType>) -> Result<Query, Error> {
    match bind {
        // It's not clear if `bind_stream` is meant to be used with anything but bulk ingestion,
        // but one suggested use-case is that each batch in the stream represents a single execution
        // of the query.
        //
        // I don't think the Arrow team want to commit to those semantics until we have a
        // multi-result-set API: https://github.com/apache/arrow-adbc/pull/3871
        Some(BindType::Stream(mut stream)) => {
            let args = stream.next().transpose()?;

            if stream.next().transpose()?.is_some() {
                return Err(Error::with_message_and_status(
                    "received more than one record batch from `Statement::bind_stream`",
                    Status::InvalidData,
                ));
            }

            if let Some(args) = args {
                query = bind_record_batch(query, args)?;
            }
        }
        Some(BindType::Single(args)) => {
            query = bind_record_batch(query, args)?;
        }
        None => (),
    }

    Ok(query)
}

fn bind_record_batch(mut query: Query, batch: RecordBatch) -> Result<Query, Error> {
    match batch.num_rows() {
        0 => return Ok(query),
        // TODO: we can bind this as arrays, but we need to disambiguate the case of 1 row
        // because it can either be bound as a scalar or a 1-element array;
        // thinking that this should be an error unless the user explicitly enables an option
        // like `"bind_params_as_arrays" = true`
        2.. => {
            return Err(Error::with_message_and_status(
                "binding a RecordBatch with more than one row is only allowed for non-streaming inserts; \
                streaming inserts must start with `INSERT INTO` (after any comments) and end with `FORMAT ArrowStream`",
                Status::InvalidArguments,
            ));
        }
        _ => (),
    }

    let schema = batch.schema_ref();

    for (field, column) in schema.fields.iter().zip(batch.columns()) {
        query = bind_scalar(query, field.name(), column)?;
    }

    Ok(query)
}

fn bind_scalar(query: Query, name: &str, array: &dyn Array) -> Result<Query, Error> {
    if array.is_null(0) {
        // The exact type doesn't matter because it just serializes to a literal `NULL`
        return Ok(query.param(name, Option::<String>::None));
    }

    match array.data_type() {
        DataType::Null => Ok(query.param(name, Option::<String>::None)),
        DataType::Boolean => Ok(query.param(name, array.as_boolean().value(0))),
        DataType::Int8 => Ok(query.param(name, array.as_primitive::<Int8Type>().value(0))),
        DataType::Int16 => Ok(query.param(name, array.as_primitive::<Int16Type>().value(0))),
        DataType::Int32 => Ok(query.param(name, array.as_primitive::<Int32Type>().value(0))),
        DataType::Int64 => Ok(query.param(name, array.as_primitive::<Int64Type>().value(0))),
        DataType::UInt8 => Ok(query.param(name, array.as_primitive::<UInt8Type>().value(0))),
        DataType::UInt16 => Ok(query.param(name, array.as_primitive::<UInt16Type>().value(0))),
        DataType::UInt32 => Ok(query.param(name, array.as_primitive::<UInt32Type>().value(0))),
        DataType::UInt64 => Ok(query.param(name, array.as_primitive::<UInt64Type>().value(0))),

        // I don't think this is the same `Float16` as ClickHouse's` BFloat16`.
        // `Float16` seems to be the standard IEEE-754 half-precision floating-point,
        // whereas `BFloat16` dedicates 3 more bits to the exponent for
        // more dynamic range at the cost of precision.
        // Thus, we can't assume them to be interchangeable.
        // DataType::Float16 => Ok(query.param(name, array.as_primitive::<Float16Type>().value(0))),
        DataType::Float32 => Ok(query.param(name, array.as_primitive::<Float32Type>().value(0))),
        DataType::Float64 => Ok(query.param(name, array.as_primitive::<Float64Type>().value(0))),
        // The timezone doesn't really matter because the Arrow format specifies that
        // it's always in UTC. Because ClickHouse doesn't support the concept of "naive" timestamps
        // without a time zone, the Arrow format schema advises to treat those as if they were UTC:
        // https://github.com/apache/arrow/blob/8e13dbc4d37247e3e9f79adfa852f482f10edec0/format/Schema.fbs#L371-L381
        DataType::Timestamp(unit, _tz) => {
            let datetime = match unit {
                TimeUnit::Second => array
                    .as_primitive::<TimestampSecondType>()
                    .value_as_datetime(0),
                TimeUnit::Millisecond => array
                    .as_primitive::<TimestampMillisecondType>()
                    .value_as_datetime(0),
                TimeUnit::Microsecond => array
                    .as_primitive::<TimestampMicrosecondType>()
                    .value_as_datetime(0),
                TimeUnit::Nanosecond => array
                    .as_primitive::<TimestampNanosecondType>()
                    .value_as_datetime(0),
            }
            .ok_or_else(|| {
                Error::with_message_and_status(
                    format!("Timestamp value out of supported range for param {name:?}"),
                    Status::InvalidArguments,
                )
            })?;

            // Needs to be explicitly serialized as UTC, otherwise it might be assumed to be
            // in the server's or column's set time zone.
            Ok(query.param(name, datetime.and_utc()))
        }
        DataType::Date32 => Ok(query.param(
            name,
            Date32Type::to_naive_date(array.as_primitive::<Date32Type>().value(0)),
        )),
        DataType::Date64 => Ok(query.param(
            name,
            Date64Type::to_naive_date_opt(array.as_primitive::<Date64Type>().value(0)).ok_or_else(
                || {
                    Error::with_message_and_status(
                        format!("Date64 value out of supported range for param {name:?}"),
                        Status::InvalidArguments,
                    )
                },
            )?,
        )),
        DataType::Time32(unit) => {
            let time = match unit {
                TimeUnit::Second => array.as_primitive::<Time32SecondType>().value_as_time(0),
                TimeUnit::Millisecond => array
                    .as_primitive::<Time32MillisecondType>()
                    .value_as_time(0),
                _ => {
                    return Err(Error::with_message_and_status(
                        format!("invalid TimeUnit for type Time32 of param {name:?}: {unit:?}"),
                        Status::InvalidArguments,
                    ));
                }
            }
            .ok_or_else(|| {
                Error::with_message_and_status(
                    format!("Time32 value out of supported range for param {name:?}"),
                    Status::InvalidArguments,
                )
            })?;

            Ok(query.param(name, time))
        }
        DataType::Time64(unit) => {
            let time = match unit {
                TimeUnit::Microsecond => array
                    .as_primitive::<Time64MicrosecondType>()
                    .value_as_time(0),
                TimeUnit::Nanosecond => array
                    .as_primitive::<Time64NanosecondType>()
                    .value_as_time(0),
                _ => {
                    return Err(Error::with_message_and_status(
                        format!("invalid TimeUnit for type Time64 of param {name:?}: {unit:?}"),
                        Status::InvalidArguments,
                    ));
                }
            }
            .ok_or_else(|| {
                Error::with_message_and_status(
                    format!("Time64 value out of supported range for param {name:?}"),
                    Status::InvalidArguments,
                )
            })?;

            Ok(query.param(name, time))
        }
        DataType::Duration(unit) => {
            let duration = match unit {
                TimeUnit::Second => array
                    .as_primitive::<DurationSecondType>()
                    .value_as_duration(0),
                TimeUnit::Millisecond => array
                    .as_primitive::<DurationMillisecondType>()
                    .value_as_duration(0),
                TimeUnit::Microsecond => array
                    .as_primitive::<DurationMicrosecondType>()
                    .value_as_duration(0),
                TimeUnit::Nanosecond => array
                    .as_primitive::<DurationNanosecondType>()
                    .value_as_duration(0),
            }
            .ok_or_else(|| {
                Error::with_message_and_status(
                    format!("Duration value out of supported range for param {name:?}"),
                    Status::InvalidArguments,
                )
            })?;

            Ok(query.param(name, duration))
        }

        // TODO: CH *has* an Interval type, but it only supports one "level" at a time:
        // https://clickhouse.com/docs/sql-reference/operators#interval
        // There's also no corresponding type in `clickhouse-rs`
        // DataType::Interval(_) => {}

        // FIXME: `ParamSerializer` in `clickhouse-rs` doesn't support `serialize_bytes()`
        // DataType::Binary => {},
        // DataType::FixedSizeBinary(_) => {}
        // DataType::LargeBinary => {}
        // DataType::BinaryView => {}

        // This is less annoying than `AsArray::as_string()`
        DataType::Utf8 => Ok(query.param(name, as_string_array(array).value(0))),
        DataType::LargeUtf8 => Ok(query.param(name, as_largestring_array(array).value(0))),
        DataType::Utf8View => Ok(query.param(name, array.as_string_view().value(0))),

        // TODO
        // DataType::List(_) => {}
        // DataType::ListView(_) => {}
        // DataType::FixedSizeList(_, _) => {}
        // DataType::LargeList(_) => {}
        // DataType::LargeListView(_) => {}
        // DataType::Struct(_) => {}
        // DataType::Union(_, _) => {}
        // DataType::Dictionary(_, _) => {}
        DataType::Decimal32(_, _) => Ok(query.param(
            name,
            array.as_primitive::<Decimal32Type>().value_as_string(0),
        )),
        DataType::Decimal64(_, _) => Ok(query.param(
            name,
            array.as_primitive::<Decimal64Type>().value_as_string(0),
        )),
        DataType::Decimal128(_, _) => Ok(query.param(
            name,
            array.as_primitive::<Decimal128Type>().value_as_string(0),
        )),
        DataType::Decimal256(_, _) => Ok(query.param(
            name,
            array.as_primitive::<Decimal256Type>().value_as_string(0),
        )),

        // TODO
        // DataType::Map(_, _) => {}
        // DataType::RunEndEncoded(_, _) => {}
        other => Err(Error::with_message_and_status(
            format!("unsupported data type {other:?} for param {name:?}"),
            Status::InvalidArguments,
        )),
    }
}

fn execute_streaming_insert(
    client: &Client,
    tokio: &TokioContext,
    sql: &str,
    mut stream: Box<dyn RecordBatchReader + Send>,
) -> Result<Option<i64>> {
    // In case any methods we invoke result in a call to Tokio
    // This whole function could just be inside a `tokio.block_on()`
    // but that results in a larger generated future type
    let _guard = tokio.enter();

    let insert = client
        .insert_formatted_with(sql)
        // Clients will expect the insert to be fully committed by the time this returns (?)
        .with_option("wait_end_of_query", "1");

    // Begins the request and writes the header
    let mut writer = ArrowStreamWriter::begin(tokio, &stream.schema(), insert)?;

    // `insert` is automatically aborted on-drop.
    while let Some(batch) = stream.next().transpose()? {
        writer.write(&batch)?;
    }

    let mut insert = writer.finish()?;

    tokio.block_on(insert.end()).map_err(|e| {
        Error::with_message_and_status(
            format!("error finishing streaming insert: {e}"),
            Status::Internal,
        )
    })?;

    // FIXME: add support for parsing the `X-ClickHouse-Summary` header
    Ok(None)
}

fn fetch_blocking(
    client: &Client,
    tokio: &TokioContext,
    sql: &str,
    bind: Option<BindType>,
) -> Result<ArrowStreamReader, Error> {
    // `Query::fetch_bytes()` spawns a task internally.
    // This whole function could just be inside a `tokio.block_on()`
    // but that results in a larger generated future type
    let _guard = tokio.enter();

    let mut query = client.query(sql);
    query = bind_query(query, bind)?;

    let cursor = query.fetch_bytes("ArrowStream").map_err(|e| {
        Error::with_message_and_status(format!("error executing query: {e}"), Status::Internal)
    })?;

    tokio
        .block_on(ArrowStreamReader::begin(tokio.clone(), cursor))
        .map_err(Into::into)
}

fn execute_blocking(
    client: &Client,
    tokio: &TokioContext,
    sql: &str,
    bind: Option<BindType>,
) -> Result<Option<i64>, Error> {
    // In case any methods we invoke result in a call to Tokio
    // This whole function could just be inside a `tokio.block_on()`
    // but that results in a larger generated future type
    let _guard = tokio.enter();

    let mut query = client.query(sql).with_option("wait_end_of_query", "1");
    query = bind_query(query, bind)?;

    tokio.block_on(query.execute()).map_err(|e| {
        Error::with_message_and_status(format!("error executing query: {e}"), Status::Internal)
    })?;

    // TODO: parse `X-ClickHouse-Summary` and return rows modified
    // https://github.com/ClickHouse/adbc_clickhouse/issues/15
    Ok(None)
}

fn is_streaming_insert(sql: &str) -> bool {
    starts_with_ignore_ascii_case(
        trim_sql_start(sql),
        "INSERT INTO"
    )
        // A comment like `-- FORMAT ArrowStream` might be treated as a false-positive,
        // but I would consider that a degenerate case
        && ends_with_ignore_ascii_case(sql.trim_end(), "FORMAT ArrowStream")
}

/// Skip blank lines and comments at the start of the SQL string.
fn trim_sql_start(mut sql: &str) -> &str {
    loop {
        // Skip leading whitespace
        sql = sql.trim_start();

        if let Some(rest) = sql.strip_prefix("/*") {
            // Consume all characters until the closing tag
            let Some((_, remainder)) = rest.split_once("*/") else {
                // The whole rest of the SQL string is a comment!
                return "";
            };

            sql = remainder;
            continue;
        }

        // Possibly more efficient than `sql.starts_with(..)` three times?
        if matches!(sql.get(..2), Some("--" | "#!" | "# ")) {
            // Consume the rest of the line
            let Some((_, remainder)) = sql.split_once('\n') else {
                // The whole rest of the SQL string is a comment!
                return "";
            };

            sql = remainder;
            continue;
        }

        break;
    }

    sql
}

fn starts_with_ignore_ascii_case(s: &str, tokens: &str) -> bool {
    s.get(..tokens.len())
        .is_some_and(|s| s.eq_ignore_ascii_case(tokens))
}

fn ends_with_ignore_ascii_case(s: &str, tokens: &str) -> bool {
    let Some(start) = s.len().checked_sub(tokens.len()) else {
        return false;
    };

    s.get(start..)
        .is_some_and(|s| s.eq_ignore_ascii_case(tokens))
}

#[cfg(test)]
mod tests {
    use super::{is_streaming_insert, trim_sql_start};

    #[test]
    fn test_trim_sql_start_identity() {
        // Strings that should not be trimmed
        let unmodified_strings = [
            "INSERT INTO foo VALUES (1, 2)",
            "SELECT\n\
                -- internal comments should be ignored
                foo,\n\
                bar,\n\
                baz\n\
             FROM quux\n\
             WHERE foo = 1 AND bar = 2",
        ];

        for s in unmodified_strings {
            assert_eq!(trim_sql_start(s), s);
        }
    }

    #[test]
    fn test_trim_sql_start_trimmed() {
        // leading spaces
        assert_eq!(
            trim_sql_start("    INSERT INTO foo VALUES (1, 2)"),
            "INSERT INTO foo VALUES (1, 2)"
        );

        // leading tabs
        assert_eq!(
            trim_sql_start("\t\t\t\tINSERT INTO foo VALUES (1, 2)"),
            "INSERT INTO foo VALUES (1, 2)"
        );

        // leading newlines
        assert_eq!(
            trim_sql_start("\n\n\n\nINSERT INTO foo VALUES (1, 2)"),
            "INSERT INTO foo VALUES (1, 2)"
        );

        // leading mixed whitespace
        assert_eq!(
            trim_sql_start("\n \t\r\nINSERT INTO foo VALUES (1, 2)"),
            "INSERT INTO foo VALUES (1, 2)"
        );

        // leading line comment
        assert_eq!(
            trim_sql_start(
                "-- insert into foo\n\
                INSERT INTO foo VALUES (1, 2)"
            ),
            "INSERT INTO foo VALUES (1, 2)"
        );

        // leading C-style comment
        assert_eq!(
            trim_sql_start(
                "/* insert into foo */\n\
                INSERT INTO foo VALUES (1, 2)"
            ),
            "INSERT INTO foo VALUES (1, 2)"
        );

        // leading C-style multiline comment
        assert_eq!(
            trim_sql_start(
                "/*\n\
                        insert into foo\n\
                    */\n\
                INSERT INTO foo VALUES (1, 2)"
            ),
            "INSERT INTO foo VALUES (1, 2)"
        );

        // mixed comments and whitespace
        assert_eq!(
            trim_sql_start(
                "\
                -- multiline comment tag in a line comment shouldn't cause the rest to be ignored
                -- insert into `foo` /*\n\
                \t\t\r\n\
                /*\n\
                insert into foo\n\
                -- line comment should be irrelevant to the end of this multiline comment(?) */\n\
                INSERT INTO foo VALUES (1, 2)"
            ),
            "INSERT INTO foo VALUES (1, 2)"
        );

        // Degenerate cases

        // Technically a syntax error
        assert_eq!(trim_sql_start("/*/INSERT INTO foo VALUES (1, 2)"), "");

        // oops, all comment (missing newline)
        assert_eq!(
            trim_sql_start(
                "-- insert into foo\
                INSERT INTO foo VALUES (1, 2)"
            ),
            ""
        );
    }

    #[test]
    fn test_is_streaming_insert_positive() {
        let should_match = [
            // Normal format
            "INSERT INTO foo FORMAT ArrowStream",
            // SQL is case-insensitive (format name might not be but close enough)
            "insert into foo format arrowstream",
            // leading whitespace
            "     INSERT INTO foo FORMAT ArrowStream",
            // trailing whitespace (might be a syntax error)
            "INSERT INTO foo FORMAT ArrowStream    ",
            // leading comment
            "-- streaming insert into `foo`\n\
             INSERT INTO foo FORMAT ArrowStream",
            // c-style comment
            "/* streaming insert into `foo */ INSERT INTO foo FORMAT ArrowStream",
        ];

        for s in should_match {
            assert!(
                is_streaming_insert(s),
                "is_streaming_insert false negative: {s:?}"
            )
        }
    }

    #[test]
    fn test_is_streaming_insert_negative() {
        let should_not_match = [
            // Normal insert
            "INSERT INTO foo VALUES (1, 2)",
            // case-insensitive
            "insert into foo values (1, 2)",
            // not an insert
            "SELECT * FROM foo",
            // oops, all comment (missing newline)
            "-- streaming insert into `foo`\
             INSERT INTO foo FORMAT ArrowStream",
            // trailing comment (probably a syntax error)
            "INSERT INTO foo FORMAT ArrowStream -- streaming insert into `foo`",
        ];

        for s in should_not_match {
            assert!(
                !is_streaming_insert(s),
                "is_streaming_insert false positive: {s:?}"
            )
        }
    }
}
