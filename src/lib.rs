use std::collections::HashSet;
use std::mem;
use adbc_core::{schemas, Connection, Database, Driver, Optionable, PartitionedResult, Statement};
use adbc_core::options::{InfoCode, ObjectDepth, OptionConnection, OptionDatabase, OptionStatement, OptionValue};
use arrow_array::{record_batch, Array, RecordBatch, RecordBatchIterator, RecordBatchReader};
use arrow_schema::{DataType, Schema, TimeUnit};
use clickhouse::Client;

use adbc_core::error::{Error, Status};
use arrow_array::cast::AsArray;
use arrow_array::types::*;
use clickhouse::query::Query;
use crate::reader::ArrowStreamReader;

mod reader;

macro_rules! err_unimplemented {
    ($path:literal) => {
        return Err(Error::with_message_and_status(format!("driver function not implemented: {}", format_args!($path)), Status::NotImplemented))
    };
    ($path:literal -> $ret:ty) => {
        return Result::<$ret, _>::Err(Error::with_message_and_status(format!("driver function not implemented: {}", format_args!($path)), Status::NotImplemented))
    }
}

pub struct ClickhouseDriver {
    tokio: TokioContext,
}

impl ClickhouseDriver {
    /// Initialize the ClickHouse driver.
    ///
    /// If this is called in the context of [an existing Tokio context][tokio::runtime::Handle::try_current],
    /// then that runtime will be used. Otherwise, a new multithreaded runtime will be started.
    pub fn init() -> adbc_core::error::Result<Self> {
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            return Ok(Self { tokio: TokioContext::Handle(handle) });
        }

        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .map_err(|e| {
                Error::with_message_and_status(
                    format!("failed to initialize Tokio runtime: {e:?}"),
                    Status::Internal
                )
            })?;

        Ok(Self {
            tokio: TokioContext::Runtime(rt),
        })
    }
}

impl Driver for ClickhouseDriver {
    type DatabaseType = ClickhouseDatabase;

    fn new_database(&mut self) -> adbc_core::error::Result<Self::DatabaseType> {
        self.new_database_with_opts([])
    }

    fn new_database_with_opts(&mut self, opts: impl IntoIterator<Item=(OptionDatabase, OptionValue)>) -> adbc_core::error::Result<Self::DatabaseType> {
        let mut client = Client::default();

        for (key, value) in opts {
            client = apply_database_option(client, key, value)?;
        }

        Ok(ClickhouseDatabase {
            client,
            tokio: self.tokio.handle().clone(),
        })
    }
}

pub struct ClickhouseDatabase {
    client: Client,
    tokio: tokio::runtime::Handle,
}

impl Database for ClickhouseDatabase {
    type ConnectionType = ClickhouseConnection;

    fn new_connection(&self) -> adbc_core::error::Result<Self::ConnectionType> {
        self.new_connection_with_opts([])
    }

    fn new_connection_with_opts(&self, opts: impl IntoIterator<Item=(OptionConnection, OptionValue)>) -> adbc_core::error::Result<Self::ConnectionType> {
        let mut connection = ClickhouseConnection {
            client: self.client.clone(),
            tokio: self.tokio.clone(),
        };

        for (key, value) in opts {
            connection.set_option(key, value)?;
        }

        Ok(connection)
    }
}

impl Optionable for ClickhouseDatabase {
    type Option = OptionDatabase;

    fn set_option(&mut self, key: Self::Option, value: OptionValue) -> adbc_core::error::Result<()> {
        // FIXME: `Client` has no way to set options through a mutable reference
        self.client = apply_database_option(self.client.clone(), key, value)?;
        Ok(())
    }

    fn get_option_string(&self, key: Self::Option) -> adbc_core::error::Result<String> {
        // FIXME: `Client` has no way to retrieve set options
        err_unimplemented!("ClickhouseDatabase::get_option_string({key:?})")
    }

    fn get_option_bytes(&self, key: Self::Option) -> adbc_core::error::Result<Vec<u8>> {
        // FIXME: `Client` has no way to retrieve set options
        err_unimplemented!("ClickhouseDatabase::get_option_bytes({key:?})")
    }

    fn get_option_int(&self, key: Self::Option) -> adbc_core::error::Result<i64> {
        // FIXME: `Client` has no way to retrieve set options
        err_unimplemented!("ClickhouseDatabase::get_option_int({key:?})")
    }

    fn get_option_double(&self, key: Self::Option) -> adbc_core::error::Result<f64> {
        // FIXME: `Client` has no way to retrieve set options
        err_unimplemented!("ClickhouseDatabase::get_option_double({key:?})")
    }
}

pub struct ClickhouseConnection {
    client: Client,
    tokio: tokio::runtime::Handle,
}

impl Connection for ClickhouseConnection {
    type StatementType = ClickhouseStatement;

    fn new_statement(&mut self) -> adbc_core::error::Result<Self::StatementType> {
        Ok(ClickhouseStatement {
            client: self.client.clone(),
            tokio: self.tokio.clone(),
            sql_query: None,
            state: StatementState::Reset,
        })
    }

    fn cancel(&mut self) -> adbc_core::error::Result<()> {
        err_unimplemented!("ClickhouseConnection::cancel()")
    }

    fn get_info(&self, _codes: Option<HashSet<InfoCode>>) -> adbc_core::error::Result<impl RecordBatchReader + Send> {
        err_unimplemented!("ClickhouseConnection::get_info()" -> ArrowStreamReader)
    }

    fn get_objects(&self, depth: ObjectDepth, catalog: Option<&str>, db_schema: Option<&str>, table_name: Option<&str>, table_type: Option<Vec<&str>>, column_name: Option<&str>) -> adbc_core::error::Result<impl RecordBatchReader + Send> {
        err_unimplemented!("ClickhouseConnection::get_objects()" -> ArrowStreamReader)
    }

    fn get_table_schema(&self, catalog: Option<&str>, db_schema: Option<&str>, table_name: &str) -> adbc_core::error::Result<Schema> {
        err_unimplemented!("ClickhouseConnection::get_table_schema()" -> Schema)
    }

    fn get_table_types(&self) -> adbc_core::error::Result<impl RecordBatchReader + Send> {
        // It's not at all clear what this is supposed to return or how it's meant to be used.
        // All implementations either return `["table", "view"]` or a `NotImplemented` error.
        let records = record_batch!(
            ("table_types", Utf8, ["table", "view"])
        )?;

        let schema = records.schema();

        Ok(RecordBatchIterator::new([Ok(records)], schema))
    }

    fn get_statistic_names(&self) -> adbc_core::error::Result<impl RecordBatchReader + Send> {
        Ok(RecordBatchIterator::new([], schemas::GET_STATISTIC_NAMES_SCHEMA.clone()))
    }

    fn get_statistics(&self, catalog: Option<&str>, db_schema: Option<&str>, table_name: Option<&str>, approximate: bool) -> adbc_core::error::Result<impl RecordBatchReader + Send> {
        err_unimplemented!("ClickhouseConnection::get_statistics()" -> ArrowStreamReader)
    }

    fn commit(&mut self) -> adbc_core::error::Result<()> {
        Err(Error::with_message_and_status("ClickHouse does not support transactions", Status::NotImplemented))
    }

    fn rollback(&mut self) -> adbc_core::error::Result<()> {
        Err(Error::with_message_and_status("ClickHouse does not support transactions", Status::NotImplemented))
    }

    fn read_partition(&self, partition: impl AsRef<[u8]>) -> adbc_core::error::Result<impl RecordBatchReader + Send> {
        err_unimplemented!("ClickhouseConnection::read_partition()" -> ArrowStreamReader)
    }
}

impl Optionable for ClickhouseConnection {
    type Option = OptionConnection;

    fn set_option(&mut self, key: Self::Option, value: OptionValue) -> adbc_core::error::Result<()> {
        err_unimplemented!("ClickhouseConnection::set_option({key:?}, {value:?})")
    }

    fn get_option_string(&self, key: Self::Option) -> adbc_core::error::Result<String> {
        err_unimplemented!("ClickhouseConnection::get_option_string({key:?})")
    }

    fn get_option_bytes(&self, key: Self::Option) -> adbc_core::error::Result<Vec<u8>> {
        err_unimplemented!("ClickhouseConnection::get_option_bytes({key:?})")
    }

    fn get_option_int(&self, key: Self::Option) -> adbc_core::error::Result<i64> {
        err_unimplemented!("ClickhouseConnection::get_option_int({key:?})")
    }

    fn get_option_double(&self, key: Self::Option) -> adbc_core::error::Result<f64> {
        err_unimplemented!("ClickhouseConnection::get_option_double({key:?})")
    }
}

pub struct ClickhouseStatement {
    client: Client,
    tokio: tokio::runtime::Handle,
    sql_query: Option<String>,
    state: StatementState,
}

enum StatementState {
    Reset,
    Query(Query),
    // TODO
    // Insert(InsertRaw),
}

impl Statement for ClickhouseStatement {
    fn bind(&mut self, batch: RecordBatch) -> adbc_core::error::Result<()> {
        let query = match mem::replace(&mut self.state, StatementState::Reset) {
            StatementState::Reset => {
                let sql = expect_sql_query(&self.sql_query)?;
                bind_record_batch(
                    self.client.query(sql),
                    batch,
                )?
            }
            StatementState::Query(query) => {
                bind_record_batch(query, batch)?
            }
        };

        self.state = StatementState::Query(query);
        Ok(())
    }

    fn bind_stream(&mut self, reader: Box<dyn RecordBatchReader + Send>) -> adbc_core::error::Result<()> {
        err_unimplemented!("ClickhouseStatement::bind_stream()")
    }

    fn execute(&mut self) -> adbc_core::error::Result<impl RecordBatchReader + Send> {
        match mem::replace(&mut self.state, StatementState::Reset) {
            StatementState::Reset => {
                let sql = expect_sql_query(&self.sql_query)?;
                fetch_blocking(&self.tokio, self.client.query(sql))
            }
            StatementState::Query(query) => {
                fetch_blocking(&self.tokio, query)
            }
        }
    }

    fn execute_update(&mut self) -> adbc_core::error::Result<Option<i64>> {
        err_unimplemented!("ClickhouseStatement::execute_update()")
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

    fn set_substrait_plan(&mut self, plan: impl AsRef<[u8]>) -> adbc_core::error::Result<()> {
        err_unimplemented!("ClickhouseStatement::set_substrait_plan()")
    }

    fn cancel(&mut self) -> adbc_core::error::Result<()> {
        err_unimplemented!("ClickhouseStatement::cancel()")
    }
}

impl Optionable for ClickhouseStatement {
    type Option = OptionStatement;

    fn set_option(&mut self, key: Self::Option, value: OptionValue) -> adbc_core::error::Result<()> {
        err_unimplemented!("ClickhouseStatement::set_option({key:?}, {value:?})")
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


enum TokioContext {
    Runtime(tokio::runtime::Runtime),
    Handle(tokio::runtime::Handle),
}

impl TokioContext {
    fn handle(&self) -> &tokio::runtime::Handle {
        match self {
            Self::Runtime(rt) => rt.handle(),
            Self::Handle(handle) => handle,
        }
    }
}

fn apply_database_option(client: Client, key: OptionDatabase, value: OptionValue) -> Result<Client, Error> {
    macro_rules! try_value {
        ($variant:ident) => {
            match value {
                OptionValue::$variant(value) => value,
                other => {
                    return Err(Error::with_message_and_status(
                        format!("expected option type {} for database option {:?}, got {other:?}", stringify!($variant), key.as_ref()),
                        Status::InvalidArguments,
                    ))
                }
            }
        };
    }

    match key {
        OptionDatabase::Uri => {
            Ok(client.with_url(try_value!(String)))
        }
        OptionDatabase::Username => {
            Ok(client.with_user(try_value!(String)))
        }
        OptionDatabase::Password => {
            Ok(client.with_password(try_value!(String)))
        }
        other => {
            Err(Error::with_message_and_status(
                format!("unknown database option {:?}", other.as_ref()),
                Status::InvalidArguments
            ))
        }
    }
}

#[inline(always)]
fn expect_sql_query(sql_query: &Option<String>) -> Result<&str, Error> {
    sql_query
        .as_deref()
        .ok_or_else(|| Error::with_message_and_status("set_sql_query() must be called before bind() or execute()", Status::InvalidState))
}

fn bind_record_batch(mut query: Query, batch: RecordBatch) -> Result<Query, Error> {
    match batch.num_rows() {
        0 => return Ok(query),
        // TODO: we can bind this as arrays, but we need to disambiguate the case of 1 row
        // because it can either be bound as a scalar or a 1-element array;
        // thinking that this should be an error unless the user explicitly enables an option
        // like `"bind_params_as_arrays" = true`
        2.. => return Err(Error::with_message_and_status(
            "binding a RecordBatch with more than one row is not currently allowed",
            Status::InvalidArguments
        )),
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
        // DataType::Float16 => Ok(query.param(name, array.as_primitive::<Float16Type>().value(0))),
        DataType::Float32 => Ok(query.param(name, array.as_primitive::<Float32Type>().value(0))),
        DataType::Float64 => Ok(query.param(name, array.as_primitive::<Float64Type>().value(0))),
        // FIXME: not sure what to do if `tz` is `None`, as CH doesn't have a concept of "naive" timestamps
        // all timestamps are input and output assuming a given timezone,
        // so this may result in subtly wrong results no matter what
        DataType::Timestamp(unit, _tz) => {
            let datetime = match unit {
                TimeUnit::Second => array.as_primitive::<TimestampSecondType>().value_as_datetime(0),
                TimeUnit::Millisecond => array.as_primitive::<TimestampMillisecondType>().value_as_datetime(0),
                TimeUnit::Microsecond => array.as_primitive::<TimestampMicrosecondType>().value_as_datetime(0),
                TimeUnit::Nanosecond => array.as_primitive::<TimestampNanosecondType>().value_as_datetime(0),
            }
                .ok_or_else(|| Error::with_message_and_status(format!("Timestamp value out of supported range for param {name:?}"), Status::InvalidArguments))?;

            Ok(query.param(name, datetime))
        }
        DataType::Date32 => Ok(query.param(name, Date32Type::to_naive_date(array.as_primitive::<Date32Type>().value(0)))),
        DataType::Date64 => Ok(
            query.param(
                name,
                Date64Type::to_naive_date_opt(array.as_primitive::<Date64Type>().value(0))
                    .ok_or_else(|| Error::with_message_and_status(format!("Date64 value out of supported range for param {name:?}"), Status::InvalidArguments))?
            )
        ),
        DataType::Time32(_) => {}
        DataType::Time64(_) => {}
        DataType::Duration(_) => {}
        DataType::Interval(_) => {}
        DataType::Binary => {}
        DataType::FixedSizeBinary(_) => {}
        DataType::LargeBinary => {}
        DataType::BinaryView => {}
        DataType::Utf8 => {}
        DataType::LargeUtf8 => {}
        DataType::Utf8View => {}
        DataType::List(_) => {}
        DataType::ListView(_) => {}
        DataType::FixedSizeList(_, _) => {}
        DataType::LargeList(_) => {}
        DataType::LargeListView(_) => {}
        DataType::Struct(_) => {}
        DataType::Union(_, _) => {}
        DataType::Dictionary(_, _) => {}
        DataType::Decimal32(_, _) => {}
        DataType::Decimal64(_, _) => {}
        DataType::Decimal128(_, _) => {}
        DataType::Decimal256(_, _) => {}
        DataType::Map(_, _) => {}
        DataType::RunEndEncoded(_, _) => {}
        other => Err(Error::with_message_and_status( format!("unsupported data type {other:?} for param {name:?}"), Status::InvalidArguments)),
    }
}

fn fetch_blocking(tokio: &tokio::runtime::Handle, query: Query) -> Result<ArrowStreamReader, Error> {
    tokio.block_on(async {
        let cursor = query
            .fetch_bytes("ArrowStream")
            .map_err(|e| Error::with_message_and_status("error executing query: {e:?}", Status::Internal))?;

        Ok(ArrowStreamReader::begin(&tokio, cursor).await?)
    })
}
