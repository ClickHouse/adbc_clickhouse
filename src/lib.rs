use crate::reader::ArrowStreamReader;
use adbc_core::error::{Error, Status};
use adbc_core::options::{InfoCode, ObjectDepth, OptionConnection, OptionDatabase, OptionValue};
use adbc_core::{Connection, Database, Driver, Optionable, schemas};
use arrow_array::{RecordBatchIterator, RecordBatchReader, record_batch};
use arrow_schema::Schema;
use clickhouse::Client;
use statement::ClickhouseStatement;
use std::collections::HashSet;
use uuid::Uuid;

macro_rules! err_unimplemented {
    ($path:literal) => {
        return Err(Error::with_message_and_status(
            format!("driver function not implemented: {}", format_args!($path)),
            Status::NotImplemented,
        ))
    };
    ($path:literal -> $ret:ty) => {
        return Result::<$ret, _>::Err(Error::with_message_and_status(
            format!("driver function not implemented: {}", format_args!($path)),
            Status::NotImplemented,
        ))
    };
}

mod reader;
mod schema;
mod statement;
mod writer;

// `adbc_core::error::Result` doesn't support overriding the error type
// so it's hazardous to import directly
pub(crate) type Result<T, E = Error> = std::result::Result<T, E>;

pub struct ClickhouseDriver {
    tokio: TokioContext,
}

impl ClickhouseDriver {
    /// Initialize the ClickHouse driver.
    ///
    /// If this is called in the context of [an existing Tokio runtime][tokio::runtime::Handle::try_current],
    /// then that runtime will be used. Otherwise, a new multithreaded runtime will be started.
    pub fn init() -> Result<Self, Error> {
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            return Ok(Self {
                tokio: TokioContext::Handle(handle),
            });
        }

        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .map_err(|e| {
                Error::with_message_and_status(
                    format!("failed to initialize Tokio runtime: {e:?}"),
                    Status::Internal,
                )
            })?;

        Ok(Self::init_with(rt))
    }

    /// Initialize the ClickHouse driver, passing ownership of the given [Tokio runtime][tokio::runtime::Runtime].
    pub fn init_with(rt: tokio::runtime::Runtime) -> Self {
        Self {
            tokio: TokioContext::Runtime(rt),
        }
    }
}

impl Driver for ClickhouseDriver {
    type DatabaseType = ClickhouseDatabase;

    fn new_database(&mut self) -> adbc_core::error::Result<Self::DatabaseType> {
        self.new_database_with_opts([])
    }

    fn new_database_with_opts(
        &mut self,
        opts: impl IntoIterator<Item = (OptionDatabase, OptionValue)>,
    ) -> adbc_core::error::Result<Self::DatabaseType> {
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

    fn new_connection_with_opts(
        &self,
        opts: impl IntoIterator<Item = (OptionConnection, OptionValue)>,
    ) -> adbc_core::error::Result<Self::ConnectionType> {
        let mut connection = ClickhouseConnection {
            client: self
                .client
                .clone()
                .with_option("session_id", Uuid::new_v4().to_string()),
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

    fn set_option(
        &mut self,
        key: Self::Option,
        value: OptionValue,
    ) -> adbc_core::error::Result<()> {
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
        Ok(ClickhouseStatement::new(
            self.client.clone(),
            self.tokio.clone(),
        ))
    }

    fn cancel(&mut self) -> adbc_core::error::Result<()> {
        err_unimplemented!("ClickhouseConnection::cancel()")
    }

    fn get_info(
        &self,
        _codes: Option<HashSet<InfoCode>>,
    ) -> adbc_core::error::Result<impl RecordBatchReader + Send> {
        err_unimplemented!("ClickhouseConnection::get_info()" -> ArrowStreamReader)
    }

    fn get_objects(
        &self,
        _depth: ObjectDepth,
        _catalog: Option<&str>,
        _db_schema: Option<&str>,
        _table_name: Option<&str>,
        _table_type: Option<Vec<&str>>,
        _column_name: Option<&str>,
    ) -> adbc_core::error::Result<impl RecordBatchReader + Send> {
        err_unimplemented!("ClickhouseConnection::get_objects()" -> ArrowStreamReader)
    }

    fn get_table_schema(
        &self,
        _catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: &str,
    ) -> adbc_core::error::Result<Schema> {
        self.tokio
            .block_on(schema::of_table(&self.client, db_schema, table_name))
    }

    fn get_table_types(&self) -> adbc_core::error::Result<impl RecordBatchReader + Send> {
        // It's not at all clear what this is supposed to return or how it's meant to be used.
        // All implementations either return `["table", "view"]` or a `NotImplemented` error.
        let records = record_batch!(("table_types", Utf8, ["table", "view"]))?;

        let schema = records.schema();

        Ok(RecordBatchIterator::new([Ok(records)], schema))
    }

    fn get_statistic_names(&self) -> adbc_core::error::Result<impl RecordBatchReader + Send> {
        Ok(RecordBatchIterator::new(
            [],
            schemas::GET_STATISTIC_NAMES_SCHEMA.clone(),
        ))
    }

    fn get_statistics(
        &self,
        _catalog: Option<&str>,
        _db_schema: Option<&str>,
        _table_name: Option<&str>,
        _approximate: bool,
    ) -> adbc_core::error::Result<impl RecordBatchReader + Send> {
        err_unimplemented!("ClickhouseConnection::get_statistics()" -> ArrowStreamReader)
    }

    fn commit(&mut self) -> adbc_core::error::Result<()> {
        Err(Error::with_message_and_status(
            "ClickHouse does not support transactions",
            Status::NotImplemented,
        ))
    }

    fn rollback(&mut self) -> adbc_core::error::Result<()> {
        Err(Error::with_message_and_status(
            "ClickHouse does not support transactions",
            Status::NotImplemented,
        ))
    }

    fn read_partition(
        &self,
        _partition: impl AsRef<[u8]>,
    ) -> adbc_core::error::Result<impl RecordBatchReader + Send> {
        err_unimplemented!("ClickhouseConnection::read_partition()" -> ArrowStreamReader)
    }
}

impl Optionable for ClickhouseConnection {
    type Option = OptionConnection;

    fn set_option(
        &mut self,
        key: Self::Option,
        value: OptionValue,
    ) -> adbc_core::error::Result<()> {
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

// RustRover incorrectly sees `value` as unused
//noinspection RsLiveness
fn apply_database_option(
    client: Client,
    key: OptionDatabase,
    value: OptionValue,
) -> Result<Client, Error> {
    macro_rules! try_value {
        ($variant:ident) => {
            match value {
                OptionValue::$variant(value) => value,
                other => {
                    return Err(Error::with_message_and_status(
                        format!(
                            "expected option type {} for database option {:?}, got {other:?}",
                            stringify!($variant),
                            key.as_ref()
                        ),
                        Status::InvalidArguments,
                    ))
                }
            }
        };
    }

    match key {
        OptionDatabase::Uri => Ok(client.with_url(try_value!(String))),
        OptionDatabase::Username => Ok(client.with_user(try_value!(String))),
        OptionDatabase::Password => Ok(client.with_password(try_value!(String))),
        other => Err(Error::with_message_and_status(
            format!("unknown database option {:?}", other.as_ref()),
            Status::InvalidArguments,
        )),
    }
}
