use crate::reader::ArrowStreamReader;
use adbc_core::error::{Error, Status};
use adbc_core::options::{InfoCode, ObjectDepth, OptionConnection, OptionDatabase, OptionValue};
use adbc_core::{Connection, Database, Driver, Optionable, schemas};
use arrow_array::{RecordBatchIterator, RecordBatchReader, record_batch};
use arrow_schema::Schema;
use clickhouse::Client;
use statement::ClickhouseStatement;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::runtime::{Handle, Runtime, RuntimeFlavor};
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

/// ClickHouse ADBC driver implementation.
pub struct ClickhouseDriver {
    tokio: Option<TokioContext>,
}

impl ClickhouseDriver {
    /// Initialize the ClickHouse driver.
    ///
    /// If this is called in the context of [an existing Tokio `Runtime`][tokio::runtime::Handle::try_current],
    /// and it is a [a multi-thread runtime][tokio::runtime#multi-threaded-runtime-behavior-at-the-time-of-writing],
    /// then that runtime will be used for all connections.
    ///
    /// Otherwise, a separate [current-thread runtime][tokio::runtime#current-thread-runtime-behavior-at-the-time-of-writing]
    /// will be used for each connection.
    ///
    /// Because Tokio's [`Handle::block_on()`] cannot drive a current-thread runtime forward,
    /// this method will not use an existing current-thread runtime, since it could result in a
    /// deadlock if another thread does not call [`Runtime::block_on()`].
    ///
    /// If you want to configure your own runtime, use [`Self::init_with()`] instead.
    pub fn init() -> Self {
        if let Ok(handle) = Handle::try_current()
            && handle.runtime_flavor() == RuntimeFlavor::MultiThread
        {
            return Self {
                tokio: Some(TokioContext::Handle(handle)),
            };
        }

        Self::init_current_thread()
    }

    /// Initialize the ClickHouse driver using the given Tokio [`Runtime`],
    /// which will be shared by all connections.
    ///
    /// A [current-thread runtime][tokio::runtime#current-thread-runtime-behavior-at-the-time-of-writing]
    /// **may** be passed here as [`Runtime::block_on()`] _can_ drive it forward.
    pub fn init_with(rt: Arc<Runtime>) -> Self {
        Self {
            tokio: Some(TokioContext::Runtime(rt)),
        }
    }

    /// Use a new Tokio [current-thread runtime][tokio::runtime#current-thread-runtime-behavior-at-the-time-of-writing]
    /// for each database connection. This avoids any extra threads being spawned.
    pub fn init_current_thread() -> Self {
        Self { tokio: None }
    }
}

impl Default for ClickhouseDriver {
    /// Equivalent to [`ClickhouseDriver::init()`].
    fn default() -> Self {
        Self::init()
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
        let mut db = ClickhouseDatabase {
            tokio: self.tokio.clone(),
            uri: None,
            username: None,
            password: None,
        };

        for (key, value) in opts {
            db.set_option(key, value)?;
        }

        Ok(db)
    }
}

pub struct ClickhouseDatabase {
    tokio: Option<TokioContext>,
    uri: Option<String>,
    username: Option<String>,
    password: Option<String>,
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
        let tokio = self
            .tokio
            .clone()
            .map_or_else(TokioContext::new_current_thread, Ok)?;

        // It's better to create a new `Client` for each connection because it may error or deadlock
        // if it tries to hop runtimes in the current-thread case.
        let mut client = {
            // In case `Client` calls anything internally.
            let _guard = tokio.enter();

            // TODO: configure the HTTP client to pool only a single connection (necessary?)
            Client::default().with_option("session_id", Uuid::new_v4().to_string())
        };

        if let Some(url) = &self.uri {
            client = client.with_url(url);
        }

        if let Some(username) = &self.username {
            client = client.with_user(username);
        }

        if let Some(password) = &self.password {
            client = client.with_password(password);
        }

        let mut connection = ClickhouseConnection { client, tokio };

        for (key, value) in opts {
            connection.set_option(key, value)?;
        }

        Ok(connection)
    }
}

impl Optionable for ClickhouseDatabase {
    type Option = OptionDatabase;

    // RustRover incorrectly sees `value` as unused because it's captured by the macro
    //noinspection RsLiveness
    fn set_option(
        &mut self,
        key: Self::Option,
        value: OptionValue,
    ) -> adbc_core::error::Result<()> {
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
            OptionDatabase::Uri => {
                self.uri = Some(try_value!(String));
            }
            OptionDatabase::Username => {
                self.username = Some(try_value!(String));
            }
            OptionDatabase::Password => {
                self.password = Some(try_value!(String));
            }
            other => {
                return Err(Error::with_message_and_status(
                    format!("unknown database option {:?}", other.as_ref()),
                    Status::InvalidArguments,
                ));
            }
        }

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
    tokio: TokioContext,
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

#[derive(Clone)]
enum TokioContext {
    Runtime(Arc<Runtime>),
    Handle(Handle),
}

impl TokioContext {
    fn new_current_thread() -> Result<Self> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| {
                Error::with_message_and_status(
                    format!("error creating Tokio runtime: {e}"),
                    Status::Internal,
                )
            })?;

        Ok(Self::Runtime(Arc::new(rt)))
    }

    fn enter(&self) -> tokio::runtime::EnterGuard<'_> {
        match self {
            Self::Runtime(rt) => rt.enter(),
            Self::Handle(hnd) => hnd.enter(),
        }
    }

    fn block_on<F: Future>(&self, f: F) -> F::Output {
        match self {
            Self::Runtime(rt) => rt.block_on(f),
            Self::Handle(hnd) => hnd.block_on(f),
        }
    }
}
