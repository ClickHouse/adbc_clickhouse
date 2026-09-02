//! Official ClickHouse driver for [Arrow Database Connectivity (ADBC)][adbc-home].
//!
//! Utilizes the [official ClickHouse Rust client][clickhouse-rs].
//!
//! [adbc-home]: https://arrow.apache.org/adbc/current/index.html
//! [clickhouse-rs]: https://github.com/ClickHouse/clickhouse-rs
//!
//! # Feature Flags
//!
//! ## ADBC Driver Manager Integration
//!
//! When the `ffi` feature is enabled, this crate exports the `AdbcDriverInit()` and `AdbcClickhouseInit()` functions.
//!
//! It then may be built as a dynamic library and loaded by an [ADBC driver manager][adbc-driver].
//!
//! [adbc-driver]: https://arrow.apache.org/adbc/current/format/how_manager.html
//!
//! ## TLS Support
//!
//! This package exposes the same Transport Layer Security (TLS) features as
//! [the `clickhouse` crate](https://github.com/ClickHouse/clickhouse-rs?tab=readme-ov-file#tls) it uses under the hood:
//!
//! * `native-tls`: use the native TLS implementation for the platform
//!   * OpenSSL on Linux
//!   * SChannel on Windows
//!   * Secure Transport on macOS
//! * `rustls-tls`: enables both `rustls-tls-aws-lc` and `rustls-tls-webpki-roots`
//! * `rustls-tls-aws-lc`: use [Rustls] with the [`aws-lc`] cryptography provider
//! * `rustls-tls-ring`: use [Rustls] with the [`ring`] cryptography provider
//! * `rustls-tls-native-roots`: configure [Rustls] to use the native TLS root certificate store for the platform
//! * `rustls-tls-webpki-roots`: configure [Rustls] to use a statically compiled set of TLS roots ([`webpki-roots`] crate)
//!
//! Note that Rustls has no TLS roots by default; when using the `rustls-tls-aws-lc` or `rustls-tls-ring` features,
//! you should also enable either `rustls-tls-native-roots` or `rustls-tls-webpki-roots` to choose a TLS root store.
//!
//! [Rustls]: https://github.com/rustls/rustls
//! [`aws-lc`]: https://github.com/aws/aws-lc-rs
//! [`ring`]: https://github.com/briansmith/ring
//! [`webpki-roots`]: https://github.com/rustls/webpki-roots
//!
//! # Data Type Support
//! This driver supports most of the Arrow datatypes that ClickHouse itself supports, as Arrow data
//! is fed directly to the server.
//!
//! The Arrow types supported by ClickHouse are covered in the
//! [`Arrow` format documentation][arrow-format]. Technically, this driver uses the [`ArrowStream`]
//! format instead, but the datatype support is the same; the only difference is that the
//! message framing is designed for streaming data vs random access from a file.
//!
//! [arrow-format]: https://clickhouse.com/docs/interfaces/formats/Arrow#data-types-matching
//! [`ArrowStream`]: https://clickhouse.com/docs/interfaces/formats/ArrowStream
//!
//! ## Bind Parameter Limitations
//! The only place this driver imposes specific limitations for datatype support is in
// `[ClickhouseStatement::bind()]` refused to resolve to the trait method
//! [binding query parameters][ClickhouseStatement#method.bind], because the values need to be converted
//! to ClickHouse types client-side and formatted as literals to be sent alongside the query.
//!
//! Currently, for binding parameters, only scalar types are supported, with a few exceptions
//! (see `bind_scalar()` in `src/statement.rs` for the actual mappings):
//!
//! The `Float16` type in Arrow does not have an exact equivalent in ClickHouse.
//! CH's `BFloat16` is a different type that trades mantissa precision for more exponent bits,
//! so the two types are not strictly compatible.
//!
//! [ClickHouse's `Interval` type][ch-interval] is not exactly the same as
//! [Arrow's `Interval` type][arrow-interval]. The former only allows one "level" of precision
//! at a time, but the latter has three "modes" all using mixed units; this means mapping the latter
//! to the former would almost always either lose precision or be forced to reject
//! intervals with mixed units.
//!
//! Instead, ClickHouse server actually maps CH's `Interval` type to
//! [Arrow's `Duration` type][arrow-duration], but only for intervals with a precision of
//! one second or smaller, and _rejects_ Arrow's `Interval` type on insert.
//!
//! This driver recognizes the [`arrow.uuid`] extension type, and will bind it as a literal UUID
//! instead of a fixed-size binary string.
//!
//! [ch-interval]: https://clickhouse.com/docs/sql-reference/data-types/special-data-types/interval
//! [arrow-interval]: https://github.com/apache/arrow/blob/f4860536b100c480cd5015941c9ec4442a61eae0/format/Schema.fbs#L399-L419
//! [arrow-duration]: https://github.com/apache/arrow/blob/f4860536b100c480cd5015941c9ec4442a61eae0/format/Schema.fbs#L421-L436
//! [`arrow.uuid`]: https://arrow.apache.org/docs/format/CanonicalExtensions.html#uuid
//!
//! ## Strings are assumed to be UTF-8 by default.
//! ClickHouse's type system does not differentiate between binary strings and UTF-8 strings.
//! While the `String` type is _conventionally_ UTF-8, this is not enforced.
//!
//! For convenience, ClickHouse defaults to reporting all `String` values as Arrow UTF-8 strings
//! without any validation, which could result in deserialization errors at runtime if a string
//! is encountered that is not valid UTF-8.
//!
//! `FixedString(N)` is always reported as binary because Arrow does not have
//! a fixed-size UTF-8 string type.
//!
//! If your schema may contain `String` values that are **not** valid UTF-8,
//! set option [`options::OUTPUT_STRING_AS_STRING`] to `"false"` to make ClickHouse report
//! all strings as binary strings instead. This may be set at the connection or statement level.
use crate::options::{OptionValueExt, ProductInfo};
use adbc_core::error::{Error, Status};
use adbc_core::constants::ADBC_VERSION_1_1_0;
use adbc_core::options::{InfoCode, ObjectDepth, OptionConnection, OptionDatabase, OptionValue};
use adbc_core::{Connection, Database, Driver, Optionable, schemas};
use arrow_array::{RecordBatchIterator, RecordBatchReader, record_batch};
use arrow_schema::Schema;
use clickhouse::Client;
use rand::distr::{Alphanumeric, SampleString};
use std::future::Future;
use std::collections::{HashMap, HashSet};
use std::ops::Deref;
use std::sync::Arc;
use tokio::runtime::{Handle, Runtime, RuntimeFlavor};
use url::Url;

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

mod info;
pub mod options;
mod reader;
mod schema;
mod statement;
mod writer;

#[cfg(doctest)]
#[doc = include_str!("../README.md")]
pub mod readme_test {}

// `adbc_core::error::Result` doesn't support overriding the error type
// so it's hazardous to import directly
pub(crate) type Result<T, E = Error> = std::result::Result<T, E>;

#[cfg(feature = "ffi")]
adbc_ffi::export_driver!(AdbcClickhouseInit, ClickhouseDriver);

pub use statement::ClickhouseStatement;

/// ClickHouse ADBC [`Driver`] implementation.
///
/// # Note: Tokio Runtime
/// This driver is built on the [official ClickHouse Rust client][clickhouse-rs],
/// which is an async-native library built on the [Tokio async runtime][tokio].
///
/// To adapt this to a synchronous API, this library uses [`Runtime::block_on()`], or
/// [`Handle::block_on()`] depending on how it was constructed (see [`Self::init()`] for details).
///
/// When loaded dynamically by an [ADBC driver manager][driver-mgr],
/// this driver defaults to using a new [current-thread runtime] for each connection,
/// which does not spawn any additional threads.
///
/// [clickhouse-rs]: https://github.com/ClickHouse/clickhouse-rs
/// [tokio]: https://tokio.rs/tokio/tutorial
/// [driver-mgr]: https://arrow.apache.org/adbc/current/format/how_manager.html
/// [current-thread runtime]: tokio::runtime#current-thread-runtime-behavior-at-the-time-of-writing
pub struct ClickhouseDriver {
    tokio: Option<TokioContext>,
    // Empty by default.
    product_info: ProductInfo,
}

impl ClickhouseDriver {
    /// Initialize the ClickHouse driver.
    ///
    /// If this is called in the context of [an existing Tokio `Runtime`][Handle::try_current],
    /// and it is a [multi-thread runtime] then that runtime will be used for all connections.
    ///
    /// Otherwise, a separate [current-thread runtime] will be used for each connection.
    ///
    /// Because Tokio's [`Handle::block_on()`] cannot drive a current-thread runtime forward,
    /// this method will not use an existing current-thread runtime, since it could result in a
    /// deadlock if another thread does not call [`Runtime::block_on()`].
    ///
    /// If you want to configure your own runtime, use [`Self::init_with()`] instead.
    ///
    /// [multi-thread runtime]: tokio::runtime#multi-threaded-runtime-behavior-at-the-time-of-writing
    /// [current-thread runtime]: tokio::runtime#current-thread-runtime-behavior-at-the-time-of-writing
    pub fn init() -> Self {
        if let Ok(handle) = Handle::try_current()
            && handle.runtime_flavor() == RuntimeFlavor::MultiThread
        {
            return Self {
                tokio: Some(TokioContext::Handle(handle)),
                product_info: ProductInfo::default(),
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
            product_info: ProductInfo::default(),
        }
    }

    /// Use a new Tokio [current-thread runtime][tokio::runtime#current-thread-runtime-behavior-at-the-time-of-writing]
    /// for each database connection. This avoids any extra threads being spawned.
    pub fn init_current_thread() -> Self {
        Self {
            tokio: None,
            product_info: ProductInfo::default(),
        }
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
            url: None,
            username: None,
            password: None,
            product_info: self.product_info.clone(),
            settings: HashMap::new(),
        };

        for (key, value) in opts {
            db.set_option(key, value)?;
        }

        Ok(db)
    }
}

/// ClickHouse ADBC [`Database`] implementation.
///
/// # URLs/URIs
/// This driver uses the [Clickhouse HTTP interface], and supports URLs with the `http://`,
/// `https://` and `clickhouse://` schemes (for automatic driver loading by ADBC driver managers).
///
/// The `clickhouse://` scheme is rewritten to `https://` by default for security reasons.
/// To override this, add `?protocol=http` to the end of the URL.
///
/// Any other query parameter in the URL, except `database` (see [Default Database](#default-database)
/// below), is treated as a [ClickHouse setting]
/// (e.g. `?mutations_sync=3`), equivalent to setting the
/// [`options::SETTING_PREFIX`]-prefixed option of the same name at that moment
/// (last write wins per setting).
///
/// [ClickHouse setting]: https://clickhouse.com/docs/operations/settings/settings
///
/// The default URL if none is set is `http://localhost:8123`.
///
/// Set the URL of the database to connect to using [`OptionDatabase::Uri`].
///
/// ## Default Database
/// Add `?database=<name>` to the URL to set the default database for all connections
/// created from this `Database` (any scheme). Unqualified table names in queries will resolve
/// to this database instead of the default database configured for the user (usually `default`).
///
/// The database name is passed with every request made through the [ClickHouse HTTP interface],
/// so it is unaffected by session expiry or load balancing, unlike a `USE <name>` statement.
///
/// The default database may be overridden per connection with [`OptionConnection::CurrentSchema`],
/// and read back with [`Optionable::get_option_string()`] on the connection.
/// (In ADBC terms, a ClickHouse database maps to a *schema*; ClickHouse has no catalog level.)
///
/// Setting [`OptionConnection::CurrentSchema`] to the empty string *clears* the default database
/// for that connection, falling back to the user's default, e.g. to keep the connection
/// usable after dropping the database it was configured with.
///
/// ```
/// use adbc_clickhouse::ClickhouseDriver;
/// use adbc_core::{Driver, Database, Optionable};
/// use adbc_core::options::{OptionConnection, OptionDatabase};
///
/// let mut driver = ClickhouseDriver::init();
///
/// let mut database = driver.new_database_with_opts([
///     (OptionDatabase::Uri, "http://localhost:8123?database=my_db".into()),
/// ]).unwrap();
///
/// // All connections default to `my_db`...
/// let connection = database.new_connection().unwrap();
///
/// // ...but it may be overridden per connection.
/// let other_connection = database.new_connection_with_opts([
///     (OptionConnection::CurrentSchema, "other_db".into()),
/// ]).unwrap();
///
/// assert_eq!(
///     connection.get_option_string(OptionConnection::CurrentSchema).unwrap(),
///     "my_db"
/// );
/// assert_eq!(
///     other_connection.get_option_string(OptionConnection::CurrentSchema).unwrap(),
///     "other_db"
/// );
/// ```
///
/// ## Example
/// ```
/// use adbc_clickhouse::ClickhouseDriver;
/// use adbc_core::{Driver, Database, Optionable};
/// use adbc_core::options::OptionDatabase;
///
/// let mut driver = ClickhouseDriver::init();
///
/// let mut database = driver.new_database().unwrap();
///
/// // `https://` and `http://` schemes are not rewritten (except for normalization)
/// database.set_option(OptionDatabase::Uri, "https://localhost:8123".into()).unwrap();
///
/// database.set_option(OptionDatabase::Uri, "http://localhost:8123".into()).unwrap();
///
/// // `clickhouse://` is rewritten to `https://`
/// database.set_option(OptionDatabase::Uri, "clickhouse://localhost:8123".into()).unwrap();
///
/// // Override to `http://` when TLS is not desired
/// database.set_option(
///     OptionDatabase::Uri,
///     "clickhouse://localhost:8123?protocol=http".into()
/// ).unwrap();
///
/// // Note: connects lazily on first query
/// let mut connection = database.new_connection().unwrap();
///
/// # drop(connection);
/// ```
///
/// [ClickHouse HTTP interface]: https://clickhouse.com/docs/interfaces/http
pub struct ClickhouseDatabase {
    tokio: Option<TokioContext>,
    url: Option<Url>,
    username: Option<String>,
    password: Option<String>,
    product_info: ProductInfo,
    settings: HashMap<String, String>,
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

        // In case `Client::default()` calls anything internally.
        let tokio_guard = tokio.enter();

        // It's better to create a new `Client` for each connection because it may error or deadlock
        // if it tries to hop runtimes in the current-thread case.
        // TODO: configure the HTTP client to pool only a single connection (necessary?)
        let mut client = Client::default()
            // Default `product_info` that should always be included
            // Note: we don't apply `self.product_info` at this level in case it's set
            // to a different value at a lower level; `AugmentedClient` covers that.
            .with_product_info("adbc_clickhouse", env!("CARGO_PKG_VERSION"));

        if let Some(url) = &self.url {
            client = client.with_url(url.to_string());
        }

        if let Some(username) = &self.username {
            client = client.with_user(username);
        }

        if let Some(password) = &self.password {
            client = client.with_password(password);
        }

        let mut client = AugmentedClient::new(client);
        client.set_product_info(&self.product_info);

        for (name, value) in &self.settings {
            client.set_setting(name, value.clone());
        }

        drop(tokio_guard);

        let mut connection = ClickhouseConnection { client, tokio };

        for (key, value) in opts {
            connection.set_option(key, value)?;
        }

        if connection
            .client
            .get_option(options::as_setting::SESSION_ID)
            .is_none()
        {
            connection
                .client
                .set_setting(options::as_setting::SESSION_ID, random_id("session"));
        }

        Ok(connection)
    }
}

/// # Note: Read-back of sensitive options omitted.
///
/// Calling `get_option_string()` with `OptionDatabase::Uri`, `Username` or `Password`
/// will return an error with [`Status::NotFound`] to avoid leaking sensitive user credentials.
impl Optionable for ClickhouseDatabase {
    type Option = OptionDatabase;

    fn set_option(
        &mut self,
        key: Self::Option,
        value: OptionValue,
    ) -> adbc_core::error::Result<()> {
        match key {
            OptionDatabase::Uri => {
                self.set_url(value.try_string(key)?)?;
            }
            OptionDatabase::Username => {
                self.username = Some(value.try_string(key)?);
            }
            OptionDatabase::Password => {
                self.password = Some(value.try_string(key)?);
            }
            OptionDatabase::Other(s) => {
                self.set_custom_option(&s, value)?;
            }
            other => {
                return Err(Error::with_message_and_status(
                    format!("unimplemented database option {:?}", other.as_ref()),
                    Status::NotImplemented,
                ));
            }
        }

        Ok(())
    }

    fn get_option_string(&self, key: Self::Option) -> adbc_core::error::Result<String> {
        match key {
            OptionDatabase::Uri | OptionDatabase::Username | OptionDatabase::Password => {
                Err(Error::with_message_and_status(
                    "readback of potentially sensitive options has been omitted",
                    Status::NotFound,
                ))
            }
            OptionDatabase::Other(s) => self.get_custom_option(&s),
            other => Err(Error::with_message_and_status(
                format!("unimplemented database option {:?}", other.as_ref()),
                Status::NotImplemented,
            )),
        }
    }

    fn get_option_bytes(&self, key: Self::Option) -> adbc_core::error::Result<Vec<u8>> {
        // There's no options that are specifically binary-only, so... *shrug*
        self.get_option_string(key).map(String::into_bytes)
    }

    fn get_option_int(&self, key: Self::Option) -> adbc_core::error::Result<i64> {
        // Currently no database options that can be integers
        Err(Error::with_message_and_status(
            format!("option {:?} is not an integer", key.as_ref()),
            Status::InvalidArguments,
        ))
    }

    fn get_option_double(&self, key: Self::Option) -> adbc_core::error::Result<f64> {
        // Currently no database options that can be doubles
        Err(Error::with_message_and_status(
            format!("option {:?} is not a double", key.as_ref()),
            Status::InvalidArguments,
        ))
    }
}

impl ClickhouseDatabase {
    fn set_url(&mut self, url: String) -> Result<()> {
        const ALLOWED_SCHEMES: &[&str] = &["http", "https", "clickhouse"];

        let url = url.parse::<Url>().map_err(|e| {
            Error::with_message_and_status(format!("invalid URL: {e}"), Status::InvalidArguments)
        })?;

        let url = match url.scheme() {
            "http" | "https" => url, // No-op
            "clickhouse" => rewrite_url(url)?,
            "" => {
                return Err(Error::with_message_and_status(
                    format!("URL scheme cannot be empty, must be one of {ALLOWED_SCHEMES:?}"),
                    Status::InvalidArguments,
                ));
            }
            scheme => {
                return Err(Error::with_message_and_status(
                    format!(
                        "unrecognized URL scheme: {scheme:?}, expected one of {ALLOWED_SCHEMES:?}"
                    ),
                    Status::InvalidArguments,
                ));
            }
        };

        // Query parameters become settings; `clickhouse-rs` clears base-URL
        // query pairs before each request, so anything left here is dead.
        // `database` is the exception: it stays in the URL so `Client::with_url()`
        // recognizes it as the default database.
        let mut database = None;

        for (name, value) in url.query_pairs() {
            // `rewrite_url()` already consumed `protocol` for `clickhouse://` URLs.
            if name == "protocol" {
                return Err(Error::with_message_and_status(
                    "the \"protocol\" URL parameter is only supported with clickhouse:// URLs",
                    Status::InvalidArguments,
                ));
            }

            if name == "database" {
                // `Client::with_url()` silently ignores an empty `database` parameter;
                // give immediate feedback instead.
                if value.is_empty() {
                    return Err(Error::with_message_and_status(
                        "URL parameter \"database\" cannot be empty",
                        Status::InvalidArguments,
                    ));
                }

                database = Some(value.into_owned());
                continue;
            }

            self.settings.insert(name.into_owned(), value.into_owned());
        }

        let mut url = url;
        url.set_query(None);

        if let Some(database) = &database {
            url.query_pairs_mut().append_pair("database", database);
        }

        self.url = Some(url);

        Ok(())
    }

    fn set_custom_option(&mut self, key: &str, value: OptionValue) -> Result<()> {
        if let Some(name) = options::setting_name(key) {
            self.settings
                .insert(name.to_owned(), value.try_string(key)?);
            return Ok(());
        }

        match key {
            options::PRODUCT_INFO => {
                self.product_info = value.try_into()?;
            }
            other => {
                return Err(Error::with_message_and_status(
                    format!("unknown database option {other:?}"),
                    Status::InvalidArguments,
                ));
            }
        }

        Ok(())
    }

    fn get_custom_option(&self, key: &str) -> Result<String> {
        if let Some(name) = options::setting_name(key) {
            return self
                .settings
                .get(name)
                .cloned()
                .ok_or_else(|| options::setting_not_set(name));
        }

        match key {
            options::PRODUCT_INFO => Ok(self.product_info.to_string()),
            other => Err(Error::with_message_and_status(
                format!("unknown database option {other:?}"),
                Status::InvalidArguments,
            )),
        }
    }
}

/// ClickHouse ADBC [`Connection`] implementation.
///
/// # Sessions in the ClickHouse HTTP Interface
/// This type does not necessarily represent a persistent TCP connection.
///
/// Instead, it makes calls to the [ClickHouse HTTP interface][ch-http]. HTTP/1.1 connections are
/// transparently cached in the object.
///
/// A random [`session_id`] is generated upon construction and subsequently passed to all
/// HTTP interface calls made by this connection and any [`ClickhouseStatement`]s created from it.
/// Any session-local state (such as [settings] and [temporary tables]) is stored in association
/// with this session ID.
///
/// The session ID may be overridden or read back using [`options::SESSION_ID`].
///
/// Sessions persist for the [default session timeout], which is 60 seconds of inactivity.
/// Server-side state is not guaranteed past this timeout.
///
/// # Tokio Runtime
/// This connection inherits the Tokio runtime of the parent [`ClickhouseDriver`].
///
/// If a runtime was not set when the driver was created, a new [current-thread runtime] is
/// created for each connection.
///
/// See [`ClickhouseDriver::init()`] for details.
///
/// [ch-http]: https://clickhouse.com/docs/interfaces/http
/// [`session_id`]: https://clickhouse.com/docs/interfaces/http#using-clickhouse-sessions-in-the-http-protocol
/// [settings]: https://clickhouse.com/docs/sql-reference/statements/set
/// [temporary tables]: https://clickhouse.com/docs/sql-reference/statements/create/table#temporary-tables
/// [default session timeout]: https://clickhouse.com/docs/operations/server-configuration-parameters/settings#default_session_timeout
/// [current-thread runtime]: tokio::runtime#current-thread-runtime-behavior-at-the-time-of-writing
pub struct ClickhouseConnection {
    client: AugmentedClient,
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

    /// Return driver and vendor metadata as described by [`InfoCode`].
    ///
    /// When `codes` is `None`, all supported info codes are returned. Unsupported codes
    /// (currently `VendorArrowVersion` and Substrait version bounds) are omitted.
    ///
    /// `VendorVersion` is fetched from the server via `SELECT version()`.
    fn get_info(
        &self,
        codes: Option<HashSet<InfoCode>>,
    ) -> adbc_core::error::Result<Box<dyn RecordBatchReader + Send + 'static>> {
        self.tokio.block_on(self.build_info(codes.as_ref()))
    }

    fn get_objects(
        &self,
        _depth: ObjectDepth,
        _catalog: Option<&str>,
        _db_schema: Option<&str>,
        _table_name: Option<&str>,
        _table_type: Option<Vec<&str>>,
        _column_name: Option<&str>,
    ) -> adbc_core::error::Result<Box<dyn RecordBatchReader + Send + 'static>> {
        err_unimplemented!("ClickhouseConnection::get_objects()" -> Box<dyn RecordBatchReader + Send + 'static>)
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

    fn get_table_types(
        &self,
    ) -> adbc_core::error::Result<Box<dyn RecordBatchReader + Send + 'static>> {
        // It's not at all clear what this is supposed to return or how it's meant to be used.
        // All implementations either return `["table", "view"]` or a `NotImplemented` error.
        let records = record_batch!(("table_types", Utf8, ["table", "view"]))?;

        let schema = records.schema();

        Ok(Box::new(RecordBatchIterator::new([Ok(records)], schema)))
    }

    fn get_statistic_names(
        &self,
    ) -> adbc_core::error::Result<Box<dyn RecordBatchReader + Send + 'static>> {
        Ok(Box::new(RecordBatchIterator::new(
            [],
            schemas::GET_STATISTIC_NAMES_SCHEMA.clone(),
        )))
    }

    fn get_statistics(
        &self,
        _catalog: Option<&str>,
        _db_schema: Option<&str>,
        _table_name: Option<&str>,
        _approximate: bool,
    ) -> adbc_core::error::Result<Box<dyn RecordBatchReader + Send + 'static>> {
        err_unimplemented!("ClickhouseConnection::get_statistics()" -> Box<dyn RecordBatchReader + Send + 'static>)
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
    ) -> adbc_core::error::Result<Box<dyn RecordBatchReader + Send + 'static>> {
        err_unimplemented!("ClickhouseConnection::read_partition()" -> Box<dyn RecordBatchReader + Send + 'static>)
    }
}

impl Optionable for ClickhouseConnection {
    type Option = OptionConnection;

    fn set_option(
        &mut self,
        key: Self::Option,
        value: OptionValue,
    ) -> adbc_core::error::Result<()> {
        match key {
            // An ADBC *schema* maps to a ClickHouse database;
            // the empty string clears the default database
            OptionConnection::CurrentSchema => {
                self.client.set_database(&value.try_string(key.as_ref())?);
            }
            OptionConnection::CurrentCatalog => {
                return Err(Error::with_message_and_status(
                    format!(
                        "error setting option {:?} = {value:?}: ClickHouse does not have catalogs",
                        key.as_ref()
                    ),
                    Status::InvalidArguments,
                ));
            }
            // OptionConnection::AutoCommit => {}
            // OptionConnection::ReadOnly => {}
            // OptionConnection::IsolationLevel => {}
            OptionConnection::Other(s) => {
                self.set_custom_option(&s, value)?;
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
        match key {
            OptionConnection::CurrentSchema => self
                .client
                .database()
                // A cleared database reads back as never set
                .filter(|database| !database.is_empty())
                .map(String::from)
                .ok_or_else(|| {
                    Error::with_message_and_status(
                        "no default database is configured; \
                         the server chooses the default database for the user (usually \"default\")",
                        Status::NotFound,
                    )
                }),
            OptionConnection::Other(s) => self.get_custom_option(&s),
            other => err_unimplemented!("ClickhouseConnection::get_option_string({other:?})"),
        }
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

impl ClickhouseConnection {
    fn set_custom_option(&mut self, key: &str, value: OptionValue) -> Result<()> {
        if let Some(name) = options::setting_name(key) {
            self.client.set_setting(name, value.try_string(key)?);
            return Ok(());
        }

        match key {
            options::PRODUCT_INFO => {
                self.client.set_product_info(&value.try_into()?);
            }
            options::SESSION_ID => {
                self.client
                    .set_setting(options::as_setting::SESSION_ID, value.try_string(key)?);
            }
            options::OUTPUT_STRING_AS_STRING => {
                self.client.set_setting(
                    options::as_setting::OUTPUT_STRING_AS_STRING,
                    value.try_string(key)?,
                );
            }
            other => {
                return Err(Error::with_message_and_status(
                    format!("unknown connection option {other:?}"),
                    Status::InvalidArguments,
                ));
            }
        }

        Ok(())
    }

    async fn build_info(
        &self,
        codes: Option<&HashSet<InfoCode>>,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        let mut builder = info::InfoBuilder::new();

        if info::wants(codes, InfoCode::VendorName) {
            builder.add_string(InfoCode::VendorName, "ClickHouse");
        }

        if info::wants(codes, InfoCode::VendorVersion) {
            let version = fetch_vendor_version(&self.client).await?;
            builder.add_string(InfoCode::VendorVersion, version);
        }

        // ClickHouse does not expose a vendor Arrow library version over HTTP.
        // Omit `VendorArrowVersion` rather than guessing.

        if info::wants(codes, InfoCode::VendorSql) {
            builder.add_bool(InfoCode::VendorSql, true);
        }

        if info::wants(codes, InfoCode::VendorSubstrait) {
            builder.add_bool(InfoCode::VendorSubstrait, false);
        }

        // Substrait is unsupported. Arrow dense unions cannot carry top-level nulls, so we omit
        // `VendorSubstrait{Min,Max}Version` rather than returning null `info_value` rows.

        if info::wants(codes, InfoCode::DriverName) {
            builder.add_string(InfoCode::DriverName, "ADBC ClickHouse Driver");
        }

        if info::wants(codes, InfoCode::DriverVersion) {
            builder.add_string(InfoCode::DriverVersion, env!("CARGO_PKG_VERSION"));
        }

        if info::wants(codes, InfoCode::DriverArrowVersion) {
            // Resolved `arrow-array` version via `cargo metadata` (see build.rs).
            builder.add_string(InfoCode::DriverArrowVersion, env!("ADBC_DRIVER_ARROW_VERSION"));
        }

        if info::wants(codes, InfoCode::DriverAdbcVersion) {
            builder.add_i64(InfoCode::DriverAdbcVersion, ADBC_VERSION_1_1_0 as i64);
        }

        builder.finish()
    }
}

#[derive(clickhouse::Row, serde::Deserialize)]
struct VendorVersionRow {
    version: String,
}

async fn fetch_vendor_version(client: &Client) -> Result<String> {
    client
        .query("SELECT version() AS version")
        .fetch_one::<VendorVersionRow>()
        .await
        .map(|row| row.version)
        .map_err(|e| {
            Error::with_message_and_status(
                format!("error fetching ClickHouse version: {e}"),
                Status::IO,
            )
        })
    fn get_custom_option(&self, key: &str) -> Result<String> {
        if let Some(name) = options::setting_name(key) {
            return self
                .client
                .get_option(name)
                .map(Into::into)
                .ok_or_else(|| options::setting_not_set(name));
        }

        match key {
            options::SESSION_ID => Ok(self
                .client
                .get_option(options::as_setting::SESSION_ID)
                .unwrap_or("")
                .into()),
            other => Err(Error::with_message_and_status(
                format!("unknown connection option {other:?}"),
                Status::InvalidArguments,
            )),
        }
    }
}

/// Wrapper for [`Client`] that implements expected semantics for certain settings.
///
/// For example, overwriting `product_info` instead of appending to it.
#[derive(Clone)]
struct AugmentedClient {
    /// `Client` without `product_info``
    original_client: Arc<Client>,
    // `Client` implements `Clone`, but it's a deep copy
    modified_client: Option<Arc<Client>>,
}

impl AugmentedClient {
    fn new(client: Client) -> Self {
        Self {
            original_client: Arc::new(client),
            modified_client: None,
        }
    }

    fn set_product_info(&mut self, product_info: &ProductInfo) {
        // If we just kept calling `.with_product_info()` on the same client,
        // it would keep adding to the product info instead of resetting it.
        self.modified_client = (!product_info.is_empty()).then(|| {
            product_info
                .apply(Client::clone(&self.original_client))
                .into()
        });
    }

    fn set_setting(&mut self, key: &str, value: String) {
        if let Some(modified_client) = &mut self.modified_client {
            Arc::make_mut(modified_client).set_setting(key, &value);
        }

        Arc::make_mut(&mut self.original_client).set_setting(key, value);
    }

    /// An empty `database` clears the default: `Client` cannot unset it,
    /// but the server ignores an empty `database` parameter.
    fn set_database(&mut self, database: &str) {
        // `Client::with_database()` takes `self` by value
        fn apply(client: &mut Arc<Client>, database: &str) {
            let client = Arc::make_mut(client);
            *client = Client::clone(client).with_database(database);
        }

        if let Some(modified_client) = &mut self.modified_client {
            apply(modified_client, database);
        }

        apply(&mut self.original_client, database);
    }

    fn get_option(&self, key: &str) -> Option<&str> {
        self.modified_client
            .as_deref()
            .unwrap_or(&self.original_client)
            .get_setting(key)
    }
}

impl Deref for AugmentedClient {
    type Target = Client;

    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        self.modified_client
            .as_deref()
            .unwrap_or(&self.original_client)
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
                    format!("error creating Tokio current-thread runtime: {e}"),
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

pub(crate) fn random_id(namespace: &str) -> String {
    let mut out = format!("{namespace}_");

    // Controversial opinion: magic constants are fine if they only appear in one obvious context.
    // Extracting the length to a constant would only add indirection and obscure the intent.
    Alphanumeric.append_string(&mut rand::rng(), &mut out, 16);

    out
}

fn rewrite_url(url: Url) -> Result<Url> {
    const ALLOWED_OVERRIDES: &[&str] = &["http", "https"];

    let mut protocol_override = None;

    let query_pairs = url
        .query_pairs()
        .filter_map(|(key, value)| {
            if key == "protocol" {
                protocol_override = Some(value);
                return None;
            }

            Some((key, value))
        })
        .collect::<Vec<_>>();

    if let Some(protocol_override) = &protocol_override
        && !ALLOWED_OVERRIDES.contains(&&**protocol_override)
    {
        return Err(Error::with_message_and_status(
            format!(
                "disallowed protocol override: {protocol_override:?}, allowed overrides: {ALLOWED_OVERRIDES:?}"
            ),
            Status::InvalidArguments,
        ));
    }

    // `.set_scheme()` doesn't allow changing `clickhouse://` to `https://`
    // so we need to implement our own override method.
    // There's a PR for this that's been open since August 2025 with no review comments:
    // https://github.com/servo/rust-url/pull/1073
    let mut new_url = Url::parse(&format!(
        "{scheme}{after_scheme}",
        scheme = protocol_override.as_deref().unwrap_or("https"),
        // Everything between the scheme and the query (host, port, username, password, path)
        after_scheme = &url[url::Position::AfterScheme..url::Position::AfterPath],
    ))
    .map_err(|e| {
        Error::with_message_and_status(
            format!("unable to rewrite URL: {e}"),
            Status::InvalidArguments,
        )
    })?;

    // `Url::parse_with_params()` appends `?` even if the iterator is empty
    if !query_pairs.is_empty() {
        new_url.query_pairs_mut().extend_pairs(query_pairs).finish();
    }

    // ClickHouse HTTP URLs don't really use fragments but this shouldn't be harmful.
    new_url.set_fragment(url.fragment());

    Ok(new_url)
}

#[cfg(test)]
mod tests {
    use crate::{ClickhouseDriver, options};
    use adbc_core::error::Status;
    use adbc_core::options::{OptionConnection, OptionDatabase, OptionValue};
    use adbc_core::{Connection, Database, Driver, Optionable};
    use url::Url;

    #[test]
    fn test_set_product_info() {
        let mut driver = ClickhouseDriver::init();

        let db = driver
            .new_database_with_opts([(
                options::PRODUCT_INFO.into(),
                "foo/1.0.0 bar/0.12.34-alpha.1".into(),
            )])
            .unwrap();

        assert!(
            db.product_info
                .pairs()
                .eq([("foo", "1.0.0"), ("bar", "0.12.34-alpha.1")])
        );

        // `clickhouse::Client` doesn't provide any way to read back the product info
        // so the rest of this test is just ensuring that it _can_ be set
        let mut conn = db
            .new_connection_with_opts([(
                options::PRODUCT_INFO.into(),
                "foo/2.0.0 bar/1.23.45-beta.1".into(),
            )])
            .unwrap();

        // FIXME: https://github.com/apache/arrow-adbc/issues/3913
        let mut statement = conn.new_statement().unwrap();
        statement
            .set_option(
                options::PRODUCT_INFO.into(),
                "foo/3.0.0 bar/2.34.56-rc.1".into(),
            )
            .unwrap();
    }

    #[test]
    fn test_set_setting() {
        let mut driver = ClickhouseDriver::init();

        let mut db = driver
            .new_database_with_opts([("clickhouse.setting.mutations_sync".into(), "1".into())])
            .unwrap();

        assert_eq!(
            db.get_option_string("clickhouse.setting.mutations_sync".into())
                .unwrap(),
            "1"
        );

        db.set_option("clickhouse.setting.mutations_sync".into(), "2".into())
            .unwrap();

        assert_eq!(
            db.get_option_string("clickhouse.setting.mutations_sync".into())
                .unwrap(),
            "2"
        );

        // Database-level settings are inherited by new connections
        let conn = db.new_connection().unwrap();

        assert_eq!(
            conn.get_option_string("clickhouse.setting.mutations_sync".into())
                .unwrap(),
            "2"
        );

        // Connection-level options override the database-level value
        let mut conn = db
            .new_connection_with_opts([("clickhouse.setting.mutations_sync".into(), "3".into())])
            .unwrap();

        assert_eq!(
            conn.get_option_string("clickhouse.setting.mutations_sync".into())
                .unwrap(),
            "3"
        );

        let mut statement = conn.new_statement().unwrap();

        assert_eq!(
            statement
                .get_option_string("clickhouse.setting.mutations_sync".into())
                .unwrap(),
            "3"
        );

        statement
            .set_option("clickhouse.setting.mutations_sync".into(), "0".into())
            .unwrap();

        assert_eq!(
            statement
                .get_option_string("clickhouse.setting.mutations_sync".into())
                .unwrap(),
            "0"
        );
    }

    #[test]
    fn test_set_setting_errors() {
        let mut driver = ClickhouseDriver::init();
        let mut db = driver.new_database().unwrap();

        // Empty setting name
        assert_eq!(
            db.set_option("clickhouse.setting.".into(), "1".into())
                .unwrap_err()
                .status,
            Status::InvalidArguments
        );

        assert_eq!(
            db.set_option(
                "clickhouse.setting.mutations_sync".into(),
                OptionValue::Int(3)
            )
            .unwrap_err()
            .status,
            Status::InvalidArguments
        );

        assert_eq!(
            db.get_option_string("clickhouse.setting.never_set".into())
                .unwrap_err()
                .status,
            Status::NotFound
        );

        let mut conn = db.new_connection().unwrap();

        assert_eq!(
            conn.set_option("clickhouse.setting.".into(), "1".into())
                .unwrap_err()
                .status,
            Status::InvalidArguments
        );

        assert_eq!(
            conn.get_option_string("clickhouse.setting.never_set".into())
                .unwrap_err()
                .status,
            Status::NotFound
        );

        let mut statement = conn.new_statement().unwrap();

        assert_eq!(
            statement
                .set_option("clickhouse.setting.".into(), "1".into())
                .unwrap_err()
                .status,
            Status::InvalidArguments
        );

        assert_eq!(
            statement
                .get_option_string("clickhouse.setting.never_set".into())
                .unwrap_err()
                .status,
            Status::NotFound
        );
    }

    #[test]
    fn test_set_uri() {
        let mut driver = ClickhouseDriver::init();

        let urls = [
            ("http://localhost:8123", "http://localhost:8123/"),
            ("https://localhost:8123", "https://localhost:8123/"),
            ("clickhouse://localhost:8123", "https://localhost:8123/"),
            (
                "clickhouse://localhost:8123?protocol=http",
                "http://localhost:8123/",
            ),
            (
                "clickhouse://localhost:8123/predefined_query_handler?protocol=http",
                "http://localhost:8123/predefined_query_handler",
            ),
            // Query parameters are folded into the settings map, not kept in the URL
            (
                "clickhouse://localhost:8123/predefined_query_handler?protocol=http&session_id=asdf1234",
                "http://localhost:8123/predefined_query_handler",
            ),
            // Preserves fragment (not used by HTTP interface but possibly useful for metadata)
            (
                "clickhouse://localhost:8123/predefined_query_handler?protocol=http&session_id=asdf1234#some-fragment",
                "http://localhost:8123/predefined_query_handler#some-fragment",
            ),
        ];

        for (original_url, expected_url) in urls {
            let mut database = driver.new_database().unwrap();

            database
                .set_option(OptionDatabase::Uri, original_url.into())
                .unwrap();

            assert_eq!(
                database.url.as_ref().map(Url::as_str),
                Some(expected_url),
                "original URL: {original_url:?}"
            );
        }

        // Invalid protocol override
        let mut database = driver.new_database().unwrap();

        assert_eq!(
            database
                .set_option(
                    OptionDatabase::Uri,
                    "clickhouse://localhost:8123?protocol=ftp".into()
                )
                .unwrap_err()
                .status,
            Status::InvalidArguments
        );
    }

    #[test]
    fn test_set_uri_settings() {
        let mut driver = ClickhouseDriver::init();

        let mut db = driver.new_database().unwrap();
        db.set_option(
            OptionDatabase::Uri,
            "http://localhost:8123?mutations_sync=3&alter_sync=2".into(),
        )
        .unwrap();

        assert_eq!(
            db.url.as_ref().map(Url::as_str),
            Some("http://localhost:8123/")
        );
        assert_eq!(
            db.get_option_string("clickhouse.setting.mutations_sync".into())
                .unwrap(),
            "3"
        );
        assert_eq!(
            db.get_option_string("clickhouse.setting.alter_sync".into())
                .unwrap(),
            "2"
        );

        // Last write wins per setting, in either direction
        db.set_option("clickhouse.setting.mutations_sync".into(), "1".into())
            .unwrap();
        assert_eq!(
            db.get_option_string("clickhouse.setting.mutations_sync".into())
                .unwrap(),
            "1"
        );

        db.set_option(
            OptionDatabase::Uri,
            "http://localhost:8123?mutations_sync=0".into(),
        )
        .unwrap();
        assert_eq!(
            db.get_option_string("clickhouse.setting.mutations_sync".into())
                .unwrap(),
            "0"
        );

        // `protocol` is reserved for clickhouse:// URLs
        assert_eq!(
            db.set_option(
                OptionDatabase::Uri,
                "http://localhost:8123?protocol=http".into()
            )
            .unwrap_err()
            .status,
            Status::InvalidArguments
        );
    }

    #[test]
    fn test_set_uri_database() {
        let mut driver = ClickhouseDriver::init();

        // (original URL, expected database)
        let urls = [
            ("http://localhost:8123", None),
            ("http://localhost:8123?database=my_db", Some("my_db")),
            ("https://localhost:8123?database=my_db", Some("my_db")),
            // Survives the `clickhouse://` scheme rewrite
            ("clickhouse://localhost:8123?database=my_db", Some("my_db")),
            (
                "clickhouse://localhost:8123?protocol=http&database=my_db",
                Some("my_db"),
            ),
            // Percent-encoded values are decoded
            ("http://localhost:8123?database=my%20db", Some("my db")),
        ];

        for (original_url, expected_db) in urls {
            let mut database = driver.new_database().unwrap();

            database
                .set_option(OptionDatabase::Uri, original_url.into())
                .unwrap();

            let conn = database.new_connection().unwrap();

            assert_eq!(
                conn.get_option_string(OptionConnection::CurrentSchema).ok(),
                expected_db.map(String::from),
                "original URL: {original_url:?}"
            );
        }

        // An empty database is rejected
        let mut database = driver.new_database().unwrap();
        let err = database
            .set_option(
                OptionDatabase::Uri,
                "http://localhost:8123?database=".into(),
            )
            .unwrap_err();
        assert_eq!(err.status, Status::InvalidArguments);
    }

    #[test]
    fn test_connection_current_schema() {
        let mut driver = ClickhouseDriver::init();

        let db = driver
            .new_database_with_opts([(
                OptionDatabase::Uri,
                "http://localhost:8123?database=from_url".into(),
            )])
            .unwrap();

        // Inherited from the URL
        let conn = db.new_connection().unwrap();
        assert_eq!(
            conn.get_option_string(OptionConnection::CurrentSchema)
                .unwrap(),
            "from_url"
        );

        // `CurrentSchema` overrides the URL parameter, whether set at creation or after
        let mut conn = db
            .new_connection_with_opts([(OptionConnection::CurrentSchema, "at_creation".into())])
            .unwrap();
        assert_eq!(
            conn.get_option_string(OptionConnection::CurrentSchema)
                .unwrap(),
            "at_creation"
        );

        conn.set_option(OptionConnection::CurrentSchema, "after_creation".into())
            .unwrap();
        assert_eq!(
            conn.get_option_string(OptionConnection::CurrentSchema)
                .unwrap(),
            "after_creation"
        );

        // The empty string clears the database; it reads back as never set
        conn.set_option(OptionConnection::CurrentSchema, "".into())
            .unwrap();
        assert_eq!(
            conn.get_option_string(OptionConnection::CurrentSchema)
                .unwrap_err()
                .status,
            Status::NotFound
        );

        // ...and the database may be set again after clearing
        conn.set_option(OptionConnection::CurrentSchema, "again".into())
            .unwrap();
        assert_eq!(
            conn.get_option_string(OptionConnection::CurrentSchema)
                .unwrap(),
            "again"
        );

        // Clearing also overrides a database inherited from the URL
        let mut conn = db.new_connection().unwrap();
        conn.set_option(OptionConnection::CurrentSchema, "".into())
            .unwrap();
        assert_eq!(
            conn.get_option_string(OptionConnection::CurrentSchema)
                .unwrap_err()
                .status,
            Status::NotFound
        );

        // Unset database reads back as `NotFound`
        let db = driver.new_database().unwrap();
        let mut conn = db.new_connection().unwrap();
        assert_eq!(
            conn.get_option_string(OptionConnection::CurrentSchema)
                .unwrap_err()
                .status,
            Status::NotFound
        );

        // Clearing a never-set database is a no-op
        conn.set_option(OptionConnection::CurrentSchema, "".into())
            .unwrap();
        assert_eq!(
            conn.get_option_string(OptionConnection::CurrentSchema)
                .unwrap_err()
                .status,
            Status::NotFound
        );

        // ClickHouse does not have catalogs
        assert_eq!(
            conn.set_option(OptionConnection::CurrentCatalog, "nope".into())
                .unwrap_err()
                .status,
            Status::InvalidArguments
        );
    }

    #[test]
    fn test_set_uri_rejects_invalid() {
        let mut driver = ClickhouseDriver::init();
        let mut database = driver.new_database().unwrap();

        let err = database
            .set_option(OptionDatabase::Uri, "localhost:8123".into())
            .unwrap_err();

        assert_eq!(err.status, adbc_core::error::Status::InvalidArguments);
        assert!(
            err.message.contains("unrecognized URL scheme"),
            "unexpected message: {:?}",
            err.message
        );

        let err = database
            .set_option(OptionDatabase::Uri, "localhost".into())
            .unwrap_err();

        assert_eq!(err.status, adbc_core::error::Status::InvalidArguments);
        assert!(
            err.message.contains("invalid URL"),
            "unexpected message: {:?}",
            err.message
        );
    }
}
