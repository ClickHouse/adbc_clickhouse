//! Keys for setting options on various ClickHouse ADBC objects.

use adbc_core::error::{Error, Status};
use adbc_core::options::OptionValue;
use std::fmt::{Display, Formatter};
use std::ops::Range;
use std::str::FromStr;
use std::time::Duration;

#[cfg(doc)]
use {
    crate::{ClickhouseConnection, ClickhouseDatabase, ClickhouseStatement},
    adbc_core::{Database, Optionable},
};

/// Product info string to include in ClickHouse API calls.
///
/// This will be added to the `User-Agent` header when invoking the ClickHouse HTTP API,
/// which is useful for identification in query logs
/// (`http_user_agent` in the [`system.query_log`] view) and capture of metrics.
///
/// [`system.query_log`]: https://clickhouse.com/docs/operations/system-tables/query_log#columns
///
/// The expected format of the string is `<product name>/<product version>`
/// with multiple product name/version pairs separated by spaces.
///
#[doc = concat!("The product info will automatically include the ADBC driver version (`adbc_clickhouse/", env!("CARGO_PKG_VERSION"), "`)")]
/// as well as the underlying ClickHouse Rust client version.
///
/// This option may be set at any level of the object hierarchy:
/// * [`ClickhouseDatabase`]
/// * [`ClickhouseConnection`]
/// * [`ClickhouseStatement`]
///
/// The set value will propagate to any newly created objects lower in the hierarchy.
///
/// # Example
/// ```rust
/// use adbc_clickhouse::ClickhouseDriver;
///
/// use adbc_core::{Driver, Database, Connection, Statement, Optionable};
/// use adbc_core::options::OptionDatabase;
///
/// let mut driver = ClickhouseDriver::init();
///
/// let db = driver.new_database_with_opts(
///     [
///         (OptionDatabase::Uri, "http://localhost:8123/".into()),
///         // Set the product info for all connections subsequently created with this `Database`.
///         (
///             adbc_clickhouse::options::PRODUCT_INFO.into(),
///             "my_product/1.0.0".into(),
///         ),
///     ],
/// ).unwrap();
///
/// let mut conn = db.new_connection_with_opts(
///     [
///         // Set the product info for all statements subsequently created with this `Connection`.
///         (
///             adbc_clickhouse::options::PRODUCT_INFO.into(),
///             // Setting the product info overrides the previous setting,
///             // so all the product info you want to include should be in the same string.
///             "my_service/0.1.0 my_product/1.0.0".into(),
///         ),
///     ],
/// ).unwrap();
///
/// let mut statement = conn.new_statement().unwrap();
///
/// // Set the product info for this `Statement` only.
/// statement.set_option(
///     adbc_clickhouse::options::PRODUCT_INFO.into(),
///     "my_component/0.1.0-alpha.1 my_service/0.1.0 my_product/1.0.0".into(),
/// ).unwrap();
/// ```
pub const PRODUCT_INFO: &str = "clickhouse.client.product_info";

/// Session ID string to use with the [ClickHouse HTTP interface][http-session_id].
///
/// This session ID is used to persist any shared state between statements, such as [settings]
/// and [temporary tables]. By default, session state persists for 60 seconds.
///
/// May be set on `ClickhouseConnection` or `ClickhouseStatement`.
///
/// When a `ClickhouseConnection` is created, a random session ID is generated if one is not passed
/// in via [`Database::new_connection_with_opts()`].
///
/// It may be read back with [`Optionable::get_option_string()`].
///
/// [http-session_id]: https://clickhouse.com/docs/interfaces/http#using-clickhouse-sessions-in-the-http-protocol
/// [settings]: https://clickhouse.com/docs/sql-reference/statements/set
/// [temporary tables]: https://clickhouse.com/docs/sql-reference/statements/create/table#temporary-tables
pub const SESSION_ID: &str = "clickhouse.client.session_id";

/// ID string for the current query for explicit cancellation and identification in query logs.
///
/// Only supported on `ClickhouseStatement`.
///
/// An ID is pre-generated when the statement is created, which can be read back for later reference.
///
/// # Example
///
/// Get the pre-generated query ID:
/// ```rust
/// use adbc_clickhouse::ClickhouseDriver;
///
/// use adbc_core::{Driver, Database, Connection, Statement, Optionable};
///
/// let mut driver = ClickhouseDriver::init();
///
/// let db = driver.new_database().unwrap();
/// let mut conn = db.new_connection().unwrap();
///
/// let statement = conn.new_statement().unwrap();
///
/// let query_id = statement.get_option_string(adbc_clickhouse::options::QUERY_ID.into()).unwrap();
/// println!("query ID: {query_id}");
/// ```
///
/// Explicitly set a query ID:
/// ```rust
/// use adbc_clickhouse::ClickhouseDriver;
///
/// use adbc_core::{Driver, Database, Connection, Statement, Optionable};
///
/// let mut driver = ClickhouseDriver::init();
///
/// let db = driver.new_database().unwrap();
/// let mut conn = db.new_connection().unwrap();
///
/// let mut statement = conn.new_statement().unwrap();
///
/// statement.set_option(adbc_clickhouse::options::QUERY_ID.into(), "asdf1234".into()).unwrap();
///
/// let query_id = statement.get_option_string(adbc_clickhouse::options::QUERY_ID.into()).unwrap();
///
/// assert_eq!(query_id, "asdf1234");
/// ```
pub const QUERY_ID: &str = "clickhouse.client.query_id";

/// Sets whether ClickHouse reports all `String` values as Arrow UTF-8 strings (enabled by default).
///
/// Set to `"false"` (ADBC convention) or `"0"` (ClickHouse convention) to have ClickHouse output
/// all `String` types as binary instead.
///
/// Maps to the [`output_format_arrow_string_as_string`] setting.
///
/// May be set on [`ClickhouseConnection`] or [`ClickhouseStatement`].
///
/// **Note**: `FixedString(N)` is always reported as binary because Arrow does not have
/// a fixed-size UTF-8 string type.
///
/// [`output_format_arrow_string_as_string`]: https://clickhouse.com/docs/operations/settings/formats#output_format_arrow_string_as_string
///
/// # Motivation
/// ClickHouse's type system does not differentiate between binary strings and UTF-8 strings.
/// While the `String` type is _conventionally_ UTF-8, this is not enforced.
///
/// For convenience, ClickHouse defaults to reporting all `String` values as Arrow UTF-8 strings
/// without any validation, which could result in deserialization errors at runtime if a string
/// is encountered that is not valid UTF-8.
///
/// Setting this to `"false"`/`"0"` causes ClickHouse to report all `String` values
/// as Arrow binary strings instead.
pub const OUTPUT_STRING_AS_STRING: &str = "clickhouse.client.output_string_as_string";

/// Maximum time, in seconds, that a query may wait to complete before it is abandoned.
///
/// Any query executed on a [`ClickhouseConnection`] or [`ClickhouseStatement`] with this
/// option set is bound end-to-end: if the server has not finished responding within the
/// configured window, the client stops waiting and drops the in-flight HTTP request, and
/// execution returns an error with [`Status::Timeout`]. Whether the server also stops the
/// query depends on server settings such as `cancel_http_readonly_queries_on_client_close`;
/// pairing this option with the server-side `max_execution_time` setting bounds server work.
///
/// This is a client-side safeguard for deployments where an unresponsive or very slow query
/// would otherwise occupy a worker, connection, or runtime thread indefinitely. It complements,
/// but does not replace, true cancellation (see [#20]).
///
/// May be set on [`ClickhouseConnection`] or [`ClickhouseStatement`].
/// A timeout set on a `Connection` propagates to every `Statement` created from it,
/// and a `Statement` may override it.
///
/// The value is a string containing a positive integer number of seconds
/// (e.g. `"30"`). Zero, negative values, and non-numeric values are rejected with
/// [`Status::InvalidArguments`]. The current value may be read back with
/// [`Optionable::get_option_string()`].
///
/// [#20]: https://github.com/ClickHouse/adbc_clickhouse/issues/20
///
/// # Example
/// ```rust
/// use adbc_clickhouse::ClickhouseDriver;
///
/// use adbc_core::{Driver, Database, Connection, Optionable};
///
/// let mut driver = ClickhouseDriver::init();
///
/// let db = driver.new_database().unwrap();
/// let mut conn = db.new_connection().unwrap();
///
/// // Abandon any query that has not completed within 60 seconds.
/// conn.set_option(
///     adbc_clickhouse::options::QUERY_TIMEOUT.into(),
///     "60".into(),
/// ).unwrap();
///
/// assert_eq!(
///     conn.get_option_string(adbc_clickhouse::options::QUERY_TIMEOUT.into()).unwrap(),
///     "60"
/// );
/// ```
pub const QUERY_TIMEOUT: &str = "clickhouse.client.query_timeout";

/// Prefix for passing arbitrary [ClickHouse settings] through as driver options.
///
/// An option key of the form `clickhouse.setting.<setting_name>` maps directly to the
/// ClickHouse setting `<setting_name>`, which is sent as a URL query parameter on every
/// call to the [ClickHouse HTTP interface]. Values must be strings, using the same textual
/// representation as a [`SET` statement][set] (e.g. `"1"`, `"3"`, `"auto"`).
///
/// May be set on [`ClickhouseDatabase`], [`ClickhouseConnection`] or [`ClickhouseStatement`];
/// like [`PRODUCT_INFO`], the value propagates to newly created objects lower in the
/// hierarchy, and lower levels may override it.
///
/// The current value may be read back with [`Optionable::get_option_string()`],
/// which returns an error with [`Status::NotFound`] if the setting has not been set.
///
/// Settings may also be passed as query parameters in the connection URI;
/// see [`ClickhouseDatabase`] for details.
///
/// Settings covered by a dedicated `clickhouse.client.*` option (e.g. [`SESSION_ID`]) may
/// also be set through this prefix; both forms write the same client state, last write wins.
///
/// [ClickHouse settings]: https://clickhouse.com/docs/operations/settings/settings
/// [ClickHouse HTTP interface]: https://clickhouse.com/docs/interfaces/http
/// [set]: https://clickhouse.com/docs/sql-reference/statements/set
///
/// # Example
/// ```rust
/// use adbc_clickhouse::ClickhouseDriver;
///
/// use adbc_core::{Driver, Database, Optionable};
///
/// let mut driver = ClickhouseDriver::init();
///
/// // Set a ClickHouse setting for all connections subsequently created with this `Database`.
/// let db = driver
///     .new_database_with_opts([("clickhouse.setting.mutations_sync".into(), "1".into())])
///     .unwrap();
///
/// let conn = db.new_connection().unwrap();
///
/// assert_eq!(
///     conn.get_option_string("clickhouse.setting.mutations_sync".into())
///         .unwrap(),
///     "1"
/// );
/// ```
pub const SETTING_PREFIX: &str = "clickhouse.setting.";

pub(crate) fn setting_name(key: &str) -> Option<&str> {
    key.strip_prefix(SETTING_PREFIX)
        .filter(|name| !name.is_empty())
}

pub(crate) fn setting_not_set(name: &str) -> Error {
    Error::with_message_and_status(format!("setting {name:?} is not set"), Status::NotFound)
}

/// Parse a client-side query timeout option value as a count of seconds.
///
/// Returns an [`Error`] with [`Status::InvalidArguments`] for zero, negative,
/// and non-numeric values.
pub(crate) fn parse_timeout(key: &str, value: &str) -> Result<Duration, Error> {
    let seconds = value.parse::<u64>().map_err(|_| {
        Error::with_message_and_status(
            format!(
                "error setting option {key:?} = {value:?}: expected a positive integer number of seconds"
            ),
            Status::InvalidArguments,
        )
    })?;

    if seconds == 0 {
        return Err(Error::with_message_and_status(
            format!("error setting option {key:?} = {value:?}: must be greater than zero"),
            Status::InvalidArguments,
        ));
    }

    Ok(Duration::from_secs(seconds))
}

/// Server-side settings for the public options in the parent module.
///
/// Prefer declaring constants over magic strings to avoid typos.
pub(crate) mod as_setting {
    pub const SESSION_ID: &str = "session_id";

    pub const QUERY_ID: &str = "query_id";

    pub const OUTPUT_STRING_AS_STRING: &str = "output_format_arrow_string_as_string";
}

#[derive(Clone, Debug, Default)]
pub(crate) struct ProductInfo {
    /// The original string that was passed for lossless read-back.
    source_str: Box<str>,
    /// Ranges for each product name and version in `source_str`.
    pair_ranges: Box<[(Range<usize>, Range<usize>)]>,
}

impl ProductInfo {
    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.pair_ranges.is_empty()
    }

    pub fn pairs(&self) -> impl DoubleEndedIterator<Item = (&'_ str, &'_ str)> + '_ {
        self.pair_ranges
            .iter()
            .cloned()
            .map(|(k, v)| (&self.source_str[k], &self.source_str[v]))
    }

    pub fn apply(&self, mut client: clickhouse::Client) -> clickhouse::Client {
        // Product info items are added to the `User-Agent` header in reverse order:
        // https://docs.rs/clickhouse/latest/clickhouse/struct.Client.html#method.with_product_info
        for (name, version) in self.pairs().rev() {
            client = client.with_product_info(name, version);
        }

        client
    }
}

impl FromStr for ProductInfo {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // I didn't want to spend a bunch of time on this parsing algorithm,
        // so it deliberately doesn't handle really any edge cases.
        //
        // We can address those as possible use-cases come up.
        let mut pair_ranges = Vec::new();
        let mut range_start = 0;
        let mut breaks = s.match_indices(char::is_whitespace);

        while range_start < s.len() {
            let (range_end, break_len) = breaks
                .next()
                .map_or((s.len(), 0), |(i, spaces)| (i, spaces.len()));

            let item = &s[range_start..range_end];

            if item.is_empty() {
                range_start = range_end + break_len;
                continue;
            }

            let split_at = item.find('/').ok_or_else(|| {
                Error::with_message_and_status(
                    format!(
                        "error parsing option {PRODUCT_INFO:?}; \
                         expected '/' in item {item:?} ({range_start}..{range_end}) \
                         of product info string {s:?}"
                    ),
                    Status::InvalidArguments,
                )
            })?;

            pair_ranges.push((
                range_start..range_start + split_at,
                range_start + split_at + 1..range_end,
            ));

            range_start = range_end + break_len;
        }

        Ok(Self {
            source_str: s.into(),
            pair_ranges: pair_ranges.into(),
        })
    }
}

impl Display for ProductInfo {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.source_str)
    }
}

impl TryFrom<OptionValue> for ProductInfo {
    type Error = Error;

    fn try_from(value: OptionValue) -> Result<Self, Self::Error> {
        value.try_string(PRODUCT_INFO)?.parse()
    }
}

pub(crate) trait OptionValueExt {
    fn try_string(self, key: impl AsRef<str>) -> Result<String, Error>;
}

impl OptionValueExt for OptionValue {
    fn try_string(self, key: impl AsRef<str>) -> Result<String, Error> {
        if let Self::String(s) = self {
            Ok(s)
        } else {
            Err(Error::with_message_and_status(
                format!(
                    "expected string for option {:?}, got {self:?}",
                    key.as_ref()
                ),
                Status::InvalidArguments,
            ))
        }
    }
}

#[test]
fn test_product_info_from_str() {
    let info = "".parse::<ProductInfo>().unwrap();
    assert_eq!(&*info.source_str, "");
    assert_eq!(*info.pair_ranges, []);
    assert_eq!(info.pairs().collect::<Vec<_>>(), []);

    let info = "my_product/1.0.0".parse::<ProductInfo>().unwrap();
    assert_eq!(&*info.source_str, "my_product/1.0.0");
    assert_eq!(*info.pair_ranges, [(0..10, 11..16)]);
    assert_eq!(info.pairs().collect::<Vec<_>>(), [("my_product", "1.0.0")]);

    let info = "my_service/0.1.0 my_product/1.0.0"
        .parse::<ProductInfo>()
        .unwrap();
    assert_eq!(&*info.source_str, "my_service/0.1.0 my_product/1.0.0");
    assert_eq!(*info.pair_ranges, [(0..10, 11..16), (17..27, 28..33)]);
    assert_eq!(
        info.pairs().collect::<Vec<_>>(),
        [("my_service", "0.1.0"), ("my_product", "1.0.0")]
    );

    let info = "my_component/0.1.0-alpha.1 my_service/0.1.0 my_product/1.0.0"
        .parse::<ProductInfo>()
        .unwrap();
    assert_eq!(
        &*info.source_str,
        "my_component/0.1.0-alpha.1 my_service/0.1.0 my_product/1.0.0"
    );
    assert_eq!(
        *info.pair_ranges,
        [(0..12, 13..26), (27..37, 38..43), (44..54, 55..60)]
    );
    assert_eq!(
        info.pairs().collect::<Vec<_>>(),
        [
            ("my_component", "0.1.0-alpha.1"),
            ("my_service", "0.1.0"),
            ("my_product", "1.0.0")
        ]
    );

    // Oops, all whitespace
    let info = " \t\r\n".parse::<ProductInfo>().unwrap();
    assert_eq!(&*info.source_str, " \t\r\n");
    assert_eq!(*info.pair_ranges, []);
    assert_eq!(info.pairs().collect::<Vec<_>>(), []);

    // Excess whitespace
    let info = "\t my_service/0.1.0 \t my_product/1.0.0 \t"
        .parse::<ProductInfo>()
        .unwrap();
    assert_eq!(
        &*info.source_str,
        "\t my_service/0.1.0 \t my_product/1.0.0 \t"
    );
    assert_eq!(*info.pair_ranges, [(2..12, 13..18), (21..31, 32..37)]);
    assert_eq!(
        info.pairs().collect::<Vec<_>>(),
        [("my_service", "0.1.0"), ("my_product", "1.0.0")]
    );
}

#[test]
fn test_product_info_from_option_value() {
    let info = ProductInfo::try_from(OptionValue::String(
        "\t my_service/0.1.0 \t my_product/1.0.0 \t".into(),
    ))
    .unwrap();
    assert_eq!(
        info.pairs().collect::<Vec<_>>(),
        [("my_service", "0.1.0"), ("my_product", "1.0.0")]
    );

    ProductInfo::try_from(OptionValue::Int(12345678)).unwrap_err();
    ProductInfo::try_from(OptionValue::Double(12345678.90)).unwrap_err();

    // We could possibly support parsing from bytes but that's extra complexity we don't really need.
    ProductInfo::try_from(OptionValue::Bytes(
        b"\t my_service/0.1.0 \t my_product/1.0.0 \t".into(),
    ))
    .unwrap_err();
}

#[test]
fn test_parse_timeout_valid() {
    assert_eq!(parse_timeout("key", "1").unwrap(), Duration::from_secs(1));
    assert_eq!(parse_timeout("key", "30").unwrap(), Duration::from_secs(30));
    assert_eq!(
        parse_timeout("key", "3600").unwrap(),
        Duration::from_secs(3600)
    );
}

#[test]
fn test_parse_timeout_refuses_invalid() {
    use adbc_core::error::Status;

    for bad in ["0", "-5", "5.5", "abc", "", " "] {
        let err = parse_timeout("key", bad).unwrap_err();
        assert_eq!(err.status, Status::InvalidArguments, "value {bad:?}");
    }
}
