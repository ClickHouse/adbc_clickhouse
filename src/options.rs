use adbc_core::error::{Error, Status};
use adbc_core::options::OptionValue;
use std::fmt::{Display, Formatter};
use std::ops::Range;
use std::str::FromStr;

/// Product info string to include in ClickHouse API calls.
///
/// This will be added to the `User-Agent` header when invoking the ClickHouse HTTP API,
/// which is useful for metrics.
///
/// The expected format of the string is `<product name>/<product version>`
/// with multiple product name/version pairs separated by spaces.
///
#[doc = concat!("The product info will automatically include the ADBC driver version (`adbc_clickhouse/", env!("CARGO_PKG_VERSION"), "`)")]
/// as well as the underlying ClickHouse Rust client version.
///
/// This option may be set at any level of the object hierarchy:
/// * `Database`
/// * `Connection`
/// * `Statement`
///
/// The set value will propagate to any newly created objects lower in the hierarchy.
///
/// # Example
/// ```rust
/// use adbc_clickhouse::ClickhouseDriver;
///
/// use adbc_core::{Driver, Database, Connection, Statement};
/// use adbc_core::options::OptionDatabase;
///
/// let driver = ClickhouseDriver::init();
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
/// ).unwrap()
///
/// let statement = db.new_statement_with_opts(
///     [
///         // Set the product info for this `Statement` only.
///         (
///             adbc_clickhouse::options::PRODUCT_INFO.into(),
///             "my_component/0.1.0-alpha.1 my_service/0.1.0 my_product/1.0.0".into(),
///         ),
///     ],
/// ).unwrap()
/// ```
pub const PRODUCT_INFO: &str = "clickhouse.client.product_info";

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
        match value {
            OptionValue::String(s) => s.parse(),
            other => Err(Error::with_message_and_status(
                format!("option {PRODUCT_INFO:?} must be a string; got: {other:?}"),
                Status::InvalidArguments,
            )),
        }
    }
}

#[test]
fn test_parse_product_info() {
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
