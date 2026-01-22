use adbc_core::error::{Error, Status};
use adbc_core::options::OptionValue;
use std::str::FromStr;
use std::sync::Arc;

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
/// * `Driver`
/// * `Database`
/// * `Connection`
/// * `Statement`
///
/// The set value will propagate to any new sub-objects lower in the hierarchy.
pub const PRODUCT_INFO: &str = "adbc.clickhouse.sql.client_option.product_info";

#[derive(Clone, Debug, Default)]
pub(crate) struct ProductInfo {
    pairs: Arc<[(Box<str>, Box<str>)]>,
}

impl ProductInfo {
    pub fn is_empty(&self) -> bool {
        self.pairs.is_empty()
    }

    pub fn apply(&self, mut client: clickhouse::Client) -> clickhouse::Client {
        // Product info items are added to the `User-Agent` header in reverse order:
        // https://docs.rs/clickhouse/latest/clickhouse/struct.Client.html#method.with_product_info
        for (name, value) in self.pairs.iter().rev() {
            client = client.with_product_info(&**name, &**value);
        }

        client
    }
}

impl FromStr for ProductInfo {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let pairs = s
            .split_whitespace()
            .map(|info_item| {
                let (name, value) = info_item
                    .split_once('/')
                    .ok_or_else(|| format!("expected `/` in product info item: {info_item:?}"))?;

                Ok((name.into(), value.into()))
            })
            .collect::<Result<Arc<[_]>, String>>()
            .map_err(|e| {
                Error::with_message_and_status(
                    format!("error parsing option {PRODUCT_INFO:?}: {e}"),
                    Status::InvalidArguments,
                )
            })?;

        Ok(Self { pairs })
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
