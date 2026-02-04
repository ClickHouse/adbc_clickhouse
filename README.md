# ClickHouse ADBC Driver

The official ClickHouse driver for [Arrow Database Connectivity (ADBC)](https://arrow.apache.org/adbc/current/index.html).

Implemented using the [official ClickHouse client for Rust](https://clickhouse.com/docs/integrations/rust).

Connects using the [ClickHouse HTTP interface][ch-http].

[ch-http]: https://clickhouse.com/docs/interfaces/http#overview

## Note: Work-in-Progress
This driver is still under active development and should not be considered ready for production use.

Many methods are stubbed out and return `NotImplemented` errors.

However, the core query flow is supported:

* Creating a `Driver` and `Database`
* Setting URL, username and password on the `Database`
* Creating a `Connection` and `Statement`
* Setting a query with `Statement::set_sql_query()` and binding parameters with `Statement::bind()`
* Binding a statement in streaming insert mode with `Statement::bind_stream()`
* Executing a statement with `Statement::execute()` or `Statement::execute_update()`

## Building

### Prerequisites

* Rust and [Cargo] 1.91 or newer
* Linux with `tls-native-tls` feature: 
  * GCC/G++ and libc headers (`build-essential`) or Clang 
  * OpenSSL/LibreSSL/BoringSSL headers (`libssl-dev` on Debian/Ubuntu)
* All platforms: 
  * `rustls-tls-aws-lc` feature: see [AWS Libcrypto requirements](https://aws.github.io/aws-lc-rs/requirements/)
  * `rustls-tls-ring` feature: see [`ring` build requirements](https://github.com/briansmith/ring/blob/main/BUILDING.md)

See below for feature descriptions.

### Driver DLL

The driver supports being built as a dynamic-link library (DLL) for loading with an [ADBC driver manager][adbc-driver].

Clone this repository and then choose one of the following `cargo build` commands
(refer to the feature descriptions below when customizing):

* Build with Native TLS support (OpenSSL on Linux, Secure Transport on macOS, SChannel on Windows):
```ignore
cargo build --release --features ffi,native-tls
```

* Build with Rustls and `aws-lc` and the native TLS root certificate store for the platform:
```ignore
cargo build --release --features ffi,rustls-tls
```

* Build without TLS support (the `ffi` feature is required for use with a driver manager):
```ignore
cargo build --release --features ffi
```

When finished, the driver DLL can be found under `target/release` with the conventional name and
extension for your platform:

* Linux, BSDs: `libadbc_clickhouse.so`
* macOS: `libadbc_clickhouse.dylib`
* Windows: `adbc_clickhouse.dll`

## Usage

### ADBC Driver Manager

Binary builds of the driver are pending.

Build the driver DLL as described in [Building](#building) above.

The driver DLL can then be loaded by path or by name (assuming it is on the search path for your platform)
using the driver manager API.

See [the ADBC documentation for your client language](https://arrow.apache.org/adbc/main/index.html) for details.

Because this driver uses the [ClickHouse HTTP interface][ch-http], the database URI (`ADBC_OPTION_URI` in `adbc.h`)
should use the `http://` or `https://` schemes.

### Rust API

The driver can be directly used as a Rust crate with or without the `ffi` feature:

`Cargo.toml`:
```toml
[dependencies]
adbc_clickhouse = "0.1.0-alpha.1"
adbc_core = "0.22.0"
arrow = "57.0.0"
```

```rust
use adbc_clickhouse::ClickhouseDriver;

use adbc_core::{Driver, Database, Connection, Statement};
use adbc_core::options::OptionDatabase;

use arrow::array::RecordBatchReader;
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;

let mut driver = ClickhouseDriver::init();

// A `Database` object in ADBC stores the login credentials for a specific database.
let database = driver.new_database_with_opts([
    // The driver connects using the ClickHouse HTTP interface:
    // https://clickhouse.com/docs/interfaces/http#overview
    (OptionDatabase::Uri, "http://localhost:8123".into()),
    (OptionDatabase::Username, "default".into()),
    (OptionDatabase::Password, "".into()),
])
    .unwrap();
    
// Each new connection uses a different `session_id`:
// https://clickhouse.com/docs/interfaces/http#using-clickhouse-sessions-in-the-http-protocol
// Note that the default session timeout is 60 seconds.
let mut connection = database.new_connection().unwrap();

// Each statement is assigned its own query ID.
let mut statement = connection.new_statement().unwrap();

statement
    .set_sql_query("SELECT number, 'test_' || number as name FROM system.numbers LIMIT 10")
    .unwrap();

let reader = statement.execute().unwrap(); // `impl RecordBatchReader`

// Required for `concat_batches()`.
// This could also be taken from the first `RecordBatch`, 
// but getting it from the reader works even when the result set is empty.
let schema = reader.schema(); 

let record_batches = reader
    .collect::<Result<Vec<RecordBatch>, ArrowError>>()
    .unwrap();
    
// In practice, ClickHouse should return the data for the above query in a single batch.
// However, for larger datasets, the data may be returned in multiple batches.
// For the sake of the example, we assume that we may have to concatenate multiple batches.
let record_batch = arrow::compute::concat_batches(&schema, &record_batches)
    .unwrap();
    
println!("{record_batch:?}");
```

[Cargo]: https://doc.rust-lang.org/cargo/index.html

### Feature Flags

#### ADBC Driver Manager Integration

When the `ffi` feature is enabled, this crate exports the `AdbcDriverInit()` and `AdbcClickhouseInit()` functions.

It then may be built as a dynamic library and loaded by an [ADBC driver manager][adbc-driver].

[adbc-driver]: https://arrow.apache.org/adbc/current/format/how_manager.html

#### TLS Support

This package exposes the same Transport Layer Security (TLS) features as
[the `clickhouse` crate](https://github.com/ClickHouse/clickhouse-rs?tab=readme-ov-file#tls) it uses under the hood:

* `native-tls`: use the native TLS implementation for the platform
  * OpenSSL on Linux
  * SChannel on Windows
  * Secure Transport on macOS
* `rustls-tls`: enables both `rustls-tls-aws-lc` and `rustls-tls-webpki-roots`
* `rustls-tls-aws-lc`: use [Rustls] with the [`aws-lc`] cryptography provider
* `rustls-tls-ring`: use [Rustls] with the [`ring`] cryptography provider
* `rustls-tls-native-roots`: configure [Rustls] to use the native TLS root certificate store for the platform
* `rustls-tls-webpki-roots`: configure [Rustls] to use a statically compiled set of TLS roots ([`webpki-roots`] crate)

Note that Rustls has no TLS roots by default; when using the `rustls-tls-aws-lc` or `rustls-tls-ring` features,
you should also enable either `rustls-tls-native-roots` or `rustls-tls-webpki-roots` to choose a TLS root store.

[Rustls]: https://github.com/rustls/rustls
[`aws-lc`]: https://github.com/aws/aws-lc-rs
[`ring`]: https://github.com/briansmith/ring
[`webpki-roots`]: https://github.com/rustls/webpki-roots

## Support Policies

This project adopts the same support policies as the [ClickHouse Rust client](https://github.com/ClickHouse/clickhouse-rs/tree/main?tab=readme-ov-file#support-policies).

### Minimum Supported Rust Version (MSRV)

This project's MSRV is the second-to-last stable release as of the beginning of the current release cycle (`0.x.0`),
where it will remain until the beginning of the _next_ release cycle (`0.{x+1}.0`).

The MSRV for the `0.1.0` release cycle is `1.91.0`.

This guarantees that `adbc_clickhouse` will compile with a Rust version that is at _least_ six weeks old,
which should be plenty of time for it to make it through any packaging system that is being actively kept up to date.

Beware when installing Rust through operating system package managers, as it can often be a year or more
out-of-date. For example, Debian Bookworm (released 10 June 2023) shipped with Rust 1.63.0 (released 11 August 2022).

### ClickHouse Versions

The supported versions of the ClickHouse database server coincide with the versions currently receiving security
updates.

For the list of currently supported versions, see <https://github.com/ClickHouse/ClickHouse/blob/master/SECURITY.md#security-change-log-and-support>.

## License

Licensed under either of

-   Apache License, Version 2.0
    ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
-   MIT license
    ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.
