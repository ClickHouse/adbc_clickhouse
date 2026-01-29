# ClickHouse ADBC Driver

The official ClickHouse driver for [Arrow Database Connectivity (ADBC)](https://arrow.apache.org/adbc/current/index.html).

Implemented using the [official ClickHouse client for Rust](https://clickhouse.com/docs/integrations/rust).

## Note: Work-in-Progress
This driver is still under active development and should not be considered ready for production use.

Many methods are stubbed out and return `NotImplemented` errors.

However, the core query flow is supported:

* Creating a `Driver` and `Database`
* Setting URL, username and password on the `Database`
* Creating a `Connection` and `Statement`
* Setting a query with `Statement::set_sql_query()` and binding parameters with `Statement::bind()`
* Binding a statement in streaming insert mode with `Statement::bind_params()`
* Executing a statement with `Statement::execute()` or `Statement::execute_update()`

## Usage

### ADBC Driver Manager

Binary builds of the driver are pending.

It may be built from source using Rust 1.91 or newer and [Cargo], Rust's first-party build tool and package manager.

After installing or updating Rust, clone this repository and then choose one of the following `cargo build` commands
(refer to the feature descriptions below when customizing):

* Build with Native TLS support (OpenSSL on Linux, Secure Transport on macOS, SChannel on Windows):
```
cargo build --release --features ffi,native-tls
```

* Build with Rustls and `aws-lc` and the native TLS root certificate store for the platform:
```
cargo build --release --features ffi,rustls-tls
```

* Build without TLS support (the `ffi` feature is required for use with a driver manager):
```
cargo build --release --features ffi
```

When finished, the driver dynamic-link library (DLL) can be found under `target/release` with the conventional name and 
extension for your platform:

* Linux, BSDs: `libadbc_clickhouse.so`
* macOS: `libadbc_clickhouse.dylib`
* Windows: `adbc_clickhouse.dll`

If the driver manager has the option to load the driver by name, pass `adbc_clickhouse` instead of the DLL filename.
The driver DLL must be available in the dynamic link search path for your platform.

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
