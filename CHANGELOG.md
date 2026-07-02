# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-07-01

First stable release of the driver.

### Breaking
* Updated `adbc_core` to `0.23`
* Updated `arrow-*` crates to `58.3.0`

### Added

* Added support for `OptionStatement::TargetDbSchema`, `OptionStatement::TargetTable` and `OptionStatement::IngestMode` 
  (but only `IngestMode::Append`/`"adbc.ingest.mode.append"` is currently accepted). ([#58])
* Added support for binding binary strings as parameters. ([#61])
* Added support for binding the [`arrow.uuid`](https://arrow.apache.org/docs/format/CanonicalExtensions.html#uuid)
  extension type as a literal UUID. ([#61])
* Added `clickhouse.client.output_string_as_string` option to map to 
  [`output_format_arrow_string_as_string`](https://clickhouse.com/docs/operations/settings/formats#output_format_arrow_string_as_string) setting. ([#61])
* Added support for URLs with the `clickhouse://` scheme for automatic detection in ADBC driver managers. ([#62])
    * Rewrites to `https://` scheme by default; add `?protocol=http` to the URL to override.
* Added eager parsing of URLs for immediate feedback instead of erroring on execute. ([#62])

### Changed
* Updated `clickhouse` to `0.15.1` ([#58])
* Added `clickhouse-ext-arrow` dependency and replaced `ArrowStreamReader` internals with it ([#58])
    * `ArrowStreamWriter` makes sense to keep because we can block on flushing the buffer in the internal `Write` impl
* Updated `rand` to `0.10` ([#58])
* Replaced stubbed-out `Database::get_option_string()` implementation. ([#62])
    * Read-back of `OptionDatabase::{Uri, Username, Password}` deliberately returns `NOT_FOUND` 
      to avoid exposing credentials.

### Fixed

* Fixed `Statement::execute()` returning an error on queries that returned no results ([#54])

[#54]: https://github.com/ClickHouse/adbc_clickhouse/pull/54
[#58]: https://github.com/ClickHouse/adbc_clickhouse/pull/58
[#61]: https://github.com/ClickHouse/adbc_clickhouse/pull/61
[#62]: https://github.com/ClickHouse/adbc_clickhouse/pull/62

## [0.1.0-alpha.1] - 2026-02-04

Initial, alpha-quality release of the driver.

Many methods are stubbed out and return `NotImplemented` errors.

However, the core query flow is supported:

* Creating a `Driver` and `Database`
* Setting URL, username and password on the `Database`
* Creating a `Connection` and `Statement`
* Setting a query with `Statement::set_sql_query()` and binding parameters with `Statement::bind()`
* Binding a statement in streaming insert mode with `Statement::bind_stream()`
* Executing a statement with `Statement::execute()` or `Statement::execute_update()`


[0.1.0]: https://github.com/ClickHouse/clickhouse-rs/compare/v0.1.0-alpha.1...v0.1.0
[0.1.0-alpha.1]: https://github.com/ClickHouse/adbc_clickhouse/releases/tag/v0.1.0-alpha.1
