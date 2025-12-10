# ClickHouse ADBC Driver

The official ClickHouse driver for [Arrow Database Connectivity (ADBC)](https://arrow.apache.org/adbc/current/index.html).

Implemented using the [official ClickHouse client for Rust](https://clickhouse.com/docs/integrations/rust).

## Feature Flags

### TLS Support

This package exposes the same TLS features as 
[the `clickhouse` crate it uses under the hood](https://github.com/ClickHouse/clickhouse-rs?tab=readme-ov-file#tls).
