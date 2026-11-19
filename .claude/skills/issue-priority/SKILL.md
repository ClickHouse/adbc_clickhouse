---
name: issue-priority
description: >
  Decide the priority of a `ClickHouse/adbc_clickhouse` issue: `Urgent`,
  `High`, `Medium`, or `Low`, for the org issue field **Priority**. Use this
  skill when the task is to "prioritize", "set the priority of", "rate", or
  "triage" an issue, or to explain why an issue has a given priority. It
  defines the reach tiers of the driver, the severity rows, the base-level
  matrix, the modifiers, and the confidence.
---

# Issue priority

This skill is the single source of truth for the **Priority** of
`ClickHouse/adbc_clickhouse` issues. People and automated triage use the same
rules.

The value goes into the org issue field **Priority** (`Urgent`, `High`,
`Medium`, `Low`). The field is visible to org members only. An automated tool
sets the value once, never changes a value that is already set, and keeps the
level and its reason out of public comments.

## When to set no priority

Set no priority when one of these is true:

- A maintainer can close the issue with no further action: it is already fixed
  on `main`, a duplicate, works as intended, already supported, answered, or
  obsolete. The ADBC methods and options that return `NotImplemented` have
  tracking issues (`README.md`, "Development Status"): a new request for the
  same method or option is a duplicate. `commit()` and `rollback()` return
  `NotImplemented` because ClickHouse has no transactions: works as intended.
- Only a deprecated public API reaches the defect (a `#[deprecated]`
  attribute, or docs that call the API deprecated). Fixes to deprecated code
  are not released.
- The issue is a question, or it is about CI, the release process, or a
  discussion.

## Vulnerabilities that are not public yet

A security issue must not stay public with a description of how to reproduce
it. This section applies when a reproduction or code inspection confirms a
vulnerability that is not public yet, as the classification skill of this
repository defines it
([`../issue-classification/SKILL.md`](../issue-classification/SKILL.md), "A
vulnerability that is not public yet").

- Report it in a private GitHub vulnerability report for this repository
  (**Security** → **Report a vulnerability**), never in a public issue,
  comment, or pull request. If a public issue reports it, copy the title, the
  text, and the comments of the issue into the report first.
- Set no Priority on the public issue. Close it with a comment that gives no
  details, and ask a repository administrator to delete it: closing does not
  hide the text of the issue.
- Rate the vulnerability with the rules below, and put the level and its reason
  into the private report as the proposed severity.

A vulnerability that is already public (a published CVE or GHSA, for example in
a dependency), and an authentication method that does not work or is missing,
get a Priority on the issue as usual.

## 1. Reach

**Reach** tells how many applications can hit the defect. There are four
tiers, highest first:

- **Core:** almost every application uses this path.
- **Common:** many applications use it, but not all.
- **Narrow:** few applications use it (rare types, special setups).
- **Satellite:** applications do not use it (examples, tests, docs, tools).

The reach is the tier of the most-used public path that reaches the defect,
not the tier of the file that contains it.

The driver has two kinds of users:

- Applications that load the driver through the ADBC C API (feature `ffi`)
  with a driver manager. Most users are of this kind, and many of them use
  prebuilt driver binaries.
- Rust applications that use the crate directly, through the `adbc_core`
  traits.

Both kinds run the same `ClickhouseDriver`, `ClickhouseDatabase`,
`ClickhouseConnection`, and `ClickhouseStatement` code, so the tier comes from
the ADBC operation, not from the kind of user.

The Python DB-API module of `adbc_driver_manager`
(`python/adbc_driver_manager/adbc_driver_manager/dbapi.py` in
`apache/arrow-adbc`) calls the driver as follows:

- `Cursor.execute()`: `set_sql_query()`, `prepare()` (it ignores a
  `NotImplemented` error), `bind()` or `bind_stream()` when the call has
  parameters, then `execute()`.
- `Cursor.executemany()`: `set_sql_query()`, `prepare()` (it does not ignore a
  `NotImplemented` error), `bind()` or `bind_stream()`, then
  `execute_update()`.
- `Cursor.adbc_ingest()`: the bulk ingest options (the default mode is
  `create`), `bind()` or `bind_stream()`, then `execute_update()`.
- `Cursor.executescript()`: `execute_update()`.
- `Connection.adbc_get_table_schema()`: `get_table_schema()`.

dbt-fusion sets the URI, the username, and the password. It sends DDL and DML
to `execute_update()`, and all other statements, also its metadata queries, to
`execute()` (`crates/dbt-adapter/src/engine/adapter_engine.rs` in
`dbt-labs/dbt-fusion`).

The driver uses three crates of `ClickHouse/clickhouse-rs`: `clickhouse` (the
HTTP transport, server errors, settings, query parameters, inserts),
`clickhouse-types` (the type-name parser of `get_table_schema()`), and
`clickhouse-ext-arrow` (the `ArrowStream` cursor of `Statement::execute()`). A
defect in one of these crates has the tier of the driver path that reaches it
(section 7). For example, a defect in the `ArrowStream` cursor is Core, because
every `Statement::execute()` reads through it, and a `clickhouse-types` parser
defect for `Map` is Common, because only `get_table_schema()` parses type
names.

| Tier      | Public paths |
| --------- | ------------ |
| Core      | loading the driver through the C API (`AdbcDriverInit()`, `AdbcClickhouseInit()`) and `ClickhouseDriver::init()`; `Database` options `uri` (`http`, `https`, `clickhouse` schemes, `?database=`), `username`, `password`; `Database::new_connection()` (one session ID for each connection); `Statement::set_sql_query()` with `execute()` for queries, and with `execute()` or `execute_update()` for DDL and DML; reading the `ArrowStream` result (`src/reader.rs`, `clickhouse-ext-arrow`), also an empty result; the HTTP transport, TLS (`native-tls`, `rustls-tls*`; ClickHouse Cloud needs it), and the compression of the `clickhouse` crate; server errors before and during the result stream, and their ADBC status and message; the result data of `(U)Int8`–`(U)Int64`, `Float32`, `Float64`, `Bool`, `String`, `FixedString`, `Date`, `Date32`, `DateTime`, `DateTime64`, `Decimal*`, `UUID`, `Nullable(T)`, `Array(T)`, and `LowCardinality(String)`; the build of the crate (it has no default features), and of the driver library with `ffi` and a TLS feature (as the prebuilt binaries use it), with the MSRV, on Linux |
| Common    | bulk ingest (`OptionStatement::TargetTable`, `TargetDbSchema`, `IngestMode::Append`, with `bind()` or `bind_stream()` and `execute_update()`) and streaming inserts (`INSERT INTO … FORMAT ArrowStream` with `bind_stream()`); `Cursor.executemany()`; query parameters (`bind()` or `bind_stream()` with one row, also for a query that is not an insert; `bind_scalar()` for `Boolean`, integers, floats, strings, binary strings, `Timestamp`, `Date32`, decimals); `Connection::get_table_schema()` (`DESCRIBE TABLE`, the type mapping of `src/schema.rs`, `clickhouse-types`); the `clickhouse.setting.<name>` options at each level; the options `clickhouse.client.product_info`, `session_id`, `query_id`, `output_string_as_string`, `OptionConnection::CurrentSchema`, and the option getters; the result data of `Map`, `Tuple`, `Enum8`, `Enum16`, `IPv4`, `IPv6`, `(U)Int128`, `(U)Int256`, and the other `LowCardinality` and nested wrappers; a Rust caller with its own Tokio runtime (`ClickhouseDriver::init_with()`, or `init()` in a multi-thread runtime); the builds with the other TLS features; the build on macOS and Windows, which CI does not test |
| Narrow    | rare types (`Variant`, `Dynamic`, `JSON`, `Time`, `Time64`, `Interval`, `BFloat16`, `AggregateFunction`, `Nested`, `Nothing`, the geo types); unusual type names (escaped `Enum` labels, deep nesting); the parameter types with a special mapping (`Date64`, `Time32`, `Time64`, `Duration`, the `arrow.uuid` extension type); `get_table_types()`, `get_statistic_names()`; input that a correct server does not send; caller input that the ADBC API does not allow, but that the driver does not reject; another platform, compiler, or dependency version that CI does not test |
| Satellite | `tests/`, the examples in `README.md` and in the crate docs, `CHANGELOG.md`, `agents.md`, the agent skills in `.claude/`; a defect only in the packaging of the prebuilt driver binaries that other projects maintain (the ADBC Driver Foundry, `dbc`, the binaries that dbt-fusion downloads), for example their build flags. A defect in the driver code of such a binary has the tier of its driver path |

## 2. Severity

First name the **impacts** of the issue with the classification skill of this
repository
([`../issue-classification/SKILL.md`](../issue-classification/SKILL.md)): none,
one, or more of Security (part vulnerability or part authentication), Data
integrity, Stability, and Performance. Most issues have none.

Then use the first severity row that fits. With more than one impact, the
highest row applies:

| Severity                | Use when |
| ----------------------- | -------- |
| Wrong data or security  | impact **Data integrity**: the driver returns or stores wrong or incomplete data with no error, or reports success for a statement that failed; or impact **Security** with a vulnerability (for a vulnerability in a dependency: only when the driver calls the vulnerable code). Memory corruption in the C API is a vulnerability. A hang, or a crash with no memory corruption, that only a hostile server or network causes does not use this row: rate it by the failure |
| Broken, no workaround   | the feature fails (an error, a hang, a panic, a leak that ends in a crash or in exhausted connections), and the caller has no reasonable workaround; also a driver that stays unusable after an error, and a build that fails on a supported toolchain; also a wrong or incomplete error with no workaround: the failure is reported, but its status, code, or message is wrong or lost. The impact **Stability** (a hang, a panic, such a leak) uses this row when the caller has no reasonable workaround |
| Workaround exists       | the feature fails, but the caller can avoid the failure with a small change in their own code, query, or options (name the workaround in the reason); also impact **Performance** of a defect: a result that is correct but slow or uses too much memory (a request to make an operation faster is a feature request: last row) |
| Cosmetic, docs, feature | wrong log or message text, documentation, a feature request (this includes a method that returns `NotImplemented`, and support for a new type or option); also a request to make an operation faster, and a dependency update (also one for a vulnerability in a dependency, in code that the driver does not call) |

An issue that has the impact Security with no vulnerability (an authentication
method that does not work or is missing) does not use the first row. Rate an
authentication method that does not work by its failure ("Broken, no
workaround" or "Workaround exists"), and a request for a missing method as a
feature request.

## 3. Base level

| Severity ↓ / Reach →    | Core   | Common | Narrow | Satellite |
| ----------------------- | ------ | ------ | ------ | --------- |
| Wrong data or security  | Urgent | Urgent | High   | Medium    |
| Broken, no workaround   | High   | High   | Medium | Low       |
| Workaround exists       | Medium | Medium | Low    | Low       |
| Cosmetic, docs, feature | Low    | Low    | Low    | Low       |

## 4. Modifiers

Apply them in this order:

1. **Security with a shown path from untrusted input.** The issue or a
   reproduction shows how a value that an application takes from its users
   reaches the defect: `Urgent` at any reach. Skip the other modifiers. A
   defect in the identifier escaping of bulk ingest (the table, database, and
   column names of `IngestOptions::to_ingest()`) or in parameter binding
   (`bind_scalar()`) that lets a value change the meaning of the query is a
   candidate: look for such a path. If none is shown, rate the defect by the
   failure that it shows. A vulnerability that is not public yet goes to a
   private vulnerability report, not to a public issue (see "Vulnerabilities
   that are not public yet").
2. **Panic.** The driver panics: a Rust `panic` in driver code or in a crate
   that the driver calls, not in the caller's code and not in `tests/`. The
   level is at least `High`, at any reach. Through the C API, `adbc_ffi`
   catches a panic in an ADBC call and returns `ADBC_STATUS_INTERNAL`, but it
   also marks the driver as poisoned: each later call in the same process fails
   (`catch_panic` and `check_poison` in `rust/ffi/src/driver_exporter.rs` of
   `apache/arrow-adbc`). A panic while the caller reads the result stream (the
   exported `ArrowArrayStream`) is not caught, and it stops the process.
   Through the Rust API, the panic reaches the caller.
3. **Ingestion.** The defect is on the write path (bulk ingest, a streaming
   insert, `src/writer.rs`, or a parameter value that an `INSERT` stores), and
   the driver sends data that is lost,
   corrupted, or different from the given value, with no error. This includes
   a missing column value, and a column that gets a default value in place of
   the given value: up one level. This can give `Urgent`. Wrong data in a table
   stays wrong after the fix; a defect on the read path does not change the
   stored data.
4. **Regression in the latest published release:** up one level. This can give
   `Urgent`. It is a regression only when the latest release shows the problem
   and the release before it does not. The tags are `vX.Y.Z` (for example
   `v0.1.2` and `v0.1.1`). A defect that only `main` has (after the latest
   tag) is not a release regression: no modifier, but write "unreleased
   regression" in the reason, so that it is fixed before the next release.
5. **Customer request in Linear.** The issue links (`linear.app`) a Linear
   issue for a customer request: up one level, but never to `Urgent` (`High`
   stays `High`).

A defect on the read path (the result of a query, `src/reader.rs`,
`get_table_schema()`) that does not panic gets neither of rules 2 and 3. So at
the same reach and severity, a read defect ranks below a panic and below an
ingestion defect.

## 5. Confidence

A missing reproduction lowers the confidence, not the level. Record the
confidence with the value when the tool that sets it supports it:

| Confidence | Use when |
| ---------- | -------- |
| `HIGH`     | a run shows the problem (or, for a feature, code inspection confirms its absence on `main`) |
| `MEDIUM`   | code inspection shows a defect, but no reproduction was run (for example, it needs ClickHouse Cloud or dbt-fusion) |
| `LOW`      | the report lacks what a reproduction needs, or a run did not show the problem |

## 6. Reason

Write one line with the value:

`<tier> (<path>) · <impacts, or "no impact"> · <severity> · <modifiers, or "no modifier"> → <level>.`

Name the workaround when the severity is "Workaround exists". When the
confidence is `LOW`, say what is missing. When the defect is in a crate of
clickhouse-rs, name the crate and the driver path (for example
`Core (clickhouse-ext-arrow ArrowStream cursor, Statement::execute())`).

## 7. Defects in the crates of clickhouse-rs

The fix of a defect in `clickhouse`, `clickhouse-types`, or
`clickhouse-ext-arrow` goes into `ClickHouse/clickhouse-rs`. After a release of
that crate:

- A Rust application gets the fix with `cargo update` when the fixed version
  matches the version requirement in `Cargo.toml`.
- An application that uses a driver binary gets the fix only after this
  repository updates `Cargo.lock` (and `Cargo.toml`, when the requirement does
  not match the fixed version), a release of the driver, and new prebuilt
  binaries. dbt-fusion also pins its own driver version.

Link the clickhouse-rs issue or pull request from the issue here. The priority
stays as the tables above give it.
