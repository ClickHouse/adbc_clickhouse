# Agent context

A primer for AI agents working on this repository. Read this before doing substantive work. **Update this file when a conversation surfaces a new architectural fact, library quirk, repo convention, or validation source worth preserving for future chats.**

## What this project is

- ADBC driver for ClickHouse, written in Rust. Work-in-progress (`0.1.0-alpha.*`); many ADBC methods are stubbed with `NotImplemented`.
- Built on the official ClickHouse Rust client (`clickhouse-rs`).
- Talks to ClickHouse via the HTTP interface only.

## Local development

- Integration tests require a running ClickHouse at `http://localhost:8123/`. Start with `docker compose up -d clickhouse`. Sanity check: `curl -s http://localhost:8123/ping` → `Ok.`
- Test commands:
  - All: `cargo test`
  - Integration only: `cargo test --test it`
  - Single test: `cargo test --test it <name>`
- Optional env flags for the integration suite: `ADBC_CLICKHOUSE_TEST_MULTI_THREAD=1`, `ADBC_CLICKHOUSE_TEST_LOAD_DYNAMIC=1` (with `--features ffi`).

## How the driver paths fit together

- `Statement::execute()` → `fetch_blocking()` → `clickhouse::Query::fetch_bytes("ArrowStream")` → `ArrowStreamReader::begin()`, which reads the Arrow IPC stream.
- `Statement::execute_update()` → `clickhouse::Query::execute()` (no format, no result reader). Sets `wait_end_of_query=1` for stronger commit semantics.
- **ADBC clients (notably the Python DB-API `Cursor.execute()`) route every statement through `execute_query` → `Statement::execute()`.** `execute_update()` is reached only by `executemany`/`adbc_ingest`/`executescript`. So correctness for DDL/DML must live on the `execute()` path, not behind client-side SQL classification.

- **`clickhouse.setting.<name>` options** map to `Client::set_setting()` (sent as URL query params on every request). `ClickhouseDatabase` holds no `Client`, so its settings are deferred state replayed in `new_connection_with_opts` *before* the per-connection opts loop — that ordering is what makes lower levels override. Prefix keys use a `strip_prefix` early return, not a match arm: if-let match guards aren't stable on the pinned 1.91.0 toolchain.

## Underlying-library behaviors that matter

- **`clickhouse-rs` error filtering:** `src/response.rs` translates HTTP non-200 status codes and the `X-ClickHouse-Exception-Code` header into `Error::BadResponse` *before* returning a `BytesCursor`. By the time a cursor exists, the response is a successful HTTP 200. An empty body from the cursor means "successful request, no payload" — exactly what ClickHouse returns for DDL/DML under `FORMAT ArrowStream`.
- **`BytesCursor::next()` is fused at EOF.** Once it returns `Ok(None)`, future calls keep returning `Ok(None)` (`src/cursors/raw.rs` comment: *"provide proper fused behavior of the cursor"*). No need for a separate "done" guard in this driver.
- **`arrow-ipc::StreamDecoder::finish()` only inspects state, doesn't mutate it.** Safe to call multiple times. It also catches every partial-data / truncated-stream scenario with `IpcError("Unexpected End of Stream")`, so this driver doesn't need to re-validate that.
- **`SchemaRef = Arc<Schema>`.** Existing codebase convention is `Schema::new(...).into()` / `Schema::empty().into()`, *not* `Arc::new(Schema::...)`. The `Arc::new` form is only used here for `Runtime` and `Client`.

## Related local repos worth referencing

- `/Users/josemunoz/repos/clickhouse-rs` — the underlying ClickHouse client. Useful files: `src/response.rs` (HTTP error filtering), `src/cursors/*` (fused stream behavior), `src/query.rs` (`fetch_bytes` / `execute`).
- `/Users/josemunoz/repos/dbt-fusion` — downstream consumer. `crates/dbt-adapter/src/engine/adapter_engine.rs` historically held ClickHouse-specific workarounds for driver bugs (e.g. the `is_update_statement` branch for issue #49). Check there when reasoning about real-world driver consumption.

## Coding norms observed in this repo

- **Default to no comments.** Add one only when the WHY is non-obvious (an upstream quirk, a surprising behavior, an issue link). Don't restate what the code does.
- **Don't add defensive code for invariants the underlying library already provides.** Example: no `done: bool` flag was needed for iterator idempotency at EOF because both `BytesCursor` and `StreamDecoder::finish()` are already idempotent.
- **Trust framework guarantees.** No validation at internal boundaries; only at system boundaries (user input, external APIs).
- **Minimal scope.** Don't refactor adjacent code, add helpers, or design for hypothetical futures while fixing a bug.
- **Tests:** use the shared `test_database()` helper in `tests/it/main.rs` for the driver/db boilerplate. Follow the existing per-test layout (statement → set_sql_query → execute / execute_update → assertions).

## Validation discipline

When making claims about library behavior:
- Read source, don't infer from prose documentation.
- For ADBC semantic questions, the authoritative sources are `apache/arrow-adbc` `c/include/arrow-adbc/adbc.h` docstrings and `python/adbc_driver_manager/adbc_driver_manager/dbapi.py` (how the driver manager actually calls things). The spec pages are often less specific than the headers.
- For ClickHouse server behavior under `FORMAT ArrowStream`, the source of truth is `clickhouse-rs` (transport) plus the official ClickHouse HTTP-interface docs.
- When applicable, cite the exact file + line.

## Issue tracker

- `ClickHouse/adbc_clickhouse` on GitHub. Use the `gh` CLI: `gh issue view <n>`, `gh api repos/ClickHouse/adbc_clickhouse/issues/comments/<id>`.

## Recent context

- Issue #49: `Statement::execute()` used to error on DDL/DML (`Schema error: response stream ended before receiving Schema`) because ClickHouse omits the Arrow IPC schema header for no-result statements. Fixed by returning an empty `RecordBatchReader` (`Schema::empty().into()`, no batches) from `ArrowStreamReader::begin()` instead of erroring. See `description.md` in the repo root for the PR description draft.
- "All result columns nullable" report (draft issue, Aug 2026): **does not reproduce at the driver level.** The server's `ArrowStream` bytes carry correct per-field nullable flags (verified against CH 26.7.1.1315 by round-tripping through `clickhouse local` + `file(..., ArrowStream)`), and the driver takes the result schema verbatim from the IPC bytes, so the flags propagate. Verified correct on: the pure-Rust API, static FFI via `adbc_driver_manager`, and the released dylibs `0.1.0-alpha.1`/`0.1.0-alpha.2`/`0.1.0` loaded dynamically — for expression columns, table columns, and zero-row results. Repro tests: `tests/it/nullability.rs` (dylib test gated on `ADBC_CLICKHOUSE_TEST_DYLIB=/path/to/dylib`). The flattening most likely happens downstream in dbt-fusion, not here.

## Downstream: how dbt-fusion loads this driver

- dbt-fusion does *not* link this crate; it downloads prebuilt dylibs from `public.cdn.getdbt.com/fs/adbc/clickhouse/...` (see `crates/dbt-xdbc/src/install.rs::format_driver_url`) and caches them at `~/Library/Caches/com.getdbt/adbc/<triplet>/libadbc_driver_clickhouse-<version>.dylib` on macOS.
- The pinned version is `CLICKHOUSE_DRIVER_VERSION` in `crates/dbt-xdbc/src/lib.rs` (currently `0.1.0-alpha.2`), even when users report "0.1.0". Checksums for known versions live in `crates/dbt-xdbc/src/checksums.rs`.
- Released dylibs can be tested directly with `adbc_driver_manager::ManagedDriver::load_dynamic_from_filename` (a dev-dependency here).

---

**Reminder:** if a future conversation surfaces another non-obvious fact about ADBC semantics, clickhouse-rs internals, arrow-ipc behavior, this repo's conventions, or downstream consumer expectations — add it here. Keep entries terse and pointer-to-source where possible.
