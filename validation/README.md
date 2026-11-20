# ADBC validation suite

Runs the [ADBC Driver Foundry validation suite](https://github.com/adbc-drivers/validation)
against this driver. The suite loads the driver as a shared library through the ADBC C API,
so it also tests the FFI layer.

## Running

Requires [uv](https://docs.astral.sh/uv/) and a ClickHouse server (`docker compose up -d clickhouse`
from the repository root).

```sh
cargo build --features ffi
cd validation
uv run pytest tests/
```

Run a subset with `-k`, e.g. `uv run pytest tests/ -k type/bind/int32`.

| Environment variable          | Default                                     |
|-------------------------------|---------------------------------------------|
| `ADBC_CLICKHOUSE_TEST_URI`    | `http://localhost:8123/`                    |
| `ADBC_CLICKHOUSE_DRIVER_PATH` | `../target/debug/libadbc_clickhouse.{so,dylib}` |

## Layout

- `tests/clickhouse.py`: the driver's quirks, i.e. which ADBC features it supports.
  A feature turned off here skips its tests.
- `tests/test_*.py`: subclasses of the suite's test classes, with `xfail` marks for known gaps.
- `queries/`: ClickHouse versions of the suite's query cases. A `.txtcase` file here replaces
  only the parts it contains (e.g. just `bind_query`) of the case with the same path in the suite.
  `[tags] broken-driver = "..."` or `broken-vendor = "..."` in a case's metadata marks it `xfail`.

`xfail` is strict: a test that starts to pass fails the run, so a change that fixes a gap must
also remove its mark.

The suite is pinned to a commit in `pyproject.toml`. To update it, change the commit,
run `uv lock`, and fix any new failures in the same change.
