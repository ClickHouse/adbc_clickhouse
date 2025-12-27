use adbc_core::options::OptionDatabase;
use adbc_core::{Connection, Database, Driver, Optionable, Statement};

#[test]
fn test_get_table_schema() {
    let mut driver = crate::test_driver();

    let mut database = driver.new_database().unwrap();

    database
        .set_option(OptionDatabase::Uri, "http://localhost:8123/".into())
        .unwrap();

    let mut conn = database.new_connection().unwrap();

    let mut create_table = conn.new_statement().unwrap();
    create_table
        .set_sql_query(
            "
            CREATE OR REPLACE TEMPORARY TABLE test_get_table_schema
            (
                int8                 Int8,
                int16                Int16,
                int32                Int32,
                int64                Int64,
                int128               Int128,

                uint8                UInt8,
                uint16               UInt16,
                uint32               UInt32,
                uint64               UInt64,
                uint128              UInt128,

                int256               Int256,
                uint256              UInt256,

                float32              Float32,
                float64              Float64,
                boolean              Boolean,
                str                  String,
                blob_str             String,
                nullable_str         Nullable(String),
                low_car_str          LowCardinality(String),
                nullable_low_car_str LowCardinality(Nullable(String)),
                fixed_str            FixedString(16),
                -- UUID not supported
                -- uuid                 UUID,
                ipv4                 IPv4,
                ipv6                 IPv6,
                enum8                Enum8('Foo', 'Bar'),
                enum16               Enum16('Qaz' = 42, 'Qux' = 255),
                decimal32_9_4        Decimal(9, 4),
                decimal64_18_8       Decimal(18, 8),
                decimal128_38_12     Decimal(38, 12),
                -- decimal256_76_20           Decimal(76, 20),

                time_date              Date,
                time_date32            Date32,
                time_datetime          DateTime,
                time_datetime_tz       DateTime('UTC'),
                time_datetime64_0      DateTime64(0),
                time_datetime64_3      DateTime64(3),
                time_datetime64_6      DateTime64(6),
                time_datetime64_9      DateTime64(9),
                time_datetime64_9_tz   DateTime64(9, 'UTC'),

                chrono_date            Date,
                chrono_date32          Date32,
                chrono_datetime        DateTime,
                chrono_datetime_tz     DateTime('UTC'),
                chrono_datetime64_0    DateTime64(0),
                chrono_datetime64_3    DateTime64(3),
                chrono_datetime64_6    DateTime64(6),
                chrono_datetime64_9    DateTime64(9),
                chrono_datetime64_9_tz DateTime64(9, 'UTC'),
            ) ENGINE MergeTree ORDER BY ();
        ",
        )
        .unwrap();

    let _ = create_table.execute_update().unwrap();

    let schema = conn
        .get_table_schema(None, None, "test_get_table_schema")
        .unwrap();

    assert_eq!(
        // Asserting the debug format is potentially kind of fragile but
        // building the whole `Schema` structure was going to take a *lot* more code.
        format!("{schema:#?}"),
        "\
Schema {
    fields: [
        Field {
            name: \"int8\",
            data_type: Int8,
            metadata: {
                \"CLICKHOUSE:type\": \"Int8\",
            },
        },
        Field {
            name: \"int16\",
            data_type: Int16,
            metadata: {
                \"CLICKHOUSE:type\": \"Int16\",
            },
        },
        Field {
            name: \"int32\",
            data_type: Int32,
            metadata: {
                \"CLICKHOUSE:type\": \"Int32\",
            },
        },
        Field {
            name: \"int64\",
            data_type: Int64,
            metadata: {
                \"CLICKHOUSE:type\": \"Int64\",
            },
        },
        Field {
            name: \"int128\",
            data_type: FixedSizeBinary(
                32,
            ),
            metadata: {
                \"CLICKHOUSE:type\": \"Int128\",
            },
        },
        Field {
            name: \"uint8\",
            data_type: UInt8,
            metadata: {
                \"CLICKHOUSE:type\": \"UInt8\",
            },
        },
        Field {
            name: \"uint16\",
            data_type: UInt16,
            metadata: {
                \"CLICKHOUSE:type\": \"UInt16\",
            },
        },
        Field {
            name: \"uint32\",
            data_type: UInt32,
            metadata: {
                \"CLICKHOUSE:type\": \"UInt32\",
            },
        },
        Field {
            name: \"uint64\",
            data_type: UInt64,
            metadata: {
                \"CLICKHOUSE:type\": \"UInt64\",
            },
        },
        Field {
            name: \"uint128\",
            data_type: FixedSizeBinary(
                32,
            ),
            metadata: {
                \"CLICKHOUSE:type\": \"UInt128\",
            },
        },
        Field {
            name: \"int256\",
            data_type: FixedSizeBinary(
                64,
            ),
            metadata: {
                \"CLICKHOUSE:type\": \"Int256\",
            },
        },
        Field {
            name: \"uint256\",
            data_type: FixedSizeBinary(
                64,
            ),
            metadata: {
                \"CLICKHOUSE:type\": \"UInt256\",
            },
        },
        Field {
            name: \"float32\",
            data_type: Float32,
            metadata: {
                \"CLICKHOUSE:type\": \"Float32\",
            },
        },
        Field {
            name: \"float64\",
            data_type: Float64,
            metadata: {
                \"CLICKHOUSE:type\": \"Float64\",
            },
        },
        Field {
            name: \"boolean\",
            data_type: Boolean,
            metadata: {
                \"CLICKHOUSE:type\": \"Bool\",
            },
        },
        Field {
            name: \"str\",
            data_type: Utf8,
            metadata: {
                \"CLICKHOUSE:type\": \"String\",
            },
        },
        Field {
            name: \"blob_str\",
            data_type: Utf8,
            metadata: {
                \"CLICKHOUSE:type\": \"String\",
            },
        },
        Field {
            name: \"nullable_str\",
            data_type: Utf8,
            nullable: true,
            metadata: {
                \"CLICKHOUSE:type\": \"Nullable(String)\",
            },
        },
        Field {
            name: \"low_car_str\",
            data_type: Utf8,
            metadata: {
                \"CLICKHOUSE:type\": \"LowCardinality(String)\",
            },
        },
        Field {
            name: \"nullable_low_car_str\",
            data_type: Utf8,
            metadata: {
                \"CLICKHOUSE:type\": \"LowCardinality(Nullable(String))\",
            },
        },
        Field {
            name: \"fixed_str\",
            data_type: FixedSizeBinary(
                16,
            ),
            metadata: {
                \"CLICKHOUSE:type\": \"FixedString(16)\",
            },
        },
        Field {
            name: \"ipv4\",
            data_type: UInt32,
            metadata: {
                \"CLICKHOUSE:type\": \"IPv4\",
            },
        },
        Field {
            name: \"ipv6\",
            data_type: FixedSizeBinary(
                16,
            ),
            metadata: {
                \"CLICKHOUSE:type\": \"IPv6\",
            },
        },
        Field {
            name: \"enum8\",
            data_type: Int8,
            metadata: {
                \"CLICKHOUSE:type\": \"Enum8('Foo' = 1, 'Bar' = 2)\",
            },
        },
        Field {
            name: \"enum16\",
            data_type: Int16,
            metadata: {
                \"CLICKHOUSE:type\": \"Enum16('Qaz' = 42, 'Qux' = 255)\",
            },
        },
        Field {
            name: \"decimal32_9_4\",
            data_type: Decimal32(
                9,
                4,
            ),
            metadata: {
                \"CLICKHOUSE:type\": \"Decimal(9, 4)\",
            },
        },
        Field {
            name: \"decimal64_18_8\",
            data_type: Decimal64(
                18,
                8,
            ),
            metadata: {
                \"CLICKHOUSE:type\": \"Decimal(18, 8)\",
            },
        },
        Field {
            name: \"decimal128_38_12\",
            data_type: Decimal128(
                38,
                12,
            ),
            metadata: {
                \"CLICKHOUSE:type\": \"Decimal(38, 12)\",
            },
        },
        Field {
            name: \"time_date\",
            data_type: UInt16,
            metadata: {
                \"CLICKHOUSE:type\": \"Date\",
            },
        },
        Field {
            name: \"time_date32\",
            data_type: Date32,
            metadata: {
                \"CLICKHOUSE:type\": \"Date32\",
            },
        },
        Field {
            name: \"time_datetime\",
            data_type: UInt32,
            metadata: {
                \"CLICKHOUSE:type\": \"DateTime\",
            },
        },
        Field {
            name: \"time_datetime_tz\",
            data_type: UInt32,
            metadata: {
                \"CLICKHOUSE:type\": \"DateTime('UTC')\",
            },
        },
        Field {
            name: \"time_datetime64_0\",
            data_type: Timestamp(
                Second,
                None,
            ),
            metadata: {
                \"CLICKHOUSE:type\": \"DateTime64(0)\",
            },
        },
        Field {
            name: \"time_datetime64_3\",
            data_type: Timestamp(
                Millisecond,
                None,
            ),
            metadata: {
                \"CLICKHOUSE:type\": \"DateTime64(3)\",
            },
        },
        Field {
            name: \"time_datetime64_6\",
            data_type: Timestamp(
                Microsecond,
                None,
            ),
            metadata: {
                \"CLICKHOUSE:type\": \"DateTime64(6)\",
            },
        },
        Field {
            name: \"time_datetime64_9\",
            data_type: Timestamp(
                Nanosecond,
                None,
            ),
            metadata: {
                \"CLICKHOUSE:type\": \"DateTime64(9)\",
            },
        },
        Field {
            name: \"time_datetime64_9_tz\",
            data_type: Timestamp(
                Nanosecond,
                Some(
                    \"UTC\",
                ),
            ),
            metadata: {
                \"CLICKHOUSE:type\": \"DateTime64(9, 'UTC')\",
            },
        },
        Field {
            name: \"chrono_date\",
            data_type: UInt16,
            metadata: {
                \"CLICKHOUSE:type\": \"Date\",
            },
        },
        Field {
            name: \"chrono_date32\",
            data_type: Date32,
            metadata: {
                \"CLICKHOUSE:type\": \"Date32\",
            },
        },
        Field {
            name: \"chrono_datetime\",
            data_type: UInt32,
            metadata: {
                \"CLICKHOUSE:type\": \"DateTime\",
            },
        },
        Field {
            name: \"chrono_datetime_tz\",
            data_type: UInt32,
            metadata: {
                \"CLICKHOUSE:type\": \"DateTime('UTC')\",
            },
        },
        Field {
            name: \"chrono_datetime64_0\",
            data_type: Timestamp(
                Second,
                None,
            ),
            metadata: {
                \"CLICKHOUSE:type\": \"DateTime64(0)\",
            },
        },
        Field {
            name: \"chrono_datetime64_3\",
            data_type: Timestamp(
                Millisecond,
                None,
            ),
            metadata: {
                \"CLICKHOUSE:type\": \"DateTime64(3)\",
            },
        },
        Field {
            name: \"chrono_datetime64_6\",
            data_type: Timestamp(
                Microsecond,
                None,
            ),
            metadata: {
                \"CLICKHOUSE:type\": \"DateTime64(6)\",
            },
        },
        Field {
            name: \"chrono_datetime64_9\",
            data_type: Timestamp(
                Nanosecond,
                None,
            ),
            metadata: {
                \"CLICKHOUSE:type\": \"DateTime64(9)\",
            },
        },
        Field {
            name: \"chrono_datetime64_9_tz\",
            data_type: Timestamp(
                Nanosecond,
                Some(
                    \"UTC\",
                ),
            ),
            metadata: {
                \"CLICKHOUSE:type\": \"DateTime64(9, 'UTC')\",
            },
        },
    ],
    metadata: {},
}"
    );
}
