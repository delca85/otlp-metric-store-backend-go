package main

// createMetadataTableSQL defines the shared metadata lookup table.
// Engine: ReplacingMergeTree — on merge, keeps the latest row per (TimeUnix, MetricName, MetricHash).
// Queries against this table should use FINAL to force merge completion before reading.
const createMetadataTableSQL = `
CREATE TABLE IF NOT EXISTS otel_metrics_metadata (
    TimeUnix            Date CODEC(Delta, LZ4),
    MetricHash          UInt64,
    MetricName          LowCardinality(String),
    MetricDescription   String,
    MetricUnit          String,
    MetricType          Enum8('Gauge'=1,'Sum'=2,'Histogram'=3,'ExpHistogram'=4,'Summary'=5),
    ServiceName         LowCardinality(String),
    ResourceAttributes  Map(LowCardinality(String), String),
    ScopeAttributes     Map(LowCardinality(String), String),
    Attributes          Map(LowCardinality(String), String),
    ScopeName           String,
    ScopeVersion        String
) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(TimeUnix)
ORDER BY (TimeUnix, MetricName, MetricHash)
SETTINGS index_granularity = 8192;
`

// createGaugeTableSQL defines slim gauge datapoints referencing the metadata table via MetricHash.
const createGaugeTableSQL = `
CREATE TABLE IF NOT EXISTS otel_metrics_gauge (
    MetricHash     UInt64,
    StartTimeUnix  DateTime64(9) CODEC(Delta(8), LZ4),
    TimeUnix       DateTime64(9) CODEC(Delta(8), LZ4),
    Value          Float64 CODEC(LZ4),
    Flags          UInt32
) ENGINE = MergeTree()
PARTITION BY toDate(TimeUnix)
ORDER BY (MetricHash, toUnixTimestamp64Nano(TimeUnix))
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1;
`

// createSumTableSQL defines slim sum datapoints. Extends gauge fields with sum-specific columns.
const createSumTableSQL = `
CREATE TABLE IF NOT EXISTS otel_metrics_sum (
    MetricHash              UInt64,
    StartTimeUnix           DateTime64(9) CODEC(Delta(8), LZ4),
    TimeUnix                DateTime64(9) CODEC(Delta(8), LZ4),
    Value                   Float64 CODEC(LZ4),
    Flags                   UInt32,
    AggregationTemporality  Int32,
    IsMonotonic             Bool
) ENGINE = MergeTree()
PARTITION BY toDate(TimeUnix)
ORDER BY (MetricHash, toUnixTimestamp64Nano(TimeUnix))
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1;
`

// createHistogramTableSQL defines slim histogram datapoints. Insert logic not yet implemented.
const createHistogramTableSQL = `
CREATE TABLE IF NOT EXISTS otel_metrics_histogram (
    MetricHash              UInt64,
    StartTimeUnix           DateTime64(9) CODEC(Delta(8), LZ4),
    TimeUnix                DateTime64(9) CODEC(Delta(8), LZ4),
    Count                   UInt64 CODEC(Delta(8), LZ4),
    Sum                     Float64 CODEC(LZ4),
    BucketCounts            Array(UInt64) CODEC(LZ4),
    ExplicitBounds          Array(Float64) CODEC(LZ4),
    Min                     Float64 CODEC(LZ4),
    Max                     Float64 CODEC(LZ4),
    Flags                   UInt32,
    AggregationTemporality  Int32
) ENGINE = MergeTree()
PARTITION BY toDate(TimeUnix)
ORDER BY (MetricHash, toUnixTimestamp64Nano(TimeUnix))
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1;
`

// createExponentialHistogramTableSQL defines slim exp-histogram datapoints. Insert logic not yet implemented.
const createExponentialHistogramTableSQL = `
CREATE TABLE IF NOT EXISTS otel_metrics_exponential_histogram (
    MetricHash              UInt64,
    StartTimeUnix           DateTime64(9) CODEC(Delta(8), LZ4),
    TimeUnix                DateTime64(9) CODEC(Delta(8), LZ4),
    Count                   UInt64 CODEC(Delta(8), LZ4),
    Sum                     Float64 CODEC(LZ4),
    Scale                   Int32,
    ZeroCount               UInt64,
    PositiveOffset          Int32,
    PositiveBucketCounts    Array(UInt64) CODEC(LZ4),
    NegativeOffset          Int32,
    NegativeBucketCounts    Array(UInt64) CODEC(LZ4),
    Min                     Float64 CODEC(LZ4),
    Max                     Float64 CODEC(LZ4),
    Flags                   UInt32,
    AggregationTemporality  Int32
) ENGINE = MergeTree()
PARTITION BY toDate(TimeUnix)
ORDER BY (MetricHash, toUnixTimestamp64Nano(TimeUnix))
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1;
`

// createSummaryTableSQL defines slim summary datapoints. Insert logic not yet implemented.
const createSummaryTableSQL = `
CREATE TABLE IF NOT EXISTS otel_metrics_summary (
    MetricHash     UInt64,
    StartTimeUnix  DateTime64(9) CODEC(Delta(8), LZ4),
    TimeUnix       DateTime64(9) CODEC(Delta(8), LZ4),
    Count          UInt64 CODEC(Delta(8), LZ4),
    Sum            Float64 CODEC(LZ4),
    ValueAtQuantiles Nested(
        Quantile Float64,
        Value    Float64
    ) CODEC(LZ4),
    Flags          UInt32
) ENGINE = MergeTree()
PARTITION BY toDate(TimeUnix)
ORDER BY (MetricHash, toUnixTimestamp64Nano(TimeUnix))
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1;
`
