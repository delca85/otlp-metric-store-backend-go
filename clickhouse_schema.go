package main

import "fmt"

// createMetadataTableSQL defines the shared metadata lookup table.
// Engine: AggregatingMergeTree — on merge, aggregates columns per (TimeUnix, MetricName, MetricHash).
// Mutable fields (description, unit, type, attributes) use SimpleAggregateFunction(anyLast, T) to
// keep the most recent value independently per column on merge. Immutable identity fields
// (MetricName, ServiceName) that are constant for a given MetricHash use plain types.
// Note: Map key type falls back to plain String inside SimpleAggregateFunction — LowCardinality
// is not supported as a Map key type within SAF wrappers.
// Queries against this table should use FINAL to force merge completion before reading.
const createMetadataTableTemplate = `
CREATE TABLE IF NOT EXISTS otel_metrics_metadata (
    TimeUnix            Date CODEC(Delta, LZ4),
    MetricHash          UInt64,
    MetricName          LowCardinality(String),
    MetricDescription   SimpleAggregateFunction(anyLast, String),
    MetricUnit          SimpleAggregateFunction(anyLast, String),
    MetricType          SimpleAggregateFunction(anyLast, Enum8('Gauge'=1,'Sum'=2,'Histogram'=3,'ExpHistogram'=4,'Summary'=5)),
    ServiceName         LowCardinality(String),
    ResourceAttributes  SimpleAggregateFunction(anyLast, Map(String, String)),
    ScopeAttributes     SimpleAggregateFunction(anyLast, Map(String, String)),
    Attributes          SimpleAggregateFunction(anyLast, Map(String, String)),
    ScopeName           SimpleAggregateFunction(anyLast, String),
    ScopeVersion        SimpleAggregateFunction(anyLast, String)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(TimeUnix)
ORDER BY (TimeUnix, MetricName, MetricHash)
TTL TimeUnix + INTERVAL %d DAY
SETTINGS index_granularity = 8192;
`

// createGaugeTableTemplate defines slim gauge datapoints referencing the metadata table via MetricHash.
// ORDER BY design contract: (TimeUnix, MetricHash) places TimeUnix first so that any time-range
// query — the only mandatory filter per requirements — uses the primary index efficiently without
// requiring a MetricHash pre-filter. Daily partitioning on TimeUnix further bounds scans to
// relevant partitions.
//
// When a MetricHash filter is also present (e.g. after a metadata lookup), ClickHouse uses the
// time range from the primary index to limit the scan and then applies MetricHash as a secondary
// filter within those granules — still efficient for low-cardinality metrics.
const createGaugeTableTemplate = `
CREATE TABLE IF NOT EXISTS otel_metrics_gauge (
    MetricHash     UInt64,
    StartTimeUnix  DateTime64(9) CODEC(Delta(8), LZ4),
    TimeUnix       DateTime64(9) CODEC(Delta(8), LZ4),
    Value          Float64 CODEC(LZ4),
    Flags          UInt32
) ENGINE = MergeTree()
PARTITION BY toDate(TimeUnix)
ORDER BY (toUnixTimestamp64Nano(TimeUnix), MetricHash)
TTL toDateTime(TimeUnix) + INTERVAL %d DAY
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1;
`

// createSumTableTemplate defines slim sum datapoints. Extends gauge fields with sum-specific columns.
const createSumTableTemplate = `
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
ORDER BY (toUnixTimestamp64Nano(TimeUnix), MetricHash)
TTL toDateTime(TimeUnix) + INTERVAL %d DAY
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1;
`

// createHistogramTableTemplate defines slim histogram datapoints. Insert logic not yet implemented.
const createHistogramTableTemplate = `
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
ORDER BY (toUnixTimestamp64Nano(TimeUnix), MetricHash)
TTL toDateTime(TimeUnix) + INTERVAL %d DAY
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1;
`

// createExponentialHistogramTableTemplate defines slim exp-histogram datapoints. Insert logic not yet implemented.
const createExponentialHistogramTableTemplate = `
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
ORDER BY (toUnixTimestamp64Nano(TimeUnix), MetricHash)
TTL toDateTime(TimeUnix) + INTERVAL %d DAY
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1;
`

// createSummaryTableTemplate defines slim summary datapoints. Insert logic not yet implemented.
const createSummaryTableTemplate = `
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
ORDER BY (toUnixTimestamp64Nano(TimeUnix), MetricHash)
TTL toDateTime(TimeUnix) + INTERVAL %d DAY
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1;
`

// ddlStatements returns all CREATE TABLE statements formatted with the given retention period.
func ddlStatements(retentionDays int) []string {
	return []string{
		fmt.Sprintf(createMetadataTableTemplate, retentionDays),
		fmt.Sprintf(createGaugeTableTemplate, retentionDays),
		fmt.Sprintf(createSumTableTemplate, retentionDays),
		fmt.Sprintf(createHistogramTableTemplate, retentionDays),
		fmt.Sprintf(createExponentialHistogramTableTemplate, retentionDays),
		fmt.Sprintf(createSummaryTableTemplate, retentionDays),
	}
}
