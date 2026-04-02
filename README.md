# OTLP Metric Store — Go

A production-oriented backend service that receives [OpenTelemetry](https://opentelemetry.io/docs/concepts/signals/metrics/) metric datapoints over gRPC and persists them in ClickHouse using a normalized schema with a shared metadata lookup table.

---

## Architecture Overview

```
OTLP sender (e.g. OTel Collector)
        │
        │  gRPC (port 4317)
        ▼
┌─────────────────────────┐
│  MetricsService.Export() │   ← gRPC handler; records OTel traces + counters
└──────────┬──────────────┘
           │
           ▼
┌─────────────────────────┐
│     MetricsMapper        │   ← converts OTLP protos to slim rows + metadata rows
│  (xxHash64 hashing)      │     using a deterministic hash as the join key
└──────────┬──────────────┘
           │
           ▼
┌─────────────────────────┐
│   ClickHouseMetricsStore │   ← batch inserts; metadata is idempotent
└──────────┬──────────────┘
           │
    ┌──────┴──────┐
    ▼             ▼
otel_metrics_      otel_metrics_gauge
metadata           otel_metrics_sum
(ReplacingMerge)   otel_metrics_histogram …
```

### Schema design

Metric identity (name, service, attributes) is extracted once into `otel_metrics_metadata` (`ReplacingMergeTree`), which deduplicates rows automatically on merge. Datapoint tables (`MergeTree`) store only the numeric value, timestamps, and a `MetricHash` — a deterministic `UInt64` xxHash64 of the identity fields computed in Go at ingest time.

This eliminates per-datapoint attribute storage while keeping JOINs fast: metadata cardinality is expected to be low and the table fits in memory for broadcast joins.

**Hashing key:** `MetricName | ServiceName | sorted(ResourceAttributes) | sorted(ScopeAttributes) | sorted(Attributes)`

**Tables created on startup:**

| Table | Engine | Partition | Purpose |
|---|---|---|---|
| `otel_metrics_metadata` | ReplacingMergeTree | monthly (`toYYYYMM(TimeUnix)`) | Metric identity + attributes lookup |
| `otel_metrics_gauge` | MergeTree | daily (`toDate(TimeUnix)`) | Gauge datapoints |
| `otel_metrics_sum` | MergeTree | daily | Sum datapoints |
| `otel_metrics_histogram` | MergeTree | daily | Histogram datapoints (schema only) |
| `otel_metrics_exponential_histogram` | MergeTree | daily | Exp. histogram datapoints (schema only) |
| `otel_metrics_summary` | MergeTree | daily | Summary datapoints (schema only) |

> Insert logic is implemented for **Gauge** and **Sum**. The remaining table schemas are defined and ready to be wired up.

---

## Prerequisites

| Dependency | Version |
|---|---|
| Go | 1.26+ |
| ClickHouse | 26.2+ (TCP native protocol on port `9000`) |
| Docker | Required for integration tests only |

---

## Build

```shell
go build ./...
# or
make build
```

---

## Run

```shell
go run ./...
# or
./otlp-log-processor-backend
```

### CLI flags

| Flag | Default | Description |
|---|---|---|
| `-listenAddr` | `localhost:4317` | gRPC listen address |
| `-maxReceiveMessageSize` | `16777216` (16 MB) | Max gRPC message size in bytes |
| `-clickhouse-addr` | `localhost:9000` | ClickHouse TCP address |
| `-clickhouse-db` | `default` | ClickHouse database name |
| `-clickhouse-user` | `default` | ClickHouse username |
| `-clickhouse-password` | _(empty)_ | ClickHouse password |

**Example:**

```shell
./otlp-log-processor-backend \
  -listenAddr 0.0.0.0:4317 \
  -clickhouse-addr clickhouse:9000 \
  -clickhouse-db metrics \
  -clickhouse-user metrics_user \
  -clickhouse-password secret
```

> Tables are created with `CREATE TABLE IF NOT EXISTS` on startup — existing data is preserved across restarts.

---

## Tests

### Unit tests

No external dependencies required.

```shell
go test ./...
# or
make test
```

### Integration tests

Require Docker. Spins up a `clickhouse/clickhouse-server:26.2` container via [testcontainers-go](https://golang.testcontainers.org/).

```shell
go test -tags integration -count=1 -v ./...
# or
make test-integration
```

### All tests

```shell
make test-all
```

---

## Querying the data

Always query `otel_metrics_metadata` with `FINAL` to force merge completion and avoid duplicates:

```sql
-- All known metric series
SELECT MetricName, ServiceName, MetricType
FROM otel_metrics_metadata FINAL
ORDER BY MetricName;

-- Gauge values for a time range, enriched with metadata
SELECT
    g.TimeUnix,
    m.MetricName,
    m.ServiceName,
    m.Attributes,
    g.Value
FROM otel_metrics_gauge AS g
LEFT ANY JOIN otel_metrics_metadata FINAL AS m ON g.MetricHash = m.MetricHash
WHERE g.TimeUnix BETWEEN '2025-01-01' AND '2025-01-02'
ORDER BY g.TimeUnix;
```

---

## Observability

The service instruments itself using the OpenTelemetry Go SDK and exports to **stdout** by default.

| Signal | What is recorded |
|---|---|
| **Traces** | One span per `Export()` call; child spans per batch insert — includes table name, batch size, error status |
| **Metrics** | `com.dash0.homeexercise.metrics.received` (counter), `metric_store.inserts_total` (counter, `{table, status}` labels), `metric_store.insert_duration_ms` (histogram) |
| **Logs** | Structured logs via `log/slog` |

---

## Development

```shell
make fmt     # go fmt
make vet     # go vet
make lint    # staticcheck (install separately if needed)
make tidy    # go mod tidy
make clean   # go clean
```

---

## Future Improvements

### Secondary skip indexes for `otel_metrics_metadata`

The current schema omits secondary indexes because the assignment guarantees a time-frame filter on every query, making partition pruning + the primary key sufficient. If optional filters are introduced later, the following indexes should be added via `ALTER TABLE otel_metrics_metadata ADD INDEX`:

```sql
-- Filter by service name
INDEX idx_service_name     ServiceName                 TYPE bloom_filter(0.01) GRANULARITY 1,
-- Hash-only join without time context
INDEX idx_metric_hash      MetricHash                  TYPE bloom_filter(0.01) GRANULARITY 1,
-- Attribute key/value filtering
INDEX idx_res_attr_key     mapKeys(ResourceAttributes)   TYPE bloom_filter(0.01) GRANULARITY 1,
INDEX idx_res_attr_value   mapValues(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
INDEX idx_scope_attr_key   mapKeys(ScopeAttributes)      TYPE bloom_filter(0.01) GRANULARITY 1,
INDEX idx_scope_attr_value mapValues(ScopeAttributes)    TYPE bloom_filter(0.01) GRANULARITY 1,
INDEX idx_attr_key         mapKeys(Attributes)           TYPE bloom_filter(0.01) GRANULARITY 1,
INDEX idx_attr_value       mapValues(Attributes)         TYPE bloom_filter(0.01) GRANULARITY 1
```

`MetricName` does not need a secondary index — it is the second `ORDER BY` key and is efficiently prunable given a time filter.

---

## References

- [OpenTelemetry Metrics concepts](https://opentelemetry.io/docs/concepts/signals/metrics/)
- [OpenTelemetry Protocol (OTLP)](https://github.com/open-telemetry/opentelemetry-proto)
- [ClickHouse ReplacingMergeTree](https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/replacingmergetree)
