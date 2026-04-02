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
(AggregatingMerge) otel_metrics_histogram …
```

### Schema design

Metric identity (name, service, attributes) is extracted once into `otel_metrics_metadata` (`AggregatingMergeTree`), which merges duplicate rows per sort key on compaction, applying `anyLast` semantics to mutable fields. Datapoint tables (`MergeTree`) store only the numeric value, timestamps, and a `MetricHash` — a deterministic `UInt64` xxHash64 of the identity fields computed in Go at ingest time.

The metadata table's `TimeUnix` column is a `Date` (day granularity), not `DateTime64`. This is intentional: the `AggregatingMergeTree` dedup unit is one metadata row per series per day, matching the `ORDER BY (TimeUnix, MetricName, MetricHash)` key. Datapoint tables use `DateTime64(9)` for full nanosecond precision.

This eliminates per-datapoint attribute storage while keeping JOINs fast: metadata cardinality is expected to be low and the table fits in memory for broadcast joins.

**Hashing key:** `MetricName \x00 ServiceName \x00 sorted(ResourceAttributes) \x00 sorted(ScopeAttributes) \x00 sorted(Attributes)`

Fields are separated by `\x00`, map entries by `\x01`, and key-value pairs by `\x02` — ASCII control characters that cannot appear in valid UTF-8 OTLP strings, making the encoding unambiguous regardless of field content.

**Tables created on startup:**

| Table | Engine | Partition | Purpose |
|---|---|---|---|
| `otel_metrics_metadata` | AggregatingMergeTree | monthly (`toYYYYMM(TimeUnix)`) · `ORDER BY (TimeUnix, MetricName, MetricHash)` | Metric identity + attributes lookup |
| `otel_metrics_gauge` | MergeTree | daily (`toDate(TimeUnix)`) | Gauge datapoints |
| `otel_metrics_sum` | MergeTree | daily | Sum datapoints |
| `otel_metrics_histogram` | MergeTree | daily | Histogram datapoints (schema only) |
| `otel_metrics_exponential_histogram` | MergeTree | daily | Exp. histogram datapoints (schema only) |
| `otel_metrics_summary` | MergeTree | daily | Summary datapoints (schema only) |

> Insert logic is implemented for **Gauge** and **Sum**. The remaining table schemas are defined and ready to be wired up.

All tables carry a `TTL` expression configured via `-data-retention-days` (default 90 days). Data older than the retention window is dropped automatically by ClickHouse without manual intervention.

### Design decisions

**Normalized schema (metadata + slim datapoint tables)**
Storing metric identity (name, service, attributes) once in a shared lookup table avoids repeating the same strings on every datapoint row. At the expected cardinality, the metadata table fits entirely in memory, making JOIN lookups effectively free.

**Deterministic hash as join key (xxHash64)**
ClickHouse has no transactions and no `RETURNING` clause, so a DB-generated ID would require a `SELECT` per insert to check existence — a bottleneck at high throughput. A hash computed in Go from the sorted identity fields is deterministic across runs: the same metric series always produces the same `MetricHash`, making metadata inserts idempotent with no DB round-trip.

**`AggregatingMergeTree` over `ReplacingMergeTree` for metadata**
Both engines deduplicate rows on compaction. `ReplacingMergeTree` keeps the latest *full row*, which loses independent tracking of mutable fields: if `MetricDescription` or `MetricUnit` changes while the metric identity stays the same, RMT can only overwrite the whole row. `AggregatingMergeTree` with `SimpleAggregateFunction(anyLast, T)` per mutable column merges each field independently, so changes to one field don't silently overwrite others.

**Partition + ordering strategy eliminates full scans**
All queries are guaranteed to include a time-frame filter. Partitioning by time (monthly for metadata, daily for datapoints) combined with a time-leading `ORDER BY` key ensures ClickHouse prunes irrelevant parts before scanning a single row.

The metadata table uses `ORDER BY (TimeUnix, MetricName, MetricHash)` — `MetricName` sits between the time key and the hash so that queries filtering by name after a time range can use the primary index. Datapoint tables use `ORDER BY (toUnixTimestamp64Nano(TimeUnix), MetricHash)` — the explicit `Int64` cast avoids any ambiguity in how ClickHouse sorts `DateTime64(9)` values.

**Server-side async inserts for high throughput**
`async_insert=1` with `wait_for_async_insert=1` lets ClickHouse buffer and merge small per-RPC inserts server-side without application-level batching logic, while still propagating errors back to the caller.

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
./otlp-metrics-processor-backend
```

### CLI flags

| Flag | Default | Description |
|---|---|---|
| `-listenAddr` | `localhost:4317` | gRPC listen address |
| `-maxReceiveMessageSize` | `16777216` (16 MB) | Max gRPC message size in bytes |
| `-shutdown-timeout` | `30s` | Max time to drain in-flight RPCs on shutdown |
| `-tls-cert-file` | *(empty)* | Path to TLS certificate (PEM). Required for TLS. |
| `-tls-key-file` | *(empty)* | Path to TLS private key (PEM). Required for TLS. |
| `-clickhouse-addr` | `localhost:9000` | ClickHouse TCP address |
| `-clickhouse-db` | `default` | ClickHouse database name |
| `-clickhouse-user` | `default` | ClickHouse username |
| `-clickhouse-password` | *(empty)* | ClickHouse password |
| `-clickhouse-max-open-conns` | `10` | Max open ClickHouse connections |
| `-clickhouse-max-idle-conns` | `5` | Max idle ClickHouse connections |
| `-clickhouse-conn-max-lifetime` | `1h` | Max lifetime of a ClickHouse connection |
| `-clickhouse-max-retries` | `3` | Max retry attempts on transient insert errors |
| `-data-retention-days` | `90` | TTL in days for all datapoint and metadata tables |
| `-log-level` | `INFO` | Minimum log level (`DEBUG`, `INFO`, `WARN`, `ERROR`) |

> **TLS:** when both `-tls-cert-file` and `-tls-key-file` are set the server uses TLS; otherwise it starts with insecure transport and logs a warning. In production, always provide a certificate.
>
> **Secrets:** `-clickhouse-password` is visible in `ps aux`. Prefer injecting it via a secrets manager or a wrapper that sets the flag value from an environment variable or file.

**Example:**

```shell
./otlp-metrics-processor-backend \
  -listenAddr 0.0.0.0:4317 \
  -clickhouse-addr clickhouse:9000 \
  -clickhouse-db metrics \
  -clickhouse-user metrics_user \
  -clickhouse-password secret
```

> Tables are created with `CREATE TABLE IF NOT EXISTS` on startup — existing data is preserved across restarts.

---

## Running locally

This section shows how to spin up a real ClickHouse instance and send test metrics end-to-end — useful for manual exploration and schema inspection.

### 1. Start ClickHouse

```shell
docker run --rm -d \
  --name clickhouse-dev \
  -p 9000:9000 \
  -p 8123:8123 \
  -e CLICKHOUSE_USER=default \
  -e CLICKHOUSE_PASSWORD=test \
  clickhouse/clickhouse-server:26.2
```

### 2. Start the server

```shell
go run ./... \
  -clickhouse-addr localhost:9000 \
  -clickhouse-password test
```

The server creates all tables on startup and is ready to accept OTLP exports on `localhost:4317`.

> **Seeing logs locally:** the server writes structured JSON logs to **stderr** directly, so log lines are always visible in the terminal. However, without an OTLP collector the exporter will log connection errors continuously. Suppress them with `OTEL_SDK_DISABLED=true`:
>
> ```shell
> OTEL_SDK_DISABLED=true go run ./... \
>   -clickhouse-addr localhost:9000 \
>   -clickhouse-password test \
>   -log-level DEBUG
> ```
>
> This disables the OTLP export pipeline while leaving the stderr JSON logs fully intact.

### 3. Send a test metric

```shell
go run tools/send-test-metric/main.go
# or point at a non-default address:
go run tools/send-test-metric/main.go -addr localhost:4317
```

This sends one gauge (`cpu.utilization`) and one sum (`requests.total`) from a fictional `my-test-service`.

### 4. Inspect the data

```shell
docker exec -it clickhouse-dev clickhouse-client --password test
```

```sql
-- Metric series registered
SELECT MetricName, ServiceName, MetricType, Attributes
FROM otel_metrics_metadata FINAL;

-- Gauge datapoints
SELECT TimeUnix, MetricName, ServiceName, Value
FROM otel_metrics_gauge AS g
LEFT ANY JOIN otel_metrics_metadata FINAL AS m ON g.MetricHash = m.MetricHash
ORDER BY TimeUnix DESC
LIMIT 20;

-- Sum datapoints
SELECT TimeUnix, MetricName, ServiceName, Value
FROM otel_metrics_sum AS s
LEFT ANY JOIN otel_metrics_metadata FINAL AS m ON s.MetricHash = m.MetricHash
ORDER BY TimeUnix DESC
LIMIT 20;
```

### 5. Stop ClickHouse

```shell
docker stop clickhouse-dev
```

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

The service instruments itself using the OpenTelemetry Go SDK and exports signals via **OTLP gRPC**. Configure the collector endpoint with the standard environment variables:

```shell
export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317   # all signals
# or signal-specific overrides:
export OTEL_EXPORTER_OTLP_TRACES_ENDPOINT=http://...
export OTEL_EXPORTER_OTLP_METRICS_ENDPOINT=http://...
export OTEL_EXPORTER_OTLP_LOGS_ENDPOINT=http://...
```

When `OTEL_EXPORTER_OTLP_ENDPOINT` is not set the exporters attempt `localhost:4317` by default; failed connections are retried silently and do not affect ingestion.

| Signal | What is recorded |
|---|---|
| **Traces** | One span per `Export()` call; child spans per batch insert — includes table name, batch size, error status |
| **Metrics** | `otlp.metrics.received` (counter — datapoints received, not RPC calls), `metric_store.inserts_total` (counter, `{table, status}` labels), `metric_store.insert_duration_ms` (histogram) |
| **Logs** | Structured logs via `log/slog` |

A standard gRPC health check service (`grpc_health_v1`) is registered and can be used for Kubernetes liveness/readiness probes and load-balancer health checks.

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

## Known Limitations

### `DATA_POINT_FLAGS_NO_RECORDED_VALUE` not preserved

The OTLP spec defines flag bit `1` (`DATA_POINT_FLAGS_NO_RECORDED_VALUE_MASK`) to signal that a measurement was attempted but no value was recorded (e.g. a scrape failure or counter reset). When this flag is set, the value field must be ignored.

Currently the flag is stored as-is in the `Flags` column but the `Value` field is stored as a regular float, which is indistinguishable from a real zero measurement. The standard behaviour — used by the OTel Collector and backends like the official ClickHouse exporter — is to store `NULL` for the value, preserving the gap in the time series for downstream tools (Grafana, Prometheus) that distinguish "value was zero" from "measurement was absent."

Fixing this requires changing `Value Float64` → `Value Nullable(Float64)` in `otel_metrics_gauge` and `otel_metrics_sum`, updating the mapper to emit `nil` when the flag is set, and adjusting the ClickHouse insert logic accordingly.

### Unimplemented metric types

Insert logic is currently implemented only for **Gauge** and **Sum** metrics. The following types have table schemas defined but are **not yet handled** — datapoints of these types are silently dropped at ingest:

| Metric type | Table | Status |
|---|---|---|
| Histogram | `otel_metrics_histogram` | Schema only — no insert logic |
| Exponential Histogram | `otel_metrics_exponential_histogram` | Schema only — no insert logic |
| Summary | `otel_metrics_summary` | Schema only — no insert logic |

To add support for a missing type, implement the corresponding mapping in [metrics_mapper.go](metrics_mapper.go) and the batch insert in [clickhouse_store.go](clickhouse_store.go), following the existing Gauge/Sum pattern.

---

## Future Improvements

### Password and secrets via environment variables or files

The `-clickhouse-password` CLI flag is visible in `ps aux` to co-tenant processes. A production deployment should prefer injecting the value via a secrets manager or a `-clickhouse-password-file` flag that reads from a file (compatible with Kubernetes secret volume mounts).

### Application-level write buffer

For sustained very-high-throughput scenarios, an in-process ring buffer (channel + flush goroutine) that accumulates rows across multiple gRPC calls and flushes in large batches on a timer or size threshold would reduce ClickHouse write amplification further. The current `async_insert=1` delegation is simpler and sufficient at moderate rates.

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
- [ClickHouse AggregatingMergeTree](https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/aggregatingmergetree)
