package main

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// MetadataRow represents a metric series metadata entry for otel_metrics_metadata.
// One row per unique (MetricName, ServiceName, ResourceAttributes, ScopeAttributes, Attributes)
// combination per day. The ReplacingMergeTree engine keeps the latest row per
// (TimeUnix, MetricName, MetricHash) key on merge.
type MetadataRow struct {
	TimeUnix           time.Time         // Date of the datapoint (toDate granularity); first ORDER BY key
	MetricHash         uint64            // xxHash64 of identity fields; join key to datapoint tables
	MetricName         string
	MetricDescription  string
	MetricUnit         string
	MetricType         string            // "Gauge", "Sum", "Histogram", "ExpHistogram", "Summary"
	ServiceName        string
	ResourceAttributes map[string]string
	ScopeAttributes    map[string]string
	Attributes         map[string]string // Datapoint-level labels
	ScopeName          string
	ScopeVersion       string
}

// GaugeRow represents a single gauge datapoint for otel_metrics_gauge.
type GaugeRow struct {
	MetricHash    uint64
	StartTimeUnix time.Time
	TimeUnix      time.Time
	Value         float64
	Flags         uint32
}

// SumRow represents a single sum datapoint for otel_metrics_sum.
type SumRow struct {
	GaugeRow
	AggregationTemporality int32
	IsMonotonic            bool
}

// MetricsStore defines the interface for storing metrics in ClickHouse.
type MetricsStore interface {
	CreateTables(ctx context.Context) error
	InsertGauge(ctx context.Context, rows []GaugeRow, metadata []MetadataRow) error
	InsertSum(ctx context.Context, rows []SumRow, metadata []MetadataRow) error
	Close() error
}

var (
	insertsTotal     metric.Int64Counter
	insertDurationMS metric.Float64Histogram
)

func init() {
	m := otel.Meter(name)
	var err error
	insertsTotal, err = m.Int64Counter("metric_store.inserts_total",
		metric.WithDescription("Total number of batch inserts to ClickHouse, by table and status"),
		metric.WithUnit("{insert}"))
	if err != nil {
		panic(err)
	}
	insertDurationMS, err = m.Float64Histogram("metric_store.insert_duration_ms",
		metric.WithDescription("Duration of ClickHouse batch inserts in milliseconds"),
		metric.WithUnit("ms"))
	if err != nil {
		panic(err)
	}
}

// ClickHouseMetricsStore implements MetricsStore using a ClickHouse connection.
type ClickHouseMetricsStore struct {
	conn   driver.Conn
	tracer trace.Tracer
}

// NewClickHouseMetricsStore creates a new ClickHouseMetricsStore connected to the given address.
func NewClickHouseMetricsStore(ctx context.Context, addr string, database string, username string, password string) (*ClickHouseMetricsStore, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{addr},
		Auth: clickhouse.Auth{
			Database: database,
			Username: username,
			Password: password,
		},
		Settings: clickhouse.Settings{
			"max_execution_time":    60,
			// Enable server-side async insert buffering: ClickHouse collects multiple
			// small inserts and flushes them together, providing cross-RPC batching
			// without application-level buffering. wait_for_async_insert=1 keeps error
			// feedback: the call returns only after data is accepted into the buffer.
			"async_insert":          1,
			"wait_for_async_insert": 1,
		},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("opening clickhouse connection: %w", err)
	}
	if err := conn.Ping(ctx); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("pinging clickhouse: %w", err)
	}
	return &ClickHouseMetricsStore{
		conn:   conn,
		tracer: otel.Tracer(name),
	}, nil
}

// CreateTables creates all metric tables if they do not already exist.
func (s *ClickHouseMetricsStore) CreateTables(ctx context.Context) error {
	for _, ddl := range []string{
		createMetadataTableSQL,
		createGaugeTableSQL,
		createSumTableSQL,
		createHistogramTableSQL,
		createExponentialHistogramTableSQL,
		createSummaryTableSQL,
	} {
		if err := s.conn.Exec(ctx, ddl); err != nil {
			return fmt.Errorf("creating table: %w", err)
		}
	}
	return nil
}

// insertMetadata batch-inserts metadata rows into otel_metrics_metadata.
// Inserting the same MetricHash row multiple times is idempotent: the ReplacingMergeTree
// engine keeps the latest row per (TimeUnix, MetricName, MetricHash) at compaction time.
// Within-batch duplicates are already collapsed by the mapper before this is called.
func (s *ClickHouseMetricsStore) insertMetadata(ctx context.Context, metadata []MetadataRow) error {
	batch, err := s.conn.PrepareBatch(ctx, "INSERT INTO otel_metrics_metadata")
	if err != nil {
		return fmt.Errorf("preparing metadata batch: %w", err)
	}
	for _, m := range metadata {
		if err := batch.Append(
			m.TimeUnix,
			m.MetricHash,
			m.MetricName,
			m.MetricDescription,
			m.MetricUnit,
			m.MetricType,
			m.ServiceName,
			m.ResourceAttributes,
			m.ScopeAttributes,
			m.Attributes,
			m.ScopeName,
			m.ScopeVersion,
		); err != nil {
			return fmt.Errorf("appending metadata row: %w", err)
		}
	}
	return batch.Send()
}

// InsertGauge batch-inserts metadata rows and gauge datapoint rows.
// Metadata is inserted first; if it fails the datapoints are not inserted, preventing
// orphaned datapoint rows with no resolvable metadata.
func (s *ClickHouseMetricsStore) InsertGauge(ctx context.Context, rows []GaugeRow, metadata []MetadataRow) error {
	ctx, span := s.tracer.Start(ctx, "InsertGauge",
		trace.WithAttributes(
			attribute.String("db.system", "clickhouse"),
			attribute.String("db.table", "otel_metrics_gauge"),
			attribute.Int("batch.size", len(rows)),
		))
	defer span.End()

	start := time.Now()

	tableAttr := attribute.String("table", "otel_metrics_metadata")
	if err := s.insertMetadata(ctx, metadata); err != nil {
		span.RecordError(err)
		insertsTotal.Add(ctx, 1, metric.WithAttributes(tableAttr, attribute.String("status", "error")))
		return fmt.Errorf("inserting gauge metadata: %w", err)
	}
	insertsTotal.Add(ctx, 1, metric.WithAttributes(tableAttr, attribute.String("status", "ok")))

	gaugeTableAttr := attribute.String("table", "otel_metrics_gauge")
	batch, err := s.conn.PrepareBatch(ctx, "INSERT INTO otel_metrics_gauge")
	if err != nil {
		span.RecordError(err)
		insertsTotal.Add(ctx, 1, metric.WithAttributes(gaugeTableAttr, attribute.String("status", "error")))
		return fmt.Errorf("preparing gauge batch: %w", err)
	}
	for _, r := range rows {
		if err := batch.Append(r.MetricHash, r.StartTimeUnix, r.TimeUnix, r.Value, r.Flags); err != nil {
			span.RecordError(err)
			insertsTotal.Add(ctx, 1, metric.WithAttributes(gaugeTableAttr, attribute.String("status", "error")))
			return fmt.Errorf("appending gauge row: %w", err)
		}
	}
	if err := batch.Send(); err != nil {
		span.RecordError(err)
		insertsTotal.Add(ctx, 1, metric.WithAttributes(gaugeTableAttr, attribute.String("status", "error")))
		return fmt.Errorf("sending gauge batch: %w", err)
	}

	elapsed := float64(time.Since(start).Milliseconds())
	insertsTotal.Add(ctx, 1, metric.WithAttributes(gaugeTableAttr, attribute.String("status", "ok")))
	insertDurationMS.Record(ctx, elapsed, metric.WithAttributes(gaugeTableAttr))
	return nil
}

// InsertSum batch-inserts metadata rows and sum datapoint rows.
func (s *ClickHouseMetricsStore) InsertSum(ctx context.Context, rows []SumRow, metadata []MetadataRow) error {
	ctx, span := s.tracer.Start(ctx, "InsertSum",
		trace.WithAttributes(
			attribute.String("db.system", "clickhouse"),
			attribute.String("db.table", "otel_metrics_sum"),
			attribute.Int("batch.size", len(rows)),
		))
	defer span.End()

	start := time.Now()

	tableAttr := attribute.String("table", "otel_metrics_metadata")
	if err := s.insertMetadata(ctx, metadata); err != nil {
		span.RecordError(err)
		insertsTotal.Add(ctx, 1, metric.WithAttributes(tableAttr, attribute.String("status", "error")))
		return fmt.Errorf("inserting sum metadata: %w", err)
	}
	insertsTotal.Add(ctx, 1, metric.WithAttributes(tableAttr, attribute.String("status", "ok")))

	sumTableAttr := attribute.String("table", "otel_metrics_sum")
	batch, err := s.conn.PrepareBatch(ctx, "INSERT INTO otel_metrics_sum")
	if err != nil {
		span.RecordError(err)
		insertsTotal.Add(ctx, 1, metric.WithAttributes(sumTableAttr, attribute.String("status", "error")))
		return fmt.Errorf("preparing sum batch: %w", err)
	}
	for _, r := range rows {
		if err := batch.Append(r.MetricHash, r.StartTimeUnix, r.TimeUnix, r.Value, r.Flags, r.AggregationTemporality, r.IsMonotonic); err != nil {
			span.RecordError(err)
			insertsTotal.Add(ctx, 1, metric.WithAttributes(sumTableAttr, attribute.String("status", "error")))
			return fmt.Errorf("appending sum row: %w", err)
		}
	}
	if err := batch.Send(); err != nil {
		span.RecordError(err)
		insertsTotal.Add(ctx, 1, metric.WithAttributes(sumTableAttr, attribute.String("status", "error")))
		return fmt.Errorf("sending sum batch: %w", err)
	}

	elapsed := float64(time.Since(start).Milliseconds())
	insertsTotal.Add(ctx, 1, metric.WithAttributes(sumTableAttr, attribute.String("status", "ok")))
	insertDurationMS.Record(ctx, elapsed, metric.WithAttributes(sumTableAttr))
	return nil
}

// Close closes the underlying ClickHouse connection.
func (s *ClickHouseMetricsStore) Close() error {
	return s.conn.Close()
}
