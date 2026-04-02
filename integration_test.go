//go:build integration

package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func setupClickHouse(t *testing.T) (*ClickHouseMetricsStore, func()) {
	t.Helper()
	ctx := context.Background()

	ctr, err := testcontainers.Run(ctx, "clickhouse/clickhouse-server:26.2",
		testcontainers.WithExposedPorts("9000/tcp"),
		testcontainers.WithEnv(map[string]string{
			"CLICKHOUSE_USER":     "default",
			"CLICKHOUSE_PASSWORD": "test",
		}),
		testcontainers.WithWaitStrategy(
			wait.ForListeningPort("9000/tcp").WithStartupTimeout(60*time.Second),
		),
	)
	if err != nil {
		t.Fatalf("starting clickhouse container: %v", err)
	}

	host, err := ctr.Host(ctx)
	if err != nil {
		t.Fatalf("getting container host: %v", err)
	}
	mappedPort, err := ctr.MappedPort(ctx, "9000/tcp")
	if err != nil {
		t.Fatalf("getting mapped port: %v", err)
	}

	addr := fmt.Sprintf("%s:%s", host, mappedPort.Port())
	store, err := NewClickHouseMetricsStore(ctx, addr, "default", "default", "test")
	if err != nil {
		t.Fatalf("creating clickhouse metrics store: %v", err)
	}

	cleanup := func() {
		store.Close()
		if err := ctr.Terminate(ctx); err != nil {
			t.Logf("terminating clickhouse container: %v", err)
		}
	}

	return store, cleanup
}

func TestCreateTables(t *testing.T) {
	store, cleanup := setupClickHouse(t)
	defer cleanup()

	ctx := context.Background()
	if err := store.CreateTables(ctx); err != nil {
		t.Fatalf("creating tables: %v", err)
	}

	expectedTables := []string{
		"otel_metrics_metadata",
		"otel_metrics_gauge",
		"otel_metrics_sum",
		"otel_metrics_histogram",
		"otel_metrics_exponential_histogram",
		"otel_metrics_summary",
	}

	for _, table := range expectedTables {
		var count uint64
		err := store.conn.QueryRow(ctx,
			"SELECT count() FROM system.tables WHERE database = 'default' AND name = $1", table,
		).Scan(&count)
		if err != nil {
			t.Fatalf("querying system.tables for %s: %v", table, err)
		}
		if count != 1 {
			t.Errorf("expected table %s to exist, got count=%d", table, count)
		}
	}
}

func TestInsertGauge(t *testing.T) {
	store, cleanup := setupClickHouse(t)
	defer cleanup()

	ctx := context.Background()
	if err := store.CreateTables(ctx); err != nil {
		t.Fatalf("creating tables: %v", err)
	}

	now := uint64(time.Now().UnixNano())
	startTime := now - uint64(time.Minute)
	resourceMetrics := []*metricspb.ResourceMetrics{
		{
			Resource: &resourcepb.Resource{
				Attributes: []*commonpb.KeyValue{
					{Key: "service.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "test-service"}}},
					{Key: "host.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "test-host"}}},
				},
			},
			SchemaUrl: "https://opentelemetry.io/schemas/1.4.0",
			ScopeMetrics: []*metricspb.ScopeMetrics{
				{
					Scope: &commonpb.InstrumentationScope{
						Name:    "test-scope",
						Version: "1.0.0",
					},
					Metrics: []*metricspb.Metric{
						{
							Name:        "cpu.utilization",
							Description: "CPU utilization percentage",
							Unit:        "%",
							Data: &metricspb.Metric_Gauge{
								Gauge: &metricspb.Gauge{
									DataPoints: []*metricspb.NumberDataPoint{
										{
											Attributes:        []*commonpb.KeyValue{{Key: "cpu", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "0"}}}},
											StartTimeUnixNano: startTime,
											TimeUnixNano:      now,
											Value:             &metricspb.NumberDataPoint_AsDouble{AsDouble: 42.5},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	rows, metadata := MapGaugeRows(resourceMetrics)
	if err := store.InsertGauge(ctx, rows, metadata); err != nil {
		t.Fatalf("inserting gauge rows: %v", err)
	}

	// Verify the datapoint row in otel_metrics_gauge.
	var (
		metricHash uint64
		value      float64
	)
	err := store.conn.QueryRow(ctx,
		"SELECT MetricHash, Value FROM otel_metrics_gauge",
	).Scan(&metricHash, &value)
	if err != nil {
		t.Fatalf("querying otel_metrics_gauge: %v", err)
	}
	if value != 42.5 {
		t.Errorf("expected Value=42.5, got %f", value)
	}
	if metricHash == 0 {
		t.Error("expected non-zero MetricHash in otel_metrics_gauge")
	}

	// Verify the metadata row in otel_metrics_metadata (FINAL forces merge before asserting).
	var (
		metaName    string
		metaSvcName string
		metaHash    uint64
	)
	err = store.conn.QueryRow(ctx,
		"SELECT MetricName, ServiceName, MetricHash FROM otel_metrics_metadata FINAL WHERE MetricHash = $1",
		metricHash,
	).Scan(&metaName, &metaSvcName, &metaHash)
	if err != nil {
		t.Fatalf("querying otel_metrics_metadata: %v", err)
	}
	if metaName != "cpu.utilization" {
		t.Errorf("expected MetricName=cpu.utilization, got %s", metaName)
	}
	if metaSvcName != "test-service" {
		t.Errorf("expected ServiceName=test-service, got %s", metaSvcName)
	}
	if metaHash != metricHash {
		t.Errorf("expected MetricHash to match gauge row, got %d vs %d", metaHash, metricHash)
	}
}

func TestInsertSum(t *testing.T) {
	store, cleanup := setupClickHouse(t)
	defer cleanup()

	ctx := context.Background()
	if err := store.CreateTables(ctx); err != nil {
		t.Fatalf("creating tables: %v", err)
	}

	now := uint64(time.Now().UnixNano())
	startTime := now - uint64(time.Minute)
	resourceMetrics := []*metricspb.ResourceMetrics{
		{
			Resource: &resourcepb.Resource{
				Attributes: []*commonpb.KeyValue{
					{Key: "service.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "test-service"}}},
					{Key: "host.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "test-host"}}},
				},
			},
			SchemaUrl: "https://opentelemetry.io/schemas/1.4.0",
			ScopeMetrics: []*metricspb.ScopeMetrics{
				{
					Scope: &commonpb.InstrumentationScope{
						Name:    "test-scope",
						Version: "1.0.0",
					},
					Metrics: []*metricspb.Metric{
						{
							Name:        "http.requests.total",
							Description: "Total HTTP requests",
							Unit:        "{request}",
							Data: &metricspb.Metric_Sum{
								Sum: &metricspb.Sum{
									AggregationTemporality: metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE,
									IsMonotonic:            true,
									DataPoints: []*metricspb.NumberDataPoint{
										{
											Attributes: []*commonpb.KeyValue{
												{Key: "method", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "GET"}}},
												{Key: "status", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "200"}}},
											},
											StartTimeUnixNano: startTime,
											TimeUnixNano:      now,
											Value:             &metricspb.NumberDataPoint_AsDouble{AsDouble: 1234},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	rows, metadata := MapSumRows(resourceMetrics)
	if err := store.InsertSum(ctx, rows, metadata); err != nil {
		t.Fatalf("inserting sum rows: %v", err)
	}

	// Verify the datapoint row in otel_metrics_sum.
	var (
		metricHash             uint64
		value                  float64
		aggregationTemporality int32
		isMonotonic            bool
	)
	err := store.conn.QueryRow(ctx,
		"SELECT MetricHash, Value, AggregationTemporality, IsMonotonic FROM otel_metrics_sum",
	).Scan(&metricHash, &value, &aggregationTemporality, &isMonotonic)
	if err != nil {
		t.Fatalf("querying otel_metrics_sum: %v", err)
	}
	if value != 1234 {
		t.Errorf("expected Value=1234, got %f", value)
	}
	if aggregationTemporality != 2 {
		t.Errorf("expected AggregationTemporality=2, got %d", aggregationTemporality)
	}
	if !isMonotonic {
		t.Errorf("expected IsMonotonic=true, got false")
	}

	// Verify the metadata row in otel_metrics_metadata.
	var (
		metaName    string
		metaSvcName string
	)
	err = store.conn.QueryRow(ctx,
		"SELECT MetricName, ServiceName FROM otel_metrics_metadata FINAL WHERE MetricHash = $1",
		metricHash,
	).Scan(&metaName, &metaSvcName)
	if err != nil {
		t.Fatalf("querying otel_metrics_metadata: %v", err)
	}
	if metaName != "http.requests.total" {
		t.Errorf("expected MetricName=http.requests.total, got %s", metaName)
	}
	if metaSvcName != "test-service" {
		t.Errorf("expected ServiceName=test-service, got %s", metaSvcName)
	}
}

// TestInsertMetadata verifies that inserting the same metadata twice results in exactly one
// row in otel_metrics_metadata FINAL — validating the idempotency guarantee of the
// ReplacingMergeTree engine with a deterministic MetricHash.
func TestInsertMetadata(t *testing.T) {
	store, cleanup := setupClickHouse(t)
	defer cleanup()

	ctx := context.Background()
	if err := store.CreateTables(ctx); err != nil {
		t.Fatalf("creating tables: %v", err)
	}

	now := uint64(time.Now().UnixNano())
	resourceMetrics := []*metricspb.ResourceMetrics{
		{
			Resource: &resourcepb.Resource{
				Attributes: []*commonpb.KeyValue{
					{Key: "service.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "idempotency-service"}}},
				},
			},
			ScopeMetrics: []*metricspb.ScopeMetrics{
				{
					Scope: &commonpb.InstrumentationScope{Name: "idempotency-scope"},
					Metrics: []*metricspb.Metric{
						{
							Name: "idempotency.gauge",
							Data: &metricspb.Metric_Gauge{
								Gauge: &metricspb.Gauge{
									DataPoints: []*metricspb.NumberDataPoint{
										{
											TimeUnixNano: now,
											Value:        &metricspb.NumberDataPoint_AsDouble{AsDouble: 1.0},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	// Insert the same metric twice — same hash, same metadata.
	rows, metadata := MapGaugeRows(resourceMetrics)
	if err := store.InsertGauge(ctx, rows, metadata); err != nil {
		t.Fatalf("first InsertGauge: %v", err)
	}
	if err := store.InsertGauge(ctx, rows, metadata); err != nil {
		t.Fatalf("second InsertGauge: %v", err)
	}

	// FINAL forces merge: duplicate (TimeUnix, MetricName, MetricHash) rows are collapsed.
	var count uint64
	err := store.conn.QueryRow(ctx,
		"SELECT count() FROM otel_metrics_metadata FINAL WHERE MetricName = 'idempotency.gauge'",
	).Scan(&count)
	if err != nil {
		t.Fatalf("querying otel_metrics_metadata: %v", err)
	}
	if count != 1 {
		t.Errorf("expected exactly 1 metadata row after two identical inserts, got %d", count)
	}
}

func TestGRPCToClickHouse(t *testing.T) {
	store, cleanup := setupClickHouse(t)
	defer cleanup()

	ctx := context.Background()
	if err := store.CreateTables(ctx); err != nil {
		t.Fatalf("creating tables: %v", err)
	}

	// Start gRPC server wired to the ClickHouse store.
	lis := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	colmetricspb.RegisterMetricsServiceServer(grpcServer, newServer("bufconn", store))
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Printf("error serving server: %v", err)
		}
	}()
	defer grpcServer.Stop()

	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("connecting to grpc server: %v", err)
	}
	defer conn.Close()

	client := colmetricspb.NewMetricsServiceClient(conn)

	// Send a gauge metric via gRPC.
	now := uint64(time.Now().UnixNano())
	_, err = client.Export(ctx, &colmetricspb.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricspb.ResourceMetrics{
			{
				Resource: &resourcepb.Resource{
					Attributes: []*commonpb.KeyValue{
						{Key: "service.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "e2e-service"}}},
					},
				},
				ScopeMetrics: []*metricspb.ScopeMetrics{
					{
						Scope: &commonpb.InstrumentationScope{Name: "e2e-scope"},
						Metrics: []*metricspb.Metric{
							{
								Name: "e2e.gauge",
								Data: &metricspb.Metric_Gauge{
									Gauge: &metricspb.Gauge{
										DataPoints: []*metricspb.NumberDataPoint{
											{
												TimeUnixNano: now,
												Value:        &metricspb.NumberDataPoint_AsDouble{AsDouble: 99.9},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("exporting metrics via grpc: %v", err)
	}

	// Verify the datapoint landed in otel_metrics_gauge.
	var (
		metricHash uint64
		value      float64
	)
	err = store.conn.QueryRow(ctx,
		"SELECT MetricHash, Value FROM otel_metrics_gauge WHERE Value = 99.9",
	).Scan(&metricHash, &value)
	if err != nil {
		t.Fatalf("querying otel_metrics_gauge: %v", err)
	}
	if value != 99.9 {
		t.Errorf("expected Value=99.9, got %f", value)
	}

	// Verify the metadata is resolvable via the MetricHash.
	var metaSvcName string
	err = store.conn.QueryRow(ctx,
		"SELECT ServiceName FROM otel_metrics_metadata FINAL WHERE MetricHash = $1",
		metricHash,
	).Scan(&metaSvcName)
	if err != nil {
		t.Fatalf("querying otel_metrics_metadata: %v", err)
	}
	if metaSvcName != "e2e-service" {
		t.Errorf("expected ServiceName=e2e-service, got %s", metaSvcName)
	}
}
