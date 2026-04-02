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
		"SELECT MetricHash, Value FROM otel_metrics_gauge WHERE MetricHash = $1",
		rows[0].MetricHash,
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
		"SELECT MetricHash, Value, AggregationTemporality, IsMonotonic FROM otel_metrics_sum WHERE MetricHash = $1",
		rows[0].GaugeRow.MetricHash,
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

// TestInsertMetadata_AnyLastSemantics verifies that when the same MetricHash is inserted
// twice with a different MetricDescription, the AggregatingMergeTree's anyLast semantics
// keep the most recently inserted value. MetricDescription is not part of the hash identity,
// so it can change independently without creating a new series.
func TestInsertMetadata_AnyLastSemantics(t *testing.T) {
	store, cleanup := setupClickHouse(t)
	defer cleanup()

	ctx := context.Background()
	if err := store.CreateTables(ctx); err != nil {
		t.Fatalf("creating tables: %v", err)
	}

	now := uint64(time.Now().UnixNano())

	makeRM := func(description string) []*metricspb.ResourceMetrics {
		return []*metricspb.ResourceMetrics{
			{
				Resource: &resourcepb.Resource{
					Attributes: []*commonpb.KeyValue{
						{Key: "service.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "anylast-service"}}},
					},
				},
				ScopeMetrics: []*metricspb.ScopeMetrics{
					{
						Scope: &commonpb.InstrumentationScope{Name: "anylast-scope"},
						Metrics: []*metricspb.Metric{
							{
								Name:        "anylast.gauge",
								Description: description,
								Data: &metricspb.Metric_Gauge{
									Gauge: &metricspb.Gauge{
										DataPoints: []*metricspb.NumberDataPoint{
											{TimeUnixNano: now, Value: &metricspb.NumberDataPoint_AsDouble{AsDouble: 1.0}},
										},
									},
								},
							},
						},
					},
				},
			},
		}
	}

	rows1, metadata1 := MapGaugeRows(makeRM("v1 description"))
	if err := store.InsertGauge(ctx, rows1, metadata1); err != nil {
		t.Fatalf("first insert: %v", err)
	}

	rows2, metadata2 := MapGaugeRows(makeRM("v2 description"))
	if rows1[0].MetricHash != rows2[0].MetricHash {
		t.Fatalf("expected same MetricHash for same identity, got %d vs %d", rows1[0].MetricHash, rows2[0].MetricHash)
	}
	if err := store.InsertGauge(ctx, rows2, metadata2); err != nil {
		t.Fatalf("second insert: %v", err)
	}

	// FINAL forces merge; anyLast should retain the most recently inserted MetricDescription.
	var desc string
	err := store.conn.QueryRow(ctx,
		"SELECT MetricDescription FROM otel_metrics_metadata FINAL WHERE MetricHash = $1",
		rows1[0].MetricHash,
	).Scan(&desc)
	if err != nil {
		t.Fatalf("querying otel_metrics_metadata: %v", err)
	}
	if desc != "v2 description" {
		t.Errorf("expected anyLast to keep 'v2 description', got %q", desc)
	}
}

// TestInsertMetadata verifies that inserting the same metadata twice results in exactly one
// row in otel_metrics_metadata FINAL — validating the idempotency guarantee of the
// AggregatingMergeTree engine with a deterministic MetricHash.
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

func TestGRPCToClickHouseSum(t *testing.T) {
	store, cleanup := setupClickHouse(t)
	defer cleanup()

	ctx := context.Background()
	if err := store.CreateTables(ctx); err != nil {
		t.Fatalf("creating tables: %v", err)
	}

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

	now := uint64(time.Now().UnixNano())
	sumResourceMetrics := []*metricspb.ResourceMetrics{
		{
			Resource: &resourcepb.Resource{
				Attributes: []*commonpb.KeyValue{
					{Key: "service.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "e2e-sum-service"}}},
				},
			},
			ScopeMetrics: []*metricspb.ScopeMetrics{
				{
					Scope: &commonpb.InstrumentationScope{Name: "e2e-scope"},
					Metrics: []*metricspb.Metric{
						{
							Name: "e2e.requests.total",
							Data: &metricspb.Metric_Sum{
								Sum: &metricspb.Sum{
									AggregationTemporality: metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE,
									IsMonotonic:            true,
									DataPoints: []*metricspb.NumberDataPoint{
										{
											TimeUnixNano: now,
											Value:        &metricspb.NumberDataPoint_AsDouble{AsDouble: 42.0},
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

	// Pre-compute expected MetricHash before sending so we can query deterministically.
	expectedSumRows, _ := MapSumRows(sumResourceMetrics)
	if len(expectedSumRows) == 0 {
		t.Fatal("expected at least one sum row from MapSumRows")
	}
	expectedSumHash := expectedSumRows[0].GaugeRow.MetricHash

	_, err = client.Export(ctx, &colmetricspb.ExportMetricsServiceRequest{
		ResourceMetrics: sumResourceMetrics,
	})
	if err != nil {
		t.Fatalf("exporting sum metric via grpc: %v", err)
	}

	// Verify the datapoint landed in otel_metrics_sum.
	var (
		metricHash  uint64
		value       float64
		isMonotonic bool
	)
	err = store.conn.QueryRow(ctx,
		"SELECT MetricHash, Value, IsMonotonic FROM otel_metrics_sum WHERE MetricHash = $1",
		expectedSumHash,
	).Scan(&metricHash, &value, &isMonotonic)
	if err != nil {
		t.Fatalf("querying otel_metrics_sum: %v", err)
	}
	if value != 42.0 {
		t.Errorf("expected Value=42.0, got %f", value)
	}
	if !isMonotonic {
		t.Errorf("expected IsMonotonic=true")
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
	if metaSvcName != "e2e-sum-service" {
		t.Errorf("expected ServiceName=e2e-sum-service, got %s", metaSvcName)
	}
}

// TestGRPCMixedBatch sends a single Export request containing both a Gauge and a Sum metric,
// exercising the errgroup concurrent insert path in metrics_service.go. Both datapoints and
// their metadata must land in their respective tables after a single RPC call.
func TestGRPCMixedBatch(t *testing.T) {
	store, cleanup := setupClickHouse(t)
	defer cleanup()

	ctx := context.Background()
	if err := store.CreateTables(ctx); err != nil {
		t.Fatalf("creating tables: %v", err)
	}

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

	now := uint64(time.Now().UnixNano())

	// A single ResourceMetrics payload containing one Gauge and one Sum under the same resource.
	mixedResourceMetrics := []*metricspb.ResourceMetrics{
		{
			Resource: &resourcepb.Resource{
				Attributes: []*commonpb.KeyValue{
					{Key: "service.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "mixed-service"}}},
				},
			},
			ScopeMetrics: []*metricspb.ScopeMetrics{
				{
					Scope: &commonpb.InstrumentationScope{Name: "mixed-scope"},
					Metrics: []*metricspb.Metric{
						{
							Name: "mixed.cpu.usage",
							Data: &metricspb.Metric_Gauge{
								Gauge: &metricspb.Gauge{
									DataPoints: []*metricspb.NumberDataPoint{
										{TimeUnixNano: now, Value: &metricspb.NumberDataPoint_AsDouble{AsDouble: 55.0}},
									},
								},
							},
						},
						{
							Name: "mixed.requests.total",
							Data: &metricspb.Metric_Sum{
								Sum: &metricspb.Sum{
									AggregationTemporality: metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE,
									IsMonotonic:            true,
									DataPoints: []*metricspb.NumberDataPoint{
										{TimeUnixNano: now, Value: &metricspb.NumberDataPoint_AsDouble{AsDouble: 100.0}},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	// Pre-compute hashes so we can query deterministically after the RPC.
	expectedGaugeRows, _ := MapGaugeRows(mixedResourceMetrics)
	expectedSumRows, _ := MapSumRows(mixedResourceMetrics)
	if len(expectedGaugeRows) == 0 || len(expectedSumRows) == 0 {
		t.Fatal("expected at least one gauge and one sum row from mappers")
	}
	gaugeHash := expectedGaugeRows[0].MetricHash
	sumHash := expectedSumRows[0].GaugeRow.MetricHash

	_, err = client.Export(ctx, &colmetricspb.ExportMetricsServiceRequest{
		ResourceMetrics: mixedResourceMetrics,
	})
	if err != nil {
		t.Fatalf("exporting mixed batch via grpc: %v", err)
	}

	// Gauge datapoint must be in otel_metrics_gauge.
	var gaugeValue float64
	if err := store.conn.QueryRow(ctx,
		"SELECT Value FROM otel_metrics_gauge WHERE MetricHash = $1",
		gaugeHash,
	).Scan(&gaugeValue); err != nil {
		t.Fatalf("querying otel_metrics_gauge: %v", err)
	}
	if gaugeValue != 55.0 {
		t.Errorf("expected gauge Value=55.0, got %f", gaugeValue)
	}

	// Sum datapoint must be in otel_metrics_sum.
	var sumValue float64
	if err := store.conn.QueryRow(ctx,
		"SELECT Value FROM otel_metrics_sum WHERE MetricHash = $1",
		sumHash,
	).Scan(&sumValue); err != nil {
		t.Fatalf("querying otel_metrics_sum: %v", err)
	}
	if sumValue != 100.0 {
		t.Errorf("expected sum Value=100.0, got %f", sumValue)
	}

	// Both metadata entries must be resolvable from their respective hashes.
	var gaugeName, sumName string
	if err := store.conn.QueryRow(ctx,
		"SELECT MetricName FROM otel_metrics_metadata FINAL WHERE MetricHash = $1",
		gaugeHash,
	).Scan(&gaugeName); err != nil {
		t.Fatalf("querying metadata for gauge hash: %v", err)
	}
	if gaugeName != "mixed.cpu.usage" {
		t.Errorf("expected MetricName=mixed.cpu.usage, got %s", gaugeName)
	}

	if err := store.conn.QueryRow(ctx,
		"SELECT MetricName FROM otel_metrics_metadata FINAL WHERE MetricHash = $1",
		sumHash,
	).Scan(&sumName); err != nil {
		t.Fatalf("querying metadata for sum hash: %v", err)
	}
	if sumName != "mixed.requests.total" {
		t.Errorf("expected MetricName=mixed.requests.total, got %s", sumName)
	}
}

// TestResourceAttributeEvolution verifies that when the same metric name is reported from two
// resources with different attributes (e.g. two hosts), each produces a distinct MetricHash and
// a separate metadata row. This validates the assignment requirement: "Resources (Attributes)
// are likely to change over time" — a new resource identity must never overwrite an existing one.
func TestResourceAttributeEvolution(t *testing.T) {
	store, cleanup := setupClickHouse(t)
	defer cleanup()

	ctx := context.Background()
	if err := store.CreateTables(ctx); err != nil {
		t.Fatalf("creating tables: %v", err)
	}

	makeRM := func(hostname string) []*metricspb.ResourceMetrics {
		now := uint64(time.Now().UnixNano())
		return []*metricspb.ResourceMetrics{
			{
				Resource: &resourcepb.Resource{
					Attributes: []*commonpb.KeyValue{
						{Key: "service.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "evo-service"}}},
						{Key: "host.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: hostname}}},
					},
				},
				ScopeMetrics: []*metricspb.ScopeMetrics{
					{
						Scope: &commonpb.InstrumentationScope{Name: "evo-scope"},
						Metrics: []*metricspb.Metric{
							{
								Name: "evo.cpu.usage",
								Data: &metricspb.Metric_Gauge{
									Gauge: &metricspb.Gauge{
										DataPoints: []*metricspb.NumberDataPoint{
											{TimeUnixNano: now, Value: &metricspb.NumberDataPoint_AsDouble{AsDouble: 1.0}},
										},
									},
								},
							},
						},
					},
				},
			},
		}
	}

	rm1 := makeRM("host-1")
	rm2 := makeRM("host-2")

	rows1, metadata1 := MapGaugeRows(rm1)
	rows2, metadata2 := MapGaugeRows(rm2)

	hash1 := rows1[0].MetricHash
	hash2 := rows2[0].MetricHash
	if hash1 == hash2 {
		t.Fatalf("expected different MetricHash for different resource attributes, got same hash %d", hash1)
	}

	if err := store.InsertGauge(ctx, rows1, metadata1); err != nil {
		t.Fatalf("inserting host-1 gauge: %v", err)
	}
	if err := store.InsertGauge(ctx, rows2, metadata2); err != nil {
		t.Fatalf("inserting host-2 gauge: %v", err)
	}

	// Each host must have its own metadata row — two distinct series in otel_metrics_metadata.
	var count uint64
	if err := store.conn.QueryRow(ctx,
		"SELECT count() FROM otel_metrics_metadata FINAL WHERE MetricName = 'evo.cpu.usage'",
	).Scan(&count); err != nil {
		t.Fatalf("querying otel_metrics_metadata: %v", err)
	}
	if count != 2 {
		t.Errorf("expected 2 metadata rows for 2 distinct resource attribute sets, got %d", count)
	}

	// Verify each hash resolves to the correct host's resource attributes.
	var resAttrs1, resAttrs2 map[string]string
	if err := store.conn.QueryRow(ctx,
		"SELECT ResourceAttributes FROM otel_metrics_metadata FINAL WHERE MetricHash = $1",
		hash1,
	).Scan(&resAttrs1); err != nil {
		t.Fatalf("querying metadata for host-1 hash: %v", err)
	}
	if resAttrs1["host.name"] != "host-1" {
		t.Errorf("expected host.name=host-1 in ResourceAttributes, got %v", resAttrs1)
	}

	if err := store.conn.QueryRow(ctx,
		"SELECT ResourceAttributes FROM otel_metrics_metadata FINAL WHERE MetricHash = $1",
		hash2,
	).Scan(&resAttrs2); err != nil {
		t.Fatalf("querying metadata for host-2 hash: %v", err)
	}
	if resAttrs2["host.name"] != "host-2" {
		t.Errorf("expected host.name=host-2 in ResourceAttributes, got %v", resAttrs2)
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

	// Build gauge resource metrics once so we can derive the expected MetricHash before sending.
	now := uint64(time.Now().UnixNano())
	gaugeResourceMetrics := []*metricspb.ResourceMetrics{
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
	}

	// Pre-compute expected MetricHash before sending so we can query deterministically.
	expectedGaugeRows, _ := MapGaugeRows(gaugeResourceMetrics)
	if len(expectedGaugeRows) == 0 {
		t.Fatal("expected at least one gauge row from MapGaugeRows")
	}
	expectedGaugeHash := expectedGaugeRows[0].MetricHash

	_, err = client.Export(ctx, &colmetricspb.ExportMetricsServiceRequest{
		ResourceMetrics: gaugeResourceMetrics,
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
		"SELECT MetricHash, Value FROM otel_metrics_gauge WHERE MetricHash = $1",
		expectedGaugeHash,
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
