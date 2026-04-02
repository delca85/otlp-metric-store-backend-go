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

	client, grpcCleanup := setupGRPCServer(t, store)
	defer grpcCleanup()

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

	_, err := client.Export(ctx, &colmetricspb.ExportMetricsServiceRequest{
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

	client, grpcCleanup := setupGRPCServer(t, store)
	defer grpcCleanup()

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

	_, err := client.Export(ctx, &colmetricspb.ExportMetricsServiceRequest{
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

// setupGRPCServer starts a bufconn-backed gRPC server wired to store and returns a connected
// MetricsServiceClient together with a teardown function. Callers must defer the teardown.
func setupGRPCServer(t *testing.T, store MetricsStore) (colmetricspb.MetricsServiceClient, func()) {
	t.Helper()
	lis := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	colmetricspb.RegisterMetricsServiceServer(grpcServer, newServer("bufconn", store))
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Printf("error serving grpc server: %v", err)
		}
	}()
	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("connecting to grpc server: %v", err)
	}
	return colmetricspb.NewMetricsServiceClient(conn), func() {
		conn.Close()
		grpcServer.Stop()
	}
}

// TestGRPC_NilResource_InsertsWithEmptyServiceName verifies that a ResourceMetrics entry with a
// nil Resource does not panic and produces a row with ServiceName="" in both the datapoint and
// metadata tables. The proto getter chain (GetResource().GetAttributes()) is nil-safe, so the
// mapper falls through to an empty service name rather than crashing.
func TestGRPC_NilResource_InsertsWithEmptyServiceName(t *testing.T) {
	store, cleanup := setupClickHouse(t)
	defer cleanup()

	ctx := context.Background()
	if err := store.CreateTables(ctx); err != nil {
		t.Fatalf("creating tables: %v", err)
	}

	client, grpcCleanup := setupGRPCServer(t, store)
	defer grpcCleanup()

	now := uint64(time.Now().UnixNano())
	rm := []*metricspb.ResourceMetrics{
		{
			Resource: nil, // intentionally absent
			ScopeMetrics: []*metricspb.ScopeMetrics{
				{
					Scope: &commonpb.InstrumentationScope{Name: "nil-resource-scope"},
					Metrics: []*metricspb.Metric{
						{
							Name: "nil.resource.gauge",
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

	_, err := client.Export(ctx, &colmetricspb.ExportMetricsServiceRequest{ResourceMetrics: rm})
	if err != nil {
		t.Fatalf("Export with nil Resource returned unexpected error: %v", err)
	}

	expectedRows, _ := MapGaugeRows(rm)
	if len(expectedRows) == 0 {
		t.Fatal("expected mapper to produce a gauge row despite nil Resource")
	}
	hash := expectedRows[0].MetricHash

	var svcName string
	if err := store.conn.QueryRow(ctx,
		"SELECT ServiceName FROM otel_metrics_metadata FINAL WHERE MetricHash = $1", hash,
	).Scan(&svcName); err != nil {
		t.Fatalf("querying otel_metrics_metadata: %v", err)
	}
	if svcName != "" {
		t.Errorf("expected ServiceName=\"\" for nil Resource, got %q", svcName)
	}
}

// TestGRPC_MissingServiceName_InsertsWithEmptyServiceName verifies that a Resource present but
// lacking a "service.name" attribute produces a row with ServiceName="" — the mapper returns ""
// when the attribute is absent rather than failing.
func TestGRPC_MissingServiceName_InsertsWithEmptyServiceName(t *testing.T) {
	store, cleanup := setupClickHouse(t)
	defer cleanup()

	ctx := context.Background()
	if err := store.CreateTables(ctx); err != nil {
		t.Fatalf("creating tables: %v", err)
	}

	client, grpcCleanup := setupGRPCServer(t, store)
	defer grpcCleanup()

	now := uint64(time.Now().UnixNano())
	rm := []*metricspb.ResourceMetrics{
		{
			Resource: &resourcepb.Resource{
				Attributes: []*commonpb.KeyValue{
					// host.name is present but service.name is intentionally omitted.
					{Key: "host.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "my-host"}}},
				},
			},
			ScopeMetrics: []*metricspb.ScopeMetrics{
				{
					Scope: &commonpb.InstrumentationScope{Name: "no-svcname-scope"},
					Metrics: []*metricspb.Metric{
						{
							Name: "no.svcname.gauge",
							Data: &metricspb.Metric_Gauge{
								Gauge: &metricspb.Gauge{
									DataPoints: []*metricspb.NumberDataPoint{
										{TimeUnixNano: now, Value: &metricspb.NumberDataPoint_AsDouble{AsDouble: 2.0}},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	_, err := client.Export(ctx, &colmetricspb.ExportMetricsServiceRequest{ResourceMetrics: rm})
	if err != nil {
		t.Fatalf("Export without service.name returned unexpected error: %v", err)
	}

	expectedRows, _ := MapGaugeRows(rm)
	if len(expectedRows) == 0 {
		t.Fatal("expected mapper to produce a gauge row despite missing service.name")
	}
	hash := expectedRows[0].MetricHash

	var svcName string
	if err := store.conn.QueryRow(ctx,
		"SELECT ServiceName FROM otel_metrics_metadata FINAL WHERE MetricHash = $1", hash,
	).Scan(&svcName); err != nil {
		t.Fatalf("querying otel_metrics_metadata: %v", err)
	}
	if svcName != "" {
		t.Errorf("expected ServiceName=\"\" when service.name attribute is absent, got %q", svcName)
	}
}

// TestGRPC_EmptyMetricName_InsertsWithEmptyName verifies that a metric with an empty name string
// is accepted and stored as-is. The hash is computed with MetricName="" — it is stable and
// non-zero — so the row is inserted and queryable via its hash.
func TestGRPC_EmptyMetricName_InsertsWithEmptyName(t *testing.T) {
	store, cleanup := setupClickHouse(t)
	defer cleanup()

	ctx := context.Background()
	if err := store.CreateTables(ctx); err != nil {
		t.Fatalf("creating tables: %v", err)
	}

	client, grpcCleanup := setupGRPCServer(t, store)
	defer grpcCleanup()

	now := uint64(time.Now().UnixNano())
	rm := []*metricspb.ResourceMetrics{
		{
			Resource: &resourcepb.Resource{
				Attributes: []*commonpb.KeyValue{
					{Key: "service.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "empty-name-svc"}}},
				},
			},
			ScopeMetrics: []*metricspb.ScopeMetrics{
				{
					Scope: &commonpb.InstrumentationScope{Name: "empty-name-scope"},
					Metrics: []*metricspb.Metric{
						{
							Name: "", // intentionally empty
							Data: &metricspb.Metric_Gauge{
								Gauge: &metricspb.Gauge{
									DataPoints: []*metricspb.NumberDataPoint{
										{TimeUnixNano: now, Value: &metricspb.NumberDataPoint_AsDouble{AsDouble: 3.0}},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	_, err := client.Export(ctx, &colmetricspb.ExportMetricsServiceRequest{ResourceMetrics: rm})
	if err != nil {
		t.Fatalf("Export with empty metric name returned unexpected error: %v", err)
	}

	expectedRows, _ := MapGaugeRows(rm)
	if len(expectedRows) == 0 {
		t.Fatal("expected mapper to produce a gauge row despite empty metric name")
	}
	hash := expectedRows[0].MetricHash

	var metricName string
	if err := store.conn.QueryRow(ctx,
		"SELECT MetricName FROM otel_metrics_metadata FINAL WHERE MetricHash = $1", hash,
	).Scan(&metricName); err != nil {
		t.Fatalf("querying otel_metrics_metadata: %v", err)
	}
	if metricName != "" {
		t.Errorf("expected MetricName=\"\" to be stored as-is, got %q", metricName)
	}
}

// TestGRPC_NilDataPointValue_InsertsAsZero verifies that a NumberDataPoint whose Value oneof is
// unset (nil) produces a row with Value=0. The numberDataPointValue helper returns 0 for
// unrecognised oneof variants rather than panicking or dropping the datapoint.
func TestGRPC_NilDataPointValue_InsertsAsZero(t *testing.T) {
	store, cleanup := setupClickHouse(t)
	defer cleanup()

	ctx := context.Background()
	if err := store.CreateTables(ctx); err != nil {
		t.Fatalf("creating tables: %v", err)
	}

	client, grpcCleanup := setupGRPCServer(t, store)
	defer grpcCleanup()

	now := uint64(time.Now().UnixNano())
	rm := []*metricspb.ResourceMetrics{
		{
			Resource: &resourcepb.Resource{
				Attributes: []*commonpb.KeyValue{
					{Key: "service.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "nil-value-svc"}}},
				},
			},
			ScopeMetrics: []*metricspb.ScopeMetrics{
				{
					Scope: &commonpb.InstrumentationScope{Name: "nil-value-scope"},
					Metrics: []*metricspb.Metric{
						{
							Name: "nil.value.gauge",
							Data: &metricspb.Metric_Gauge{
								Gauge: &metricspb.Gauge{
									DataPoints: []*metricspb.NumberDataPoint{
										{TimeUnixNano: now, Value: nil}, // oneof unset
									},
								},
							},
						},
					},
				},
			},
		},
	}

	_, err := client.Export(ctx, &colmetricspb.ExportMetricsServiceRequest{ResourceMetrics: rm})
	if err != nil {
		t.Fatalf("Export with nil DataPoint Value returned unexpected error: %v", err)
	}

	expectedRows, _ := MapGaugeRows(rm)
	if len(expectedRows) == 0 {
		t.Fatal("expected mapper to produce a gauge row despite nil DataPoint Value")
	}
	hash := expectedRows[0].MetricHash

	var value float64
	if err := store.conn.QueryRow(ctx,
		"SELECT Value FROM otel_metrics_gauge WHERE MetricHash = $1", hash,
	).Scan(&value); err != nil {
		t.Fatalf("querying otel_metrics_gauge: %v", err)
	}
	if value != 0 {
		t.Errorf("expected Value=0 for nil DataPoint Value oneof, got %f", value)
	}
}

// TestGRPC_UnimplementedMetricType_SilentlyDropped verifies that sending a Histogram metric — a
// type whose insert logic is not yet implemented — results in a successful RPC with no rows
// written to any table. The current behaviour is a silent drop: no error, no PartialSuccess
// rejection count.
func TestGRPC_UnimplementedMetricType_SilentlyDropped(t *testing.T) {
	store, cleanup := setupClickHouse(t)
	defer cleanup()

	ctx := context.Background()
	if err := store.CreateTables(ctx); err != nil {
		t.Fatalf("creating tables: %v", err)
	}

	client, grpcCleanup := setupGRPCServer(t, store)
	defer grpcCleanup()

	now := uint64(time.Now().UnixNano())
	resp, err := client.Export(ctx, &colmetricspb.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricspb.ResourceMetrics{
			{
				Resource: &resourcepb.Resource{
					Attributes: []*commonpb.KeyValue{
						{Key: "service.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "histogram-svc"}}},
					},
				},
				ScopeMetrics: []*metricspb.ScopeMetrics{
					{
						Scope: &commonpb.InstrumentationScope{Name: "histogram-scope"},
						Metrics: []*metricspb.Metric{
							{
								Name: "unimplemented.histogram",
								Data: &metricspb.Metric_Histogram{
									Histogram: &metricspb.Histogram{
										AggregationTemporality: metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE,
										DataPoints: []*metricspb.HistogramDataPoint{
											{
												TimeUnixNano: now,
												Count:        10,
												Sum:          func() *float64 { v := 100.0; return &v }(),
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
		t.Fatalf("Export with Histogram metric returned unexpected error: %v", err)
	}
	if ps := resp.GetPartialSuccess(); ps != nil && ps.GetRejectedDataPoints() > 0 {
		t.Errorf("expected no rejected data points in PartialSuccess, got %d: %s",
			ps.GetRejectedDataPoints(), ps.GetErrorMessage())
	}

	// No rows must appear in any metric or metadata table.
	for _, table := range []string{"otel_metrics_gauge", "otel_metrics_sum", "otel_metrics_histogram", "otel_metrics_metadata"} {
		var count uint64
		if err := store.conn.QueryRow(ctx,
			fmt.Sprintf("SELECT count() FROM %s", table), //nolint:gosec // table name is a hardcoded literal
		).Scan(&count); err != nil {
			t.Fatalf("querying %s: %v", table, err)
		}
		if count != 0 {
			t.Errorf("expected 0 rows in %s after Histogram export, got %d", table, count)
		}
	}
}

// TestGRPC_ValidAndUnimplementedTypeMixed_OnlyValidLands sends a single Export containing both a
// Gauge (implemented) and a Histogram (not yet implemented) under the same resource. The Gauge
// datapoint must land in otel_metrics_gauge; the Histogram must be silently dropped with no error.
func TestGRPC_ValidAndUnimplementedTypeMixed_OnlyValidLands(t *testing.T) {
	store, cleanup := setupClickHouse(t)
	defer cleanup()

	ctx := context.Background()
	if err := store.CreateTables(ctx); err != nil {
		t.Fatalf("creating tables: %v", err)
	}

	client, grpcCleanup := setupGRPCServer(t, store)
	defer grpcCleanup()

	now := uint64(time.Now().UnixNano())
	rm := []*metricspb.ResourceMetrics{
		{
			Resource: &resourcepb.Resource{
				Attributes: []*commonpb.KeyValue{
					{Key: "service.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "mixed-types-svc"}}},
				},
			},
			ScopeMetrics: []*metricspb.ScopeMetrics{
				{
					Scope: &commonpb.InstrumentationScope{Name: "mixed-types-scope"},
					Metrics: []*metricspb.Metric{
						{
							Name: "mixed.types.gauge",
							Data: &metricspb.Metric_Gauge{
								Gauge: &metricspb.Gauge{
									DataPoints: []*metricspb.NumberDataPoint{
										{TimeUnixNano: now, Value: &metricspb.NumberDataPoint_AsDouble{AsDouble: 7.0}},
									},
								},
							},
						},
						{
							Name: "mixed.types.histogram",
							Data: &metricspb.Metric_Histogram{
								Histogram: &metricspb.Histogram{
									AggregationTemporality: metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE,
									DataPoints: []*metricspb.HistogramDataPoint{
										{
											TimeUnixNano: now,
											Count:        5,
											Sum:          func() *float64 { v := 50.0; return &v }(),
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

	_, err := client.Export(ctx, &colmetricspb.ExportMetricsServiceRequest{ResourceMetrics: rm})
	if err != nil {
		t.Fatalf("Export with mixed metric types returned unexpected error: %v", err)
	}

	// The gauge datapoint must be present in otel_metrics_gauge.
	expectedGaugeRows, _ := MapGaugeRows(rm)
	if len(expectedGaugeRows) == 0 {
		t.Fatal("expected mapper to produce a gauge row for the Gauge metric")
	}
	gaugeHash := expectedGaugeRows[0].MetricHash

	var gaugeValue float64
	if err := store.conn.QueryRow(ctx,
		"SELECT Value FROM otel_metrics_gauge WHERE MetricHash = $1", gaugeHash,
	).Scan(&gaugeValue); err != nil {
		t.Fatalf("querying otel_metrics_gauge: %v", err)
	}
	if gaugeValue != 7.0 {
		t.Errorf("expected gauge Value=7.0, got %f", gaugeValue)
	}

	// The histogram table must remain empty — the datapoint was silently dropped.
	var histCount uint64
	if err := store.conn.QueryRow(ctx,
		"SELECT count() FROM otel_metrics_histogram",
	).Scan(&histCount); err != nil {
		t.Fatalf("querying otel_metrics_histogram: %v", err)
	}
	if histCount != 0 {
		t.Errorf("expected 0 rows in otel_metrics_histogram, got %d", histCount)
	}

	// Exactly one metadata row must exist — for the gauge only.
	var metaCount uint64
	if err := store.conn.QueryRow(ctx,
		"SELECT count() FROM otel_metrics_metadata FINAL",
	).Scan(&metaCount); err != nil {
		t.Fatalf("querying otel_metrics_metadata: %v", err)
	}
	if metaCount != 1 {
		t.Errorf("expected exactly 1 metadata row (gauge only), got %d", metaCount)
	}
}

func TestGRPCToClickHouse(t *testing.T) {
	store, cleanup := setupClickHouse(t)
	defer cleanup()

	ctx := context.Background()
	if err := store.CreateTables(ctx); err != nil {
		t.Fatalf("creating tables: %v", err)
	}

	client, grpcCleanup := setupGRPCServer(t, store)
	defer grpcCleanup()

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

	_, err := client.Export(ctx, &colmetricspb.ExportMetricsServiceRequest{
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
