package main

import (
	"context"
	"errors"
	"testing"
	"time"

	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
)

// mockMetricsStore records the arguments passed to InsertGauge and InsertSum so tests
// can assert on them without a real ClickHouse connection.
type mockMetricsStore struct {
	gaugeRows     []GaugeRow
	gaugeMetadata []MetadataRow
	sumRows       []SumRow
	sumMetadata   []MetadataRow

	insertGaugeCalls int
	insertSumCalls   int

	insertGaugeErr error
	insertSumErr   error
}

func (m *mockMetricsStore) CreateTables(_ context.Context) error { return nil }
func (m *mockMetricsStore) Close() error                         { return nil }

func (m *mockMetricsStore) InsertGauge(_ context.Context, rows []GaugeRow, metadata []MetadataRow) error {
	m.insertGaugeCalls++
	m.gaugeRows = rows
	m.gaugeMetadata = metadata
	return m.insertGaugeErr
}

func (m *mockMetricsStore) InsertSum(_ context.Context, rows []SumRow, metadata []MetadataRow) error {
	m.insertSumCalls++
	m.sumRows = rows
	m.sumMetadata = metadata
	return m.insertSumErr
}

// --- helpers ---

func exportGaugeRequest(svcName, metricName string, value float64) *colmetricspb.ExportMetricsServiceRequest {
	now := uint64(time.Now().UnixNano())
	return &colmetricspb.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricspb.ResourceMetrics{
			{
				Resource: &resourcepb.Resource{
					Attributes: []*commonpb.KeyValue{
						{Key: "service.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: svcName}}},
					},
				},
				ScopeMetrics: []*metricspb.ScopeMetrics{
					{
						Metrics: []*metricspb.Metric{
							{
								Name: metricName,
								Data: &metricspb.Metric_Gauge{
									Gauge: &metricspb.Gauge{
										DataPoints: []*metricspb.NumberDataPoint{
											{
												TimeUnixNano: now,
												Value:        &metricspb.NumberDataPoint_AsDouble{AsDouble: value},
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
	}
}

func exportSumRequest(svcName, metricName string, value float64) *colmetricspb.ExportMetricsServiceRequest {
	now := uint64(time.Now().UnixNano())
	return &colmetricspb.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricspb.ResourceMetrics{
			{
				Resource: &resourcepb.Resource{
					Attributes: []*commonpb.KeyValue{
						{Key: "service.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: svcName}}},
					},
				},
				ScopeMetrics: []*metricspb.ScopeMetrics{
					{
						Metrics: []*metricspb.Metric{
							{
								Name: metricName,
								Data: &metricspb.Metric_Sum{
									Sum: &metricspb.Sum{
										IsMonotonic:            true,
										AggregationTemporality: metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE,
										DataPoints: []*metricspb.NumberDataPoint{
											{
												TimeUnixNano: now,
												Value:        &metricspb.NumberDataPoint_AsDouble{AsDouble: value},
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
	}
}

// --- Export tests ---

func TestExport_GaugeMetric_CallsInsertGauge(t *testing.T) {
	store := &mockMetricsStore{}
	srv := newServer("test", store)

	resp, err := srv.Export(context.Background(), exportGaugeRequest("my-svc", "cpu.usage", 55.5))

	if err != nil {
		t.Fatalf("Export returned unexpected error: %v", err)
	}
	if resp == nil {
		t.Fatal("Export returned nil response")
	}
	if store.insertGaugeCalls != 1 {
		t.Errorf("expected InsertGauge called once, got %d", store.insertGaugeCalls)
	}
	if store.insertSumCalls != 0 {
		t.Errorf("expected InsertSum not called, got %d calls", store.insertSumCalls)
	}
	if len(store.gaugeRows) != 1 {
		t.Fatalf("expected 1 gauge row forwarded to store, got %d", len(store.gaugeRows))
	}
	if store.gaugeRows[0].Value != 55.5 {
		t.Errorf("gauge row Value: got %f, want 55.5", store.gaugeRows[0].Value)
	}
	if len(store.gaugeMetadata) != 1 {
		t.Errorf("expected 1 metadata row forwarded to store, got %d", len(store.gaugeMetadata))
	}
	if store.gaugeMetadata[0].ServiceName != "my-svc" {
		t.Errorf("metadata ServiceName: got %q, want %q", store.gaugeMetadata[0].ServiceName, "my-svc")
	}
}

func TestExport_SumMetric_CallsInsertSum(t *testing.T) {
	store := &mockMetricsStore{}
	srv := newServer("test", store)

	_, err := srv.Export(context.Background(), exportSumRequest("my-svc", "requests.total", 100))

	if err != nil {
		t.Fatalf("Export returned unexpected error: %v", err)
	}
	if store.insertSumCalls != 1 {
		t.Errorf("expected InsertSum called once, got %d", store.insertSumCalls)
	}
	if store.insertGaugeCalls != 0 {
		t.Errorf("expected InsertGauge not called, got %d calls", store.insertGaugeCalls)
	}
	if len(store.sumRows) != 1 {
		t.Fatalf("expected 1 sum row forwarded to store, got %d", len(store.sumRows))
	}
	if store.sumRows[0].Value != 100 {
		t.Errorf("sum row Value: got %f, want 100", store.sumRows[0].Value)
	}
}

func TestExport_EmptyRequest_NoInsertCalls(t *testing.T) {
	store := &mockMetricsStore{}
	srv := newServer("test", store)

	_, err := srv.Export(context.Background(), &colmetricspb.ExportMetricsServiceRequest{})

	if err != nil {
		t.Fatalf("Export returned unexpected error: %v", err)
	}
	if store.insertGaugeCalls != 0 || store.insertSumCalls != 0 {
		t.Errorf("expected no insert calls for empty request, got gauge=%d sum=%d",
			store.insertGaugeCalls, store.insertSumCalls)
	}
}

func TestExport_NilStore_DoesNotPanic(t *testing.T) {
	// When no store is configured the server must handle requests gracefully.
	srv := newServer("test", nil)

	resp, err := srv.Export(context.Background(), exportGaugeRequest("svc", "m", 1.0))

	if err != nil {
		t.Fatalf("Export with nil store returned error: %v", err)
	}
	if resp == nil {
		t.Error("Export with nil store returned nil response")
	}
}

func TestExport_InsertGaugeError_ReturnsError(t *testing.T) {
	sentinel := errors.New("clickhouse unavailable")
	store := &mockMetricsStore{insertGaugeErr: sentinel}
	srv := newServer("test", store)

	_, err := srv.Export(context.Background(), exportGaugeRequest("svc", "m", 1.0))

	if err == nil {
		t.Fatal("expected error from Export when InsertGauge fails, got nil")
	}
	if !errors.Is(err, sentinel) {
		t.Errorf("expected sentinel error, got %v", err)
	}
}

func TestExport_InsertSumError_ReturnsError(t *testing.T) {
	sentinel := errors.New("clickhouse unavailable")
	store := &mockMetricsStore{insertSumErr: sentinel}
	srv := newServer("test", store)

	_, err := srv.Export(context.Background(), exportSumRequest("svc", "m", 1.0))

	if err == nil {
		t.Fatal("expected error from Export when InsertSum fails, got nil")
	}
	if !errors.Is(err, sentinel) {
		t.Errorf("expected sentinel error, got %v", err)
	}
}

func TestExport_MetadataHashMatchesDatapointHash(t *testing.T) {
	store := &mockMetricsStore{}
	srv := newServer("test", store)

	_, err := srv.Export(context.Background(), exportGaugeRequest("svc", "cpu.usage", 1.0))
	if err != nil {
		t.Fatalf("Export error: %v", err)
	}

	if store.gaugeRows[0].MetricHash != store.gaugeMetadata[0].MetricHash {
		t.Errorf("GaugeRow.MetricHash %d != MetadataRow.MetricHash %d — join key mismatch",
			store.gaugeRows[0].MetricHash, store.gaugeMetadata[0].MetricHash)
	}
}
