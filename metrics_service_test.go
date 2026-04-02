package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	grpccodes "google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, 1, store.insertGaugeCalls, "expected InsertGauge called once")
	assert.Equal(t, 0, store.insertSumCalls, "expected InsertSum not called")
	require.Len(t, store.gaugeRows, 1, "expected 1 gauge row forwarded to store")
	assert.Equal(t, 55.5, store.gaugeRows[0].Value)
	require.Len(t, store.gaugeMetadata, 1, "expected 1 metadata row forwarded to store")
	assert.Equal(t, "my-svc", store.gaugeMetadata[0].ServiceName)
}

func TestExport_SumMetric_CallsInsertSum(t *testing.T) {
	store := &mockMetricsStore{}
	srv := newServer("test", store)

	_, err := srv.Export(context.Background(), exportSumRequest("my-svc", "requests.total", 100))

	require.NoError(t, err)
	assert.Equal(t, 1, store.insertSumCalls, "expected InsertSum called once")
	assert.Equal(t, 0, store.insertGaugeCalls, "expected InsertGauge not called")
	require.Len(t, store.sumRows, 1, "expected 1 sum row forwarded to store")
	assert.Equal(t, float64(100), store.sumRows[0].Value)
}

func TestExport_EmptyRequest_NoInsertCalls(t *testing.T) {
	store := &mockMetricsStore{}
	srv := newServer("test", store)

	_, err := srv.Export(context.Background(), &colmetricspb.ExportMetricsServiceRequest{})

	require.NoError(t, err)
	assert.Equal(t, 0, store.insertGaugeCalls, "expected no gauge insert calls for empty request")
	assert.Equal(t, 0, store.insertSumCalls, "expected no sum insert calls for empty request")
}

func TestExport_NilStore_DoesNotPanic(t *testing.T) {
	// When no store is configured the server must handle requests gracefully.
	srv := newServer("test", nil)

	resp, err := srv.Export(context.Background(), exportGaugeRequest("svc", "m", 1.0))

	require.NoError(t, err)
	assert.NotNil(t, resp)
}

func TestExport_InsertGaugeError_ReturnsError(t *testing.T) {
	store := &mockMetricsStore{insertGaugeErr: errors.New("clickhouse unavailable")}
	srv := newServer("test", store)

	_, err := srv.Export(context.Background(), exportGaugeRequest("svc", "m", 1.0))

	require.Error(t, err)
	assert.Equal(t, grpccodes.Internal, status.Code(err))
}

func TestExport_InsertSumError_ReturnsError(t *testing.T) {
	store := &mockMetricsStore{insertSumErr: errors.New("clickhouse unavailable")}
	srv := newServer("test", store)

	_, err := srv.Export(context.Background(), exportSumRequest("svc", "m", 1.0))

	require.Error(t, err)
	assert.Equal(t, grpccodes.Internal, status.Code(err))
}

func TestExport_EmptyMetricName_ReturnsInvalidArgument(t *testing.T) {
	store := &mockMetricsStore{}
	srv := newServer("test", store)

	_, err := srv.Export(context.Background(), exportGaugeRequest("svc", "", 1.0))

	require.Error(t, err)
	assert.Equal(t, grpccodes.InvalidArgument, status.Code(err))
	assert.Equal(t, 0, store.insertGaugeCalls, "InsertGauge must not be called when validation fails")
}

func TestExport_EmptyMetricNameInBatch_ReturnsInvalidArgument(t *testing.T) {
	// Validates that an empty name is caught even when it appears after valid metrics in the batch.
	store := &mockMetricsStore{}
	srv := newServer("test", store)

	now := uint64(time.Now().UnixNano())
	req := &colmetricspb.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricspb.ResourceMetrics{
			{
				ScopeMetrics: []*metricspb.ScopeMetrics{
					{
						Metrics: []*metricspb.Metric{
							{Name: "valid.metric", Data: &metricspb.Metric_Gauge{Gauge: &metricspb.Gauge{
								DataPoints: []*metricspb.NumberDataPoint{{TimeUnixNano: now, Value: &metricspb.NumberDataPoint_AsDouble{AsDouble: 1.0}}},
							}}},
							{Name: "", Data: &metricspb.Metric_Gauge{Gauge: &metricspb.Gauge{
								DataPoints: []*metricspb.NumberDataPoint{{TimeUnixNano: now, Value: &metricspb.NumberDataPoint_AsDouble{AsDouble: 2.0}}},
							}}},
						},
					},
				},
			},
		},
	}

	_, err := srv.Export(context.Background(), req)

	require.Error(t, err)
	assert.Equal(t, grpccodes.InvalidArgument, status.Code(err))
	assert.Equal(t, 0, store.insertGaugeCalls, "InsertGauge must not be called when validation fails")
}

func TestExport_GaugeZeroTimeUnixNano_ReturnsInvalidArgument(t *testing.T) {
	store := &mockMetricsStore{}
	srv := newServer("test", store)

	req := &colmetricspb.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricspb.ResourceMetrics{
			{ScopeMetrics: []*metricspb.ScopeMetrics{
				{Metrics: []*metricspb.Metric{
					{Name: "cpu.usage", Data: &metricspb.Metric_Gauge{Gauge: &metricspb.Gauge{
						DataPoints: []*metricspb.NumberDataPoint{
							{TimeUnixNano: 0, Value: &metricspb.NumberDataPoint_AsDouble{AsDouble: 1.0}},
						},
					}}},
				}},
			}},
		},
	}

	_, err := srv.Export(context.Background(), req)

	require.Error(t, err)
	assert.Equal(t, grpccodes.InvalidArgument, status.Code(err))
	assert.Equal(t, 0, store.insertGaugeCalls, "InsertGauge must not be called when validation fails")
}

func TestExport_SumZeroTimeUnixNano_ReturnsInvalidArgument(t *testing.T) {
	store := &mockMetricsStore{}
	srv := newServer("test", store)

	req := &colmetricspb.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricspb.ResourceMetrics{
			{ScopeMetrics: []*metricspb.ScopeMetrics{
				{Metrics: []*metricspb.Metric{
					{Name: "requests.total", Data: &metricspb.Metric_Sum{Sum: &metricspb.Sum{
						AggregationTemporality: metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE,
						DataPoints: []*metricspb.NumberDataPoint{
							{TimeUnixNano: 0, Value: &metricspb.NumberDataPoint_AsDouble{AsDouble: 1.0}},
						},
					}}},
				}},
			}},
		},
	}

	_, err := srv.Export(context.Background(), req)

	require.Error(t, err)
	assert.Equal(t, grpccodes.InvalidArgument, status.Code(err))
	assert.Equal(t, 0, store.insertSumCalls, "InsertSum must not be called when validation fails")
}

func TestExport_SumUnspecifiedAggregationTemporality_ReturnsInvalidArgument(t *testing.T) {
	store := &mockMetricsStore{}
	srv := newServer("test", store)

	now := uint64(time.Now().UnixNano())
	req := &colmetricspb.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricspb.ResourceMetrics{
			{ScopeMetrics: []*metricspb.ScopeMetrics{
				{Metrics: []*metricspb.Metric{
					{Name: "requests.total", Data: &metricspb.Metric_Sum{Sum: &metricspb.Sum{
						AggregationTemporality: metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_UNSPECIFIED,
						DataPoints: []*metricspb.NumberDataPoint{
							{TimeUnixNano: now, Value: &metricspb.NumberDataPoint_AsDouble{AsDouble: 1.0}},
						},
					}}},
				}},
			}},
		},
	}

	_, err := srv.Export(context.Background(), req)

	require.Error(t, err)
	assert.Equal(t, grpccodes.InvalidArgument, status.Code(err))
	assert.Equal(t, 0, store.insertSumCalls, "InsertSum must not be called when validation fails")
}

func TestExport_SumDeltaTemporality_Accepted(t *testing.T) {
	store := &mockMetricsStore{}
	srv := newServer("test", store)

	_, err := srv.Export(context.Background(), exportSumRequest("svc", "requests.total", 1.0))
	require.NoError(t, err, "expected valid sum (CUMULATIVE) to be accepted")

	now := uint64(time.Now().UnixNano())
	deltaReq := &colmetricspb.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricspb.ResourceMetrics{
			{ScopeMetrics: []*metricspb.ScopeMetrics{
				{Metrics: []*metricspb.Metric{
					{Name: "requests.total", Data: &metricspb.Metric_Sum{Sum: &metricspb.Sum{
						AggregationTemporality: metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_DELTA,
						DataPoints: []*metricspb.NumberDataPoint{
							{TimeUnixNano: now, Value: &metricspb.NumberDataPoint_AsDouble{AsDouble: 1.0}},
						},
					}}},
				}},
			}},
		},
	}
	_, err = srv.Export(context.Background(), deltaReq)
	require.NoError(t, err, "expected valid sum (DELTA) to be accepted")
}

func TestExport_MetadataHashMatchesDatapointHash(t *testing.T) {
	store := &mockMetricsStore{}
	srv := newServer("test", store)

	_, err := srv.Export(context.Background(), exportGaugeRequest("svc", "cpu.usage", 1.0))
	require.NoError(t, err)

	assert.Equal(t, store.gaugeRows[0].MetricHash, store.gaugeMetadata[0].MetricHash,
		"GaugeRow.MetricHash must match MetadataRow.MetricHash — join key mismatch")
}
