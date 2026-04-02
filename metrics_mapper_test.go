package main

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
)

// --- anyValueToString ---

func TestAnyValueToString_Types(t *testing.T) {
	cases := []struct {
		name string
		v    *commonpb.AnyValue
		want string
	}{
		{"nil", nil, ""},
		{"string", &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "hello"}}, "hello"},
		{"int", &commonpb.AnyValue{Value: &commonpb.AnyValue_IntValue{IntValue: 42}}, "42"},
		{"double", &commonpb.AnyValue{Value: &commonpb.AnyValue_DoubleValue{DoubleValue: 3.14}}, "3.14"},
		{"bool_true", &commonpb.AnyValue{Value: &commonpb.AnyValue_BoolValue{BoolValue: true}}, "true"},
		{"bool_false", &commonpb.AnyValue{Value: &commonpb.AnyValue_BoolValue{BoolValue: false}}, "false"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := anyValueToString(c.v)
			assert.Equal(t, c.want, got)
		})
	}
}

func TestAnyValueToString_UnknownType_DoesNotPanic(t *testing.T) {
	// BytesValue falls through to the default branch; we only assert no panic and non-empty output.
	v := &commonpb.AnyValue{Value: &commonpb.AnyValue_BytesValue{BytesValue: []byte("raw")}}
	got := anyValueToString(v)
	assert.NotEmpty(t, got, "expected non-empty fallback string for unknown AnyValue type")
}

// --- serviceName ---

func TestServiceName_EdgeCases(t *testing.T) {
	cases := []struct {
		name     string
		resource *resourcepb.Resource
		want     string
	}{
		{"nil resource", nil, ""},
		{"no attributes", &resourcepb.Resource{}, ""},
		{"missing service.name key", &resourcepb.Resource{
			Attributes: []*commonpb.KeyValue{
				{Key: "host.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "host-1"}}},
			},
		}, ""},
		{"service.name present", &resourcepb.Resource{
			Attributes: []*commonpb.KeyValue{
				{Key: "service.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "my-svc"}}},
			},
		}, "my-svc"},
		// GetStringValue() returns "" when the underlying value is not a string.
		{"service.name is non-string (int)", &resourcepb.Resource{
			Attributes: []*commonpb.KeyValue{
				{Key: "service.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_IntValue{IntValue: 42}}},
			},
		}, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := serviceName(c.resource)
			assert.Equal(t, c.want, got)
		})
	}
}

// --- computeMetricHash ---

func TestComputeMetricHash_Deterministic(t *testing.T) {
	attrs := map[string]string{"env": "prod", "region": "us-east-1"}
	h1 := computeMetricHash("cpu.usage", "my-service", attrs, nil, nil)
	h2 := computeMetricHash("cpu.usage", "my-service", attrs, nil, nil)
	assert.Equal(t, h1, h2)
}

func TestComputeMetricHash_MapKeyOrderNormalized(t *testing.T) {
	// Two maps with identical contents but different insertion order.
	// Go map iteration is randomized; our sort must normalize this.
	m1 := map[string]string{"a": "1", "b": "2", "c": "3"}
	m2 := map[string]string{"c": "3", "a": "1", "b": "2"}
	h1 := computeMetricHash("m", "svc", m1, nil, nil)
	h2 := computeMetricHash("m", "svc", m2, nil, nil)
	assert.Equal(t, h1, h2, "identical attribute maps with different key order should produce the same hash")
}

func TestComputeMetricHash_DistinctInputsDistinctHashes(t *testing.T) {
	cases := []struct {
		label string
		name  string
		svc   string
		res   map[string]string
	}{
		{"base", "cpu", "svc-a", nil},
		{"diff service", "cpu", "svc-b", nil},
		{"diff metric name", "mem", "svc-a", nil},
		{"diff resource attr", "cpu", "svc-a", map[string]string{"k": "v"}},
	}

	seen := make(map[uint64]string)
	for _, c := range cases {
		h := computeMetricHash(c.name, c.svc, c.res, nil, nil)
		if prev, exists := seen[h]; exists {
			assert.Failf(t, "hash collision", "between %q and %q (hash=%d)", prev, c.label, h)
		}
		seen[h] = c.label
	}
}

func TestComputeMetricHash_NilMapsProduceStableHash(t *testing.T) {
	// Nil maps must not panic and must produce the same hash as empty maps.
	h1 := computeMetricHash("m", "svc", nil, nil, nil)
	h2 := computeMetricHash("m", "svc", map[string]string{}, map[string]string{}, map[string]string{})
	assert.Equal(t, h1, h2, "nil and empty maps should produce the same hash")
}

func TestComputeMetricHash_ScopeAndAttrsArePartOfIdentity(t *testing.T) {
	base := computeMetricHash("m", "svc", nil, nil, nil)
	withScope := computeMetricHash("m", "svc", nil, map[string]string{"scope": "v"}, nil)
	withAttrs := computeMetricHash("m", "svc", nil, nil, map[string]string{"attr": "v"})

	assert.NotEqual(t, base, withScope, "scope attributes should be part of the hash identity")
	assert.NotEqual(t, base, withAttrs, "datapoint attributes should be part of the hash identity")
	assert.NotEqual(t, withScope, withAttrs, "scope and datapoint attributes should produce different hashes")
}

// --- writeMapSorted ---

func TestWriteMapSorted_ProducesSortedOutput(t *testing.T) {
	var b strings.Builder
	writeMapSorted(&b, map[string]string{"z": "3", "a": "1", "m": "2"})
	parts := strings.Split(b.String(), string(entrySep))
	require.Len(t, parts, 3, "expected 3 entries")
	want := []struct{ k, v string }{{"a", "1"}, {"m", "2"}, {"z", "3"}}
	for i, p := range parts {
		kv := strings.SplitN(p, string(kvSep), 2)
		require.Lenf(t, kv, 2, "entry %d: invalid format %q", i, p)
		assert.Equal(t, want[i].k, kv[0], "entry %d key", i)
		assert.Equal(t, want[i].v, kv[1], "entry %d value", i)
	}
}

func TestWriteMapSorted_EmptyMap(t *testing.T) {
	var b strings.Builder
	writeMapSorted(&b, map[string]string{})
	assert.Empty(t, b.String(), "empty map should produce empty string")
}

func TestWriteMapSorted_NilMap(t *testing.T) {
	var b strings.Builder
	writeMapSorted(&b, nil) // must not panic
	assert.Empty(t, b.String(), "nil map should produce empty string")
}

// --- MapGaugeRows ---

func gaugeResourceMetrics(svcName, metricName string, value float64, nowNano uint64) []*metricspb.ResourceMetrics {
	return []*metricspb.ResourceMetrics{
		{
			Resource: &resourcepb.Resource{
				Attributes: []*commonpb.KeyValue{
					{Key: "service.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: svcName}}},
				},
			},
			ScopeMetrics: []*metricspb.ScopeMetrics{
				{
					Scope: &commonpb.InstrumentationScope{Name: "test-scope", Version: "1.0"},
					Metrics: []*metricspb.Metric{
						{
							Name:        metricName,
							Description: "test description",
							Unit:        "{unit}",
							Data: &metricspb.Metric_Gauge{
								Gauge: &metricspb.Gauge{
									DataPoints: []*metricspb.NumberDataPoint{
										{
											Attributes:        []*commonpb.KeyValue{{Key: "cpu", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "0"}}}},
											StartTimeUnixNano: nowNano - uint64(time.Minute),
											TimeUnixNano:      nowNano,
											Value:             &metricspb.NumberDataPoint_AsDouble{AsDouble: value},
											Flags:             3,
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

func TestMapGaugeRows_FieldMapping(t *testing.T) {
	now := uint64(time.Now().UnixNano())
	rm := gaugeResourceMetrics("my-service", "cpu.usage", 42.5, now)

	rows, metadata := MapGaugeRows(rm)

	require.Len(t, rows, 1, "expected 1 GaugeRow")
	require.Len(t, metadata, 1, "expected 1 MetadataRow")

	r := rows[0]
	m := metadata[0]

	assert.Equal(t, 42.5, r.Value)
	assert.Equal(t, uint32(3), r.Flags)
	assert.NotZero(t, r.MetricHash, "MetricHash must be non-zero")
	assert.Equal(t, r.MetricHash, m.MetricHash, "GaugeRow.MetricHash must match MetadataRow.MetricHash")
	assert.Equal(t, "cpu.usage", m.MetricName)
	assert.Equal(t, "test description", m.MetricDescription)
	assert.Equal(t, "{unit}", m.MetricUnit)
	assert.Equal(t, "Gauge", m.MetricType)
	assert.Equal(t, "my-service", m.ServiceName)
	assert.Equal(t, "test-scope", m.ScopeName)
	assert.Equal(t, "1.0", m.ScopeVersion)
	assert.Equal(t, "0", m.Attributes["cpu"])
}

func TestMapGaugeRows_EmptyInput(t *testing.T) {
	rows, metadata := MapGaugeRows(nil)
	assert.Empty(t, rows, "expected 0 rows for nil input")
	assert.Empty(t, metadata, "expected 0 metadata for nil input")
}

func TestMapGaugeRows_SkipsNonGaugeMetrics(t *testing.T) {
	rm := []*metricspb.ResourceMetrics{
		{
			Resource: &resourcepb.Resource{
				Attributes: []*commonpb.KeyValue{
					{Key: "service.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "svc"}}},
				},
			},
			ScopeMetrics: []*metricspb.ScopeMetrics{
				{
					Metrics: []*metricspb.Metric{
						// Sum metric — must be skipped by MapGaugeRows.
						{
							Name: "requests.total",
							Data: &metricspb.Metric_Sum{
								Sum: &metricspb.Sum{
									DataPoints: []*metricspb.NumberDataPoint{
										{TimeUnixNano: uint64(time.Now().UnixNano()), Value: &metricspb.NumberDataPoint_AsDouble{AsDouble: 1}},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	rows, metadata := MapGaugeRows(rm)
	assert.Empty(t, rows, "expected 0 gauge rows from sum metric input")
	assert.Empty(t, metadata, "expected 0 metadata rows from sum metric input")
}

func TestMapGaugeRows_MultipleDataPoints(t *testing.T) {
	now := uint64(time.Now().UnixNano())
	rm := []*metricspb.ResourceMetrics{
		{
			Resource: &resourcepb.Resource{
				Attributes: []*commonpb.KeyValue{
					{Key: "service.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "svc"}}},
				},
			},
			ScopeMetrics: []*metricspb.ScopeMetrics{
				{
					Metrics: []*metricspb.Metric{
						{
							Name: "cpu.usage",
							Data: &metricspb.Metric_Gauge{
								Gauge: &metricspb.Gauge{
									DataPoints: []*metricspb.NumberDataPoint{
										{TimeUnixNano: now, Value: &metricspb.NumberDataPoint_AsDouble{AsDouble: 1.0}},
										{TimeUnixNano: now + 1, Value: &metricspb.NumberDataPoint_AsDouble{AsDouble: 2.0}},
										{TimeUnixNano: now + 2, Value: &metricspb.NumberDataPoint_AsDouble{AsDouble: 3.0}},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	rows, metadata := MapGaugeRows(rm)
	assert.Len(t, rows, 3, "expected 3 GaugeRows")
	assert.Len(t, metadata, 1, "expected 1 MetadataRow for 3 datapoints of the same series")
	// All rows for the same series must share the same MetricHash.
	hash := rows[0].MetricHash
	for i, r := range rows {
		assert.Equalf(t, hash, r.MetricHash, "row[%d].MetricHash differs from row[0].MetricHash", i)
	}
}

// --- MapSumRows ---

func TestMapSumRows_FieldMapping(t *testing.T) {
	now := uint64(time.Now().UnixNano())
	rm := []*metricspb.ResourceMetrics{
		{
			Resource: &resourcepb.Resource{
				Attributes: []*commonpb.KeyValue{
					{Key: "service.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "my-service"}}},
				},
			},
			ScopeMetrics: []*metricspb.ScopeMetrics{
				{
					Scope: &commonpb.InstrumentationScope{Name: "scope"},
					Metrics: []*metricspb.Metric{
						{
							Name: "http.requests",
							Data: &metricspb.Metric_Sum{
								Sum: &metricspb.Sum{
									AggregationTemporality: metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE,
									IsMonotonic:            true,
									DataPoints: []*metricspb.NumberDataPoint{
										{
											TimeUnixNano: now,
											Value:        &metricspb.NumberDataPoint_AsDouble{AsDouble: 100},
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

	rows, metadata := MapSumRows(rm)

	require.Len(t, rows, 1, "expected 1 SumRow")
	require.Len(t, metadata, 1, "expected 1 MetadataRow")

	r := rows[0]
	m := metadata[0]

	assert.Equal(t, float64(100), r.Value)
	assert.Equal(t, int32(metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE), r.AggregationTemporality)
	assert.True(t, r.IsMonotonic, "IsMonotonic should be true")
	assert.Equal(t, "Sum", m.MetricType)
	assert.Equal(t, r.MetricHash, m.MetricHash, "SumRow.MetricHash must match MetadataRow.MetricHash")
}

func TestMapSumRows_SkipsNonSumMetrics(t *testing.T) {
	now := uint64(time.Now().UnixNano())
	rm := gaugeResourceMetrics("svc", "cpu.usage", 1.0, now)

	rows, metadata := MapSumRows(rm)
	assert.Empty(t, rows, "expected 0 sum rows from gauge metric input")
	assert.Empty(t, metadata, "expected 0 metadata rows from gauge metric input")
}

// TestMapGaugeRows_HashConsistency verifies that the hash is purely a function of the
// identity fields (name, service, attributes) and does not depend on value or timestamp.
func TestMapGaugeRows_HashIndependentOfValueAndTime(t *testing.T) {
	mkRM := func(value float64, timeNano uint64) []*metricspb.ResourceMetrics {
		return gaugeResourceMetrics("svc", "cpu.usage", value, timeNano)
	}
	now := uint64(time.Now().UnixNano())

	rows1, _ := MapGaugeRows(mkRM(1.0, now))
	rows2, _ := MapGaugeRows(mkRM(999.0, now+1000000000))

	assert.Equal(t, rows1[0].MetricHash, rows2[0].MetricHash,
		"hash should be identical for same series regardless of value/time")
}

// TestMapGaugeRows_IntValueDataPoint verifies AsInt datapoints are converted to float64 correctly.
func TestMapGaugeRows_IntValueDataPoint(t *testing.T) {
	now := uint64(time.Now().UnixNano())
	rm := []*metricspb.ResourceMetrics{
		{
			Resource: &resourcepb.Resource{},
			ScopeMetrics: []*metricspb.ScopeMetrics{
				{
					Metrics: []*metricspb.Metric{
						{
							Name: "counter",
							Data: &metricspb.Metric_Gauge{
								Gauge: &metricspb.Gauge{
									DataPoints: []*metricspb.NumberDataPoint{
										{
											TimeUnixNano: now,
											Value:        &metricspb.NumberDataPoint_AsInt{AsInt: 42},
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

	rows, _ := MapGaugeRows(rm)
	require.Len(t, rows, 1, "expected 1 row")
	assert.Equal(t, float64(42), rows[0].Value, "AsInt value should be converted to float64")
}
