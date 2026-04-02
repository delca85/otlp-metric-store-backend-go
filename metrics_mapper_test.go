package main

import (
	"strings"
	"testing"
	"time"

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
			if got != c.want {
				t.Errorf("anyValueToString: got %q, want %q", got, c.want)
			}
		})
	}
}

func TestAnyValueToString_UnknownType_DoesNotPanic(t *testing.T) {
	// BytesValue falls through to the default branch; we only assert no panic and non-empty output.
	v := &commonpb.AnyValue{Value: &commonpb.AnyValue_BytesValue{BytesValue: []byte("raw")}}
	got := anyValueToString(v)
	if got == "" {
		t.Error("expected non-empty fallback string for unknown AnyValue type, got empty")
	}
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
			if got != c.want {
				t.Errorf("serviceName: got %q, want %q", got, c.want)
			}
		})
	}
}

// --- computeMetricHash ---

func TestComputeMetricHash_Deterministic(t *testing.T) {
	attrs := map[string]string{"env": "prod", "region": "us-east-1"}
	h1 := computeMetricHash("cpu.usage", "my-service", attrs, nil, nil)
	h2 := computeMetricHash("cpu.usage", "my-service", attrs, nil, nil)
	if h1 != h2 {
		t.Errorf("same inputs produced different hashes: %d vs %d", h1, h2)
	}
}

func TestComputeMetricHash_MapKeyOrderNormalized(t *testing.T) {
	// Two maps with identical contents but different insertion order.
	// Go map iteration is randomized; our sort must normalize this.
	m1 := map[string]string{"a": "1", "b": "2", "c": "3"}
	m2 := map[string]string{"c": "3", "a": "1", "b": "2"}
	h1 := computeMetricHash("m", "svc", m1, nil, nil)
	h2 := computeMetricHash("m", "svc", m2, nil, nil)
	if h1 != h2 {
		t.Errorf("identical attribute maps with different key order produced different hashes: %d vs %d", h1, h2)
	}
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
			t.Errorf("hash collision between %q and %q (hash=%d)", prev, c.label, h)
		}
		seen[h] = c.label
	}
}

func TestComputeMetricHash_NilMapsProduceStableHash(t *testing.T) {
	// Nil maps must not panic and must produce the same hash as empty maps.
	h1 := computeMetricHash("m", "svc", nil, nil, nil)
	h2 := computeMetricHash("m", "svc", map[string]string{}, map[string]string{}, map[string]string{})
	if h1 != h2 {
		t.Errorf("nil and empty maps produced different hashes: %d vs %d", h1, h2)
	}
}

func TestComputeMetricHash_ScopeAndAttrsArePartOfIdentity(t *testing.T) {
	base := computeMetricHash("m", "svc", nil, nil, nil)
	withScope := computeMetricHash("m", "svc", nil, map[string]string{"scope": "v"}, nil)
	withAttrs := computeMetricHash("m", "svc", nil, nil, map[string]string{"attr": "v"})

	if base == withScope {
		t.Error("scope attributes should be part of the hash identity")
	}
	if base == withAttrs {
		t.Error("datapoint attributes should be part of the hash identity")
	}
	if withScope == withAttrs {
		t.Error("scope and datapoint attributes should produce different hashes")
	}
}

// --- writeMapSorted ---

func TestWriteMapSorted_ProducesSortedOutput(t *testing.T) {
	var b strings.Builder
	writeMapSorted(&b, map[string]string{"z": "3", "a": "1", "m": "2"})
	parts := strings.Split(b.String(), string(entrySep))
	if len(parts) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(parts))
	}
	want := []struct{ k, v string }{{"a", "1"}, {"m", "2"}, {"z", "3"}}
	for i, p := range parts {
		kv := strings.SplitN(p, string(kvSep), 2)
		if len(kv) != 2 || kv[0] != want[i].k || kv[1] != want[i].v {
			t.Errorf("entry %d: got %q, want key=%q value=%q", i, p, want[i].k, want[i].v)
		}
	}
}

func TestWriteMapSorted_EmptyMap(t *testing.T) {
	var b strings.Builder
	writeMapSorted(&b, map[string]string{})
	if b.String() != "" {
		t.Errorf("empty map should produce empty string, got %q", b.String())
	}
}

func TestWriteMapSorted_NilMap(t *testing.T) {
	var b strings.Builder
	writeMapSorted(&b, nil) // must not panic
	if b.String() != "" {
		t.Errorf("nil map should produce empty string, got %q", b.String())
	}
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

	if len(rows) != 1 {
		t.Fatalf("expected 1 GaugeRow, got %d", len(rows))
	}
	if len(metadata) != 1 {
		t.Fatalf("expected 1 MetadataRow, got %d", len(metadata))
	}

	r := rows[0]
	m := metadata[0]

	if r.Value != 42.5 {
		t.Errorf("Value: got %f, want 42.5", r.Value)
	}
	if r.Flags != 3 {
		t.Errorf("Flags: got %d, want 3", r.Flags)
	}
	if r.MetricHash == 0 {
		t.Error("MetricHash must be non-zero")
	}
	if r.MetricHash != m.MetricHash {
		t.Errorf("GaugeRow.MetricHash %d does not match MetadataRow.MetricHash %d", r.MetricHash, m.MetricHash)
	}
	if m.MetricName != "cpu.usage" {
		t.Errorf("MetricName: got %q, want %q", m.MetricName, "cpu.usage")
	}
	if m.MetricDescription != "test description" {
		t.Errorf("MetricDescription: got %q, want %q", m.MetricDescription, "test description")
	}
	if m.MetricUnit != "{unit}" {
		t.Errorf("MetricUnit: got %q, want %q", m.MetricUnit, "{unit}")
	}
	if m.MetricType != "Gauge" {
		t.Errorf("MetricType: got %q, want %q", m.MetricType, "Gauge")
	}
	if m.ServiceName != "my-service" {
		t.Errorf("ServiceName: got %q, want %q", m.ServiceName, "my-service")
	}
	if m.ScopeName != "test-scope" {
		t.Errorf("ScopeName: got %q, want %q", m.ScopeName, "test-scope")
	}
	if m.ScopeVersion != "1.0" {
		t.Errorf("ScopeVersion: got %q, want %q", m.ScopeVersion, "1.0")
	}
	if m.Attributes["cpu"] != "0" {
		t.Errorf("Attributes[cpu]: got %q, want %q", m.Attributes["cpu"], "0")
	}
}

func TestMapGaugeRows_EmptyInput(t *testing.T) {
	rows, metadata := MapGaugeRows(nil)
	if len(rows) != 0 {
		t.Errorf("expected 0 rows for nil input, got %d", len(rows))
	}
	if len(metadata) != 0 {
		t.Errorf("expected 0 metadata for nil input, got %d", len(metadata))
	}
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
	if len(rows) != 0 {
		t.Errorf("expected 0 gauge rows from sum metric input, got %d", len(rows))
	}
	if len(metadata) != 0 {
		t.Errorf("expected 0 metadata rows from sum metric input, got %d", len(metadata))
	}
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
	if len(rows) != 3 {
		t.Errorf("expected 3 GaugeRows, got %d", len(rows))
	}
	if len(metadata) != 1 {
		t.Errorf("expected 1 MetadataRow for 3 datapoints of the same series, got %d", len(metadata))
	}
	// All rows for the same series must share the same MetricHash.
	hash := rows[0].MetricHash
	for i, r := range rows {
		if r.MetricHash != hash {
			t.Errorf("row[%d].MetricHash %d differs from row[0].MetricHash %d", i, r.MetricHash, hash)
		}
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

	if len(rows) != 1 {
		t.Fatalf("expected 1 SumRow, got %d", len(rows))
	}
	if len(metadata) != 1 {
		t.Fatalf("expected 1 MetadataRow, got %d", len(metadata))
	}

	r := rows[0]
	m := metadata[0]

	if r.Value != 100 {
		t.Errorf("Value: got %f, want 100", r.Value)
	}
	if r.AggregationTemporality != int32(metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE) {
		t.Errorf("AggregationTemporality: got %d, want %d", r.AggregationTemporality, metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE)
	}
	if !r.IsMonotonic {
		t.Error("IsMonotonic: got false, want true")
	}
	if m.MetricType != "Sum" {
		t.Errorf("MetricType: got %q, want %q", m.MetricType, "Sum")
	}
	if r.MetricHash != m.MetricHash {
		t.Errorf("SumRow.MetricHash %d does not match MetadataRow.MetricHash %d", r.MetricHash, m.MetricHash)
	}
}

func TestMapSumRows_SkipsNonSumMetrics(t *testing.T) {
	now := uint64(time.Now().UnixNano())
	rm := gaugeResourceMetrics("svc", "cpu.usage", 1.0, now)

	rows, metadata := MapSumRows(rm)
	if len(rows) != 0 {
		t.Errorf("expected 0 sum rows from gauge metric input, got %d", len(rows))
	}
	if len(metadata) != 0 {
		t.Errorf("expected 0 metadata rows from gauge metric input, got %d", len(metadata))
	}
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

	if rows1[0].MetricHash != rows2[0].MetricHash {
		t.Errorf("hash should be identical for same series regardless of value/time: %d vs %d",
			rows1[0].MetricHash, rows2[0].MetricHash)
	}
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
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	if rows[0].Value != 42.0 {
		t.Errorf("AsInt value: got %f, want 42.0", rows[0].Value)
	}
}
