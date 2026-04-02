package main

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/cespare/xxhash/v2"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
)

// serviceName extracts the service.name from resource attributes, returning "" if not found.
func serviceName(resource *resourcepb.Resource) string {
	if resource == nil {
		return ""
	}
	for _, attr := range resource.GetAttributes() {
		if attr.GetKey() == "service.name" {
			return attr.GetValue().GetStringValue()
		}
	}
	return ""
}

// kvToMap converts a slice of OTLP KeyValue pairs to a Go map.
func kvToMap(attrs []*commonpb.KeyValue) map[string]string {
	m := make(map[string]string, len(attrs))
	for _, kv := range attrs {
		m[kv.GetKey()] = anyValueToString(kv.GetValue())
	}
	return m
}

// anyValueToString converts an OTLP AnyValue to its string representation.
func anyValueToString(v *commonpb.AnyValue) string {
	if v == nil {
		return ""
	}
	switch v.Value.(type) {
	case *commonpb.AnyValue_StringValue:
		return v.GetStringValue()
	case *commonpb.AnyValue_IntValue:
		return fmt.Sprintf("%d", v.GetIntValue())
	case *commonpb.AnyValue_DoubleValue:
		return fmt.Sprintf("%g", v.GetDoubleValue())
	case *commonpb.AnyValue_BoolValue:
		return fmt.Sprintf("%t", v.GetBoolValue())
	default:
		return fmt.Sprintf("%v", v)
	}
}

// nanosToTime converts a uint64 nanoseconds-since-epoch to time.Time.
func nanosToTime(nanos uint64) time.Time {
	return time.Unix(0, int64(nanos))
}

// numberDataPointValue extracts the float64 value from a NumberDataPoint.
func numberDataPointValue(dp *metricspb.NumberDataPoint) float64 {
	switch v := dp.GetValue().(type) {
	case *metricspb.NumberDataPoint_AsDouble:
		return v.AsDouble
	case *metricspb.NumberDataPoint_AsInt:
		return float64(v.AsInt)
	default:
		return 0
	}
}

// metadataKey uniquely identifies a metadata row per (hash, day) — matching the
// AggregatingMergeTree ORDER BY (TimeUnix Date, MetricName, MetricHash).
// Using date granularity ensures one metadata row is emitted per day per series,
// which is the dedup unit the engine merges on.
type metadataKey struct {
	hash uint64
	date time.Time // truncated to UTC midnight
}

// truncateToDay returns t truncated to midnight UTC, matching the Date column
// granularity used by otel_metrics_metadata's TimeUnix column and ORDER BY key.
func truncateToDay(t time.Time) time.Time {
	y, m, d := t.UTC().Date()
	return time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
}

// Separator bytes used in the canonical hash string. ASCII control characters are chosen
// because they cannot appear in valid UTF-8 OTLP strings (metric names, attribute keys/values).
// Using distinct bytes for each role makes the encoding unambiguous regardless of field content.
const (
	fieldSep = '\x00' // separates top-level identity fields
	entrySep = '\x01' // separates key=value pairs within a map
	kvSep    = '\x02' // separates a key from its value
)

// computeMetricHash returns xxHash64 of the canonical series identity string:
//
//	MetricName\x00ServiceName\x00sorted(ResourceAttributes)\x00sorted(ScopeAttributes)\x00sorted(Attributes)
//
// Maps are serialized as key\x02value\x01key\x02value,... with keys sorted ascending so that
// the same attribute set always produces the same hash regardless of Go map iteration order.
// MetricType is intentionally excluded — it is a property of the series, not part of its identity.
func computeMetricHash(metricName, svcName string, resourceAttrs, scopeAttrs, attrs map[string]string) uint64 {
	var b strings.Builder
	b.WriteString(metricName)
	b.WriteByte(fieldSep)
	b.WriteString(svcName)
	b.WriteByte(fieldSep)
	writeMapSorted(&b, resourceAttrs)
	b.WriteByte(fieldSep)
	writeMapSorted(&b, scopeAttrs)
	b.WriteByte(fieldSep)
	writeMapSorted(&b, attrs)
	return xxhash.Sum64String(b.String())
}

// writeMapSorted writes map entries to b as key\x02value\x01key\x02value,... with keys sorted ascending.
func writeMapSorted(b *strings.Builder, m map[string]string) {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for i, k := range keys {
		if i > 0 {
			b.WriteByte(entrySep)
		}
		b.WriteString(k)
		b.WriteByte(kvSep)
		b.WriteString(m[k])
	}
}

// MapGaugeRows converts ResourceMetrics into slim GaugeRows and their corresponding MetadataRows.
// One MetadataRow is emitted per unique MetricHash — duplicate hashes within the batch are
// collapsed here so the caller receives a pre-deduplicated metadata slice.
func MapGaugeRows(resourceMetrics []*metricspb.ResourceMetrics) ([]GaugeRow, []MetadataRow) {
	var rows []GaugeRow
	var metadata []MetadataRow
	seenHashes := make(map[metadataKey]struct{})

	for _, rm := range resourceMetrics {
		svcName := serviceName(rm.GetResource())
		resAttrs := kvToMap(rm.GetResource().GetAttributes())

		for _, sm := range rm.GetScopeMetrics() {
			scope := sm.GetScope()
			scopeAttrs := kvToMap(scope.GetAttributes())

			for _, metric := range sm.GetMetrics() {
				gauge := metric.GetGauge()
				if gauge == nil {
					continue
				}
				for _, dp := range gauge.GetDataPoints() {
					attrs := kvToMap(dp.GetAttributes())
					hash := computeMetricHash(metric.GetName(), svcName, resAttrs, scopeAttrs, attrs)
					t := nanosToTime(dp.GetTimeUnixNano())

					rows = append(rows, GaugeRow{
						MetricHash:    hash,
						StartTimeUnix: nanosToTime(dp.GetStartTimeUnixNano()),
						TimeUnix:      t,
						Value:         numberDataPointValue(dp),
						Flags:         dp.GetFlags(),
					})
					mKey := metadataKey{hash: hash, date: truncateToDay(t)}
					if _, seen := seenHashes[mKey]; !seen {
						seenHashes[mKey] = struct{}{}
						metadata = append(metadata, MetadataRow{
							TimeUnix:           t,
							MetricHash:         hash,
							MetricName:         metric.GetName(),
							MetricDescription:  metric.GetDescription(),
							MetricUnit:         metric.GetUnit(),
							MetricType:         "Gauge",
							ServiceName:        svcName,
							ResourceAttributes: resAttrs,
							ScopeAttributes:    scopeAttrs,
							Attributes:         attrs,
							ScopeName:          scope.GetName(),
							ScopeVersion:       scope.GetVersion(),
						})
					}
				}
			}
		}
	}
	return rows, metadata
}

// MapSumRows converts ResourceMetrics into slim SumRows and their corresponding MetadataRows.
// One MetadataRow is emitted per unique MetricHash — duplicate hashes within the batch are
// collapsed here so the caller receives a pre-deduplicated metadata slice.
func MapSumRows(resourceMetrics []*metricspb.ResourceMetrics) ([]SumRow, []MetadataRow) {
	var rows []SumRow
	var metadata []MetadataRow
	seenHashes := make(map[metadataKey]struct{})

	for _, rm := range resourceMetrics {
		svcName := serviceName(rm.GetResource())
		resAttrs := kvToMap(rm.GetResource().GetAttributes())

		for _, sm := range rm.GetScopeMetrics() {
			scope := sm.GetScope()
			scopeAttrs := kvToMap(scope.GetAttributes())

			for _, metric := range sm.GetMetrics() {
				sum := metric.GetSum()
				if sum == nil {
					continue
				}
				for _, dp := range sum.GetDataPoints() {
					attrs := kvToMap(dp.GetAttributes())
					hash := computeMetricHash(metric.GetName(), svcName, resAttrs, scopeAttrs, attrs)
					t := nanosToTime(dp.GetTimeUnixNano())

					rows = append(rows, SumRow{
						GaugeRow: GaugeRow{
							MetricHash:    hash,
							StartTimeUnix: nanosToTime(dp.GetStartTimeUnixNano()),
							TimeUnix:      t,
							Value:         numberDataPointValue(dp),
							Flags:         dp.GetFlags(),
						},
						AggregationTemporality: int32(sum.GetAggregationTemporality()),
						IsMonotonic:            sum.GetIsMonotonic(),
					})
					mKey := metadataKey{hash: hash, date: truncateToDay(t)}
					if _, seen := seenHashes[mKey]; !seen {
						seenHashes[mKey] = struct{}{}
						metadata = append(metadata, MetadataRow{
							TimeUnix:           t,
							MetricHash:         hash,
							MetricName:         metric.GetName(),
							MetricDescription:  metric.GetDescription(),
							MetricUnit:         metric.GetUnit(),
							MetricType:         "Sum",
							ServiceName:        svcName,
							ResourceAttributes: resAttrs,
							ScopeAttributes:    scopeAttrs,
							Attributes:         attrs,
							ScopeName:          scope.GetName(),
							ScopeVersion:       scope.GetVersion(),
						})
					}
				}
			}
		}
	}
	return rows, metadata
}
