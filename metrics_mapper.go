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

// computeMetricHash returns xxHash64 of the canonical series identity string:
//
//	MetricName|ServiceName|sorted(ResourceAttributes)|sorted(ScopeAttributes)|sorted(Attributes)
//
// Maps are serialized as key=value,key=value,... with keys sorted ascending so that
// the same attribute set always produces the same hash regardless of Go map iteration order.
// MetricType is intentionally excluded — it is a property of the series, not part of its identity.
func computeMetricHash(metricName, svcName string, resourceAttrs, scopeAttrs, attrs map[string]string) uint64 {
	var b strings.Builder
	b.WriteString(metricName)
	b.WriteByte('|')
	b.WriteString(svcName)
	b.WriteByte('|')
	writeMapSorted(&b, resourceAttrs)
	b.WriteByte('|')
	writeMapSorted(&b, scopeAttrs)
	b.WriteByte('|')
	writeMapSorted(&b, attrs)
	return xxhash.Sum64String(b.String())
}

// writeMapSorted writes map entries to b as key=value,key=value,... with keys sorted ascending.
func writeMapSorted(b *strings.Builder, m map[string]string) {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for i, k := range keys {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(m[k])
	}
}

// MapGaugeRows converts ResourceMetrics into slim GaugeRows and their corresponding MetadataRows.
// One MetadataRow is emitted per unique MetricHash — duplicate hashes within the batch are
// collapsed here so the caller receives a pre-deduplicated metadata slice.
func MapGaugeRows(resourceMetrics []*metricspb.ResourceMetrics) ([]GaugeRow, []MetadataRow) {
	var rows []GaugeRow
	var metadata []MetadataRow
	seenHashes := make(map[uint64]struct{})

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
					if _, seen := seenHashes[hash]; !seen {
						seenHashes[hash] = struct{}{}
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
	seenHashes := make(map[uint64]struct{})

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
					if _, seen := seenHashes[hash]; !seen {
						seenHashes[hash] = struct{}{}
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
