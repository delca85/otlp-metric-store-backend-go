package main

import (
	"context"
	"fmt"
	"log/slog"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	"golang.org/x/sync/errgroup"
	grpccodes "google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type metricsServiceServer struct {
	addr  string
	store MetricsStore

	colmetricspb.UnimplementedMetricsServiceServer
}

func newServer(addr string, store MetricsStore) colmetricspb.MetricsServiceServer {
	return &metricsServiceServer{addr: addr, store: store}
}

// validateResourceMetrics returns an InvalidArgument error for any OTLP spec violation found in rm:
//   - metric name must not be empty
//   - every data point's time_unix_nano must be non-zero
//   - Sum metrics must not have AGGREGATION_TEMPORALITY_UNSPECIFIED
func validateResourceMetrics(rm []*metricspb.ResourceMetrics) error {
	for _, r := range rm {
		for _, sm := range r.GetScopeMetrics() {
			for _, metric := range sm.GetMetrics() {
				if metric.GetName() == "" {
					return fmt.Errorf("metric name must not be empty")
				}
				if err := validateMetric(metric); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// validateMetric checks per-metric OTLP constraints beyond the name.
func validateMetric(metric *metricspb.Metric) error {
	name := metric.GetName()
	switch data := metric.GetData().(type) {
	case *metricspb.Metric_Gauge:
		for i, dp := range data.Gauge.GetDataPoints() {
			if dp.GetTimeUnixNano() == 0 {
				return fmt.Errorf("metric %q: data point %d has time_unix_nano=0 (required by OTLP spec)", name, i)
			}
		}
	case *metricspb.Metric_Sum:
		if data.Sum.GetAggregationTemporality() == metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_UNSPECIFIED {
			return fmt.Errorf("sum metric %q: aggregation_temporality must not be UNSPECIFIED", name)
		}
		for i, dp := range data.Sum.GetDataPoints() {
			if dp.GetTimeUnixNano() == 0 {
				return fmt.Errorf("metric %q: data point %d has time_unix_nano=0 (required by OTLP spec)", name, i)
			}
		}
	}
	return nil
}

func (m *metricsServiceServer) Export(ctx context.Context, request *colmetricspb.ExportMetricsServiceRequest) (*colmetricspb.ExportMetricsServiceResponse, error) {
	ctx, span := tracer.Start(ctx, "Export")
	defer span.End()

	slog.DebugContext(ctx, "Received ExportMetricsServiceRequest")
	metricsReceivedCounter.Add(ctx, 1)

	rm := request.GetResourceMetrics()
	if err := validateResourceMetrics(rm); err != nil {
		span.SetStatus(codes.Error, err.Error())
		return nil, status.Error(grpccodes.InvalidArgument, err.Error())
	}

	if m.store != nil {

		gaugeRows, gaugeMetadata := MapGaugeRows(rm)
		sumRows, sumMetadata := MapSumRows(rm)
		span.SetAttributes(
			attribute.Int("metric.gauge.count", len(gaugeRows)),
			attribute.Int("metric.sum.count", len(sumRows)),
		)

		g, gctx := errgroup.WithContext(ctx)
		if len(gaugeRows) > 0 {
			g.Go(func() error { return m.store.InsertGauge(gctx, gaugeRows, gaugeMetadata) })
		}
		if len(sumRows) > 0 {
			g.Go(func() error { return m.store.InsertSum(gctx, sumRows, sumMetadata) })
		}
		if err := g.Wait(); err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			slog.ErrorContext(ctx, "failed to insert metrics", slog.Any("error", err))
			return nil, status.Error(grpccodes.Internal, "failed to store metrics")
		}
	}

	return &colmetricspb.ExportMetricsServiceResponse{}, nil
}
