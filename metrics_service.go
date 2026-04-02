package main

import (
	"context"
	"log/slog"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
)

type dash0MetricsServiceServer struct {
	addr  string
	store MetricsStore

	colmetricspb.UnimplementedMetricsServiceServer
}

func newServer(addr string, store MetricsStore) colmetricspb.MetricsServiceServer {
	return &dash0MetricsServiceServer{addr: addr, store: store}
}

func (m *dash0MetricsServiceServer) Export(ctx context.Context, request *colmetricspb.ExportMetricsServiceRequest) (*colmetricspb.ExportMetricsServiceResponse, error) {
	ctx, span := tracer.Start(ctx, "Export")
	defer span.End()

	slog.DebugContext(ctx, "Received ExportMetricsServiceRequest")
	metricsReceivedCounter.Add(ctx, 1)

	if m.store != nil {
		rm := request.GetResourceMetrics()

		gaugeRows, gaugeMetadata := MapGaugeRows(rm)
		span.SetAttributes(attribute.Int("metric.gauge.count", len(gaugeRows)))
		if len(gaugeRows) > 0 {
			if err := m.store.InsertGauge(ctx, gaugeRows, gaugeMetadata); err != nil {
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())
				return nil, err
			}
		}

		sumRows, sumMetadata := MapSumRows(rm)
		span.SetAttributes(attribute.Int("metric.sum.count", len(sumRows)))
		if len(sumRows) > 0 {
			if err := m.store.InsertSum(ctx, sumRows, sumMetadata); err != nil {
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())
				return nil, err
			}
		}
	}

	return &colmetricspb.ExportMetricsServiceResponse{}, nil
}
