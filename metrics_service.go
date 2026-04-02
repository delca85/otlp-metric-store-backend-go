package main

import (
	"context"
	"log/slog"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	grpccodes "google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"golang.org/x/sync/errgroup"
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
