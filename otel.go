package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/log/global"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.40.0"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	grpcinsecure "google.golang.org/grpc/credentials/insecure"
)

// version is set at build time via -ldflags="-X main.version=<tag>".
// Falls back to "dev" when not set.
var version = "dev"

// setupOTelSDK bootstraps the OpenTelemetry pipeline.
// Exporters are configured via the standard OTLP environment variables:
//
//	OTEL_EXPORTER_OTLP_ENDPOINT        — gRPC collector endpoint (default: localhost:4317)
//	OTEL_EXPORTER_OTLP_TRACES_ENDPOINT — override for traces
//	OTEL_EXPORTER_OTLP_METRICS_ENDPOINT — override for metrics
//	OTEL_EXPORTER_OTLP_LOGS_ENDPOINT   — override for logs
//
// Set OTEL_SDK_DISABLED=true to skip initialisation entirely (useful in local dev
// without a collector). Logs are still written to stderr via the JSON handler.
//
// If it does not return an error, make sure to call shutdown for proper cleanup.
func setupOTelSDK(ctx context.Context) (shutdown func(context.Context) error, err error) {
	if strings.EqualFold(os.Getenv("OTEL_SDK_DISABLED"), "true") {
		return func(context.Context) error { return nil }, nil
	}

	res, err := newResource()
	if err != nil {
		return nil, fmt.Errorf("creating otel resource: %w", err)
	}

	var shutdownFuncs []func(context.Context) error

	// shutdown calls cleanup functions registered via shutdownFuncs.
	// The errors from the calls are joined.
	// Each registered cleanup will be invoked once.
	shutdown = func(ctx context.Context) error {
		var err error
		for _, fn := range shutdownFuncs {
			err = errors.Join(err, fn(ctx))
		}
		shutdownFuncs = nil
		return err
	}

	// handleErr calls shutdown for cleanup and makes sure that all errors are returned.
	handleErr := func(inErr error) {
		err = errors.Join(inErr, shutdown(ctx))
	}

	// Set up propagator.
	prop := newPropagator()
	otel.SetTextMapPropagator(prop)

	// Set up trace provider.
	tracerProvider, err := newTraceProvider(ctx, res)
	if err != nil {
		handleErr(err)
		return
	}
	shutdownFuncs = append(shutdownFuncs, tracerProvider.Shutdown)
	otel.SetTracerProvider(tracerProvider)

	// Set up meter provider.
	meterProvider, err := newMeterProvider(ctx, res)
	if err != nil {
		handleErr(err)
		return
	}
	shutdownFuncs = append(shutdownFuncs, meterProvider.Shutdown)
	otel.SetMeterProvider(meterProvider)

	// Set up logger provider.
	loggerProvider, err := newLoggerProvider(ctx, res)
	if err != nil {
		handleErr(err)
		return
	}
	shutdownFuncs = append(shutdownFuncs, loggerProvider.Shutdown)
	global.SetLoggerProvider(loggerProvider)

	return
}

// newResource merges service identity attributes on top of the SDK-detected defaults
// (host.name, process.pid, os.type, etc.). The service version is injected at build
// time via -ldflags="-X main.version=<tag>".
func newResource() (*resource.Resource, error) {
	return resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String("otlp-metrics-processor-backend"),
			semconv.ServiceVersionKey.String(version),
		),
	)
}

// newSampler returns a parent-based sampler so that downstream spans respect the
// sampling decision of the caller. The root sampler ratio is read from
// OTEL_TRACES_SAMPLER_ARG (0.0–1.0); when absent or invalid it defaults to 1.0
// (sample everything), which is safe for low-traffic services.
func newSampler() trace.Sampler {
	if v := os.Getenv("OTEL_TRACES_SAMPLER_ARG"); v != "" {
		if ratio, err := strconv.ParseFloat(v, 64); err == nil {
			return trace.ParentBased(trace.TraceIDRatioBased(ratio))
		}
	}
	return trace.ParentBased(trace.AlwaysSample())
}

// otlpDialOption returns a gRPC dial option with TLS enabled by default.
// Set OTEL_EXPORTER_OTLP_INSECURE=true to use a plaintext connection (e.g. localhost sidecar).
func otlpDialOption() grpc.DialOption {
	if strings.EqualFold(os.Getenv("OTEL_EXPORTER_OTLP_INSECURE"), "true") {
		return grpc.WithTransportCredentials(grpcinsecure.NewCredentials())
	}
	return grpc.WithTransportCredentials(credentials.NewClientTLSFromCert(nil, ""))
}

func newPropagator() propagation.TextMapPropagator {
	return propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	)
}

func newTraceProvider(ctx context.Context, res *resource.Resource) (*trace.TracerProvider, error) {
	traceExporter, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithDialOption(otlpDialOption()),
		otlptracegrpc.WithTimeout(10*time.Second),
	)
	if err != nil {
		return nil, err
	}

	traceProvider := trace.NewTracerProvider(
		trace.WithResource(res),
		trace.WithSampler(newSampler()),
		trace.WithBatcher(traceExporter,
			trace.WithBatchTimeout(5*time.Second)),
	)
	return traceProvider, nil
}

func newMeterProvider(ctx context.Context, res *resource.Resource) (*metric.MeterProvider, error) {
	metricExporter, err := otlpmetricgrpc.New(ctx,
		otlpmetricgrpc.WithDialOption(otlpDialOption()),
		otlpmetricgrpc.WithTimeout(10*time.Second),
	)
	if err != nil {
		return nil, err
	}

	meterProvider := metric.NewMeterProvider(
		metric.WithResource(res),
		metric.WithReader(metric.NewPeriodicReader(metricExporter,
			metric.WithInterval(time.Minute))),
	)
	return meterProvider, nil
}

func newLoggerProvider(ctx context.Context, res *resource.Resource) (*log.LoggerProvider, error) {
	logExporter, err := otlploggrpc.New(ctx,
		otlploggrpc.WithDialOption(otlpDialOption()),
		otlploggrpc.WithTimeout(10*time.Second),
	)
	if err != nil {
		return nil, err
	}

	loggerProvider := log.NewLoggerProvider(
		log.WithResource(res),
		log.WithProcessor(log.NewBatchProcessor(logExporter)),
	)
	return loggerProvider, nil
}
