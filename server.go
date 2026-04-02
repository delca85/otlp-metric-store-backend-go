package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	listenAddr            = flag.String("listenAddr", "localhost:4317", "The listen address")
	maxReceiveMessageSize = flag.Int("maxReceiveMessageSize", 16777216, "The max message size in bytes the server can receive")

	// ClickHouse connection flags. Separate flags are preferred over a single DSN string
	// so that each value can be injected independently from a secrets manager.
	clickhouseAddr     = flag.String("clickhouse-addr", "localhost:9000", "ClickHouse address (host:port)")
	clickhouseDB       = flag.String("clickhouse-db", "default", "ClickHouse database")
	clickhouseUser     = flag.String("clickhouse-user", "default", "ClickHouse username")
	clickhousePassword = flag.String("clickhouse-password", "", "ClickHouse password")
)

const name = "otlp-metrics-processor-backend"

var (
	meter                  = otel.Meter(name)
	tracer                 trace.Tracer
	logger                 = otelslog.NewLogger(name)
	metricsReceivedCounter metric.Int64Counter
)

func init() {
	tracer = otel.Tracer(name)

	var err error
	metricsReceivedCounter, err = meter.Int64Counter("otlp.metrics.received",
		metric.WithDescription("The number of metrics received by otlp-metrics-processor-backend"),
		metric.WithUnit("{metric}"))
	if err != nil {
		panic(err)
	}
}

func main() {
	if err := run(); err != nil {
		log.Fatalln(err)
	}
}

func run() (err error) {
	slog.SetDefault(logger)
	logger.Info("Starting application")

	ctx := context.Background()

	// Set up OpenTelemetry.
	otelShutdown, err := setupOTelSDK(ctx)
	if err != nil {
		return
	}
	defer func() {
		err = errors.Join(err, otelShutdown(ctx))
	}()

	flag.Parse()

	store, err := NewClickHouseMetricsStore(ctx, *clickhouseAddr, *clickhouseDB, *clickhouseUser, *clickhousePassword)
	if err != nil {
		return fmt.Errorf("connecting to clickhouse at %s: %w", *clickhouseAddr, err)
	}
	defer func() {
		err = errors.Join(err, store.Close())
	}()

	if err = store.CreateTables(ctx); err != nil {
		return fmt.Errorf("creating tables: %w", err)
	}

	slog.Debug("Starting listener", slog.String("listenAddr", *listenAddr))
	listener, err := net.Listen("tcp", *listenAddr)
	if err != nil {
		return err
	}

	grpcServer := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		grpc.MaxRecvMsgSize(*maxReceiveMessageSize),
		grpc.Creds(insecure.NewCredentials()),
	)
	colmetricspb.RegisterMetricsServiceServer(grpcServer, newServer(*listenAddr, store))

	slog.Info("gRPC server listening", slog.String("addr", *listenAddr))

	serverErr := make(chan error, 1)
	go func() {
		serverErr <- grpcServer.Serve(listener)
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGTERM, syscall.SIGINT)

	select {
	case sig := <-quit:
		slog.Info("Shutdown signal received, stopping gRPC server", slog.String("signal", sig.String()))
		grpcServer.GracefulStop()
	case err = <-serverErr:
		return err
	}
	return nil
}
