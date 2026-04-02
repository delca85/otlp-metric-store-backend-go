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
	"time"

	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
)

var (
	listenAddr            = flag.String("listenAddr", "localhost:4317", "The listen address")
	maxReceiveMessageSize = flag.Int("maxReceiveMessageSize", 16777216, "The max message size in bytes the server can receive")
	shutdownTimeout       = flag.Duration("shutdown-timeout", 30*time.Second, "Maximum time to wait for in-flight RPCs to complete on shutdown")

	// TLS flags. When both are provided the server uses TLS; otherwise it falls back to insecure.
	tlsCertFile = flag.String("tls-cert-file", "", "Path to the TLS certificate file (PEM)")
	tlsKeyFile  = flag.String("tls-key-file", "", "Path to the TLS private key file (PEM)")

	// ClickHouse connection flags. Separate flags are preferred over a single DSN string
	// so that each value can be injected independently from a secrets manager.
	clickhouseAddr            = flag.String("clickhouse-addr", "localhost:9000", "ClickHouse address (host:port)")
	clickhouseDB              = flag.String("clickhouse-db", "default", "ClickHouse database")
	clickhouseUser            = flag.String("clickhouse-user", "default", "ClickHouse username")
	clickhousePassword        = flag.String("clickhouse-password", "", "ClickHouse password")
	clickhouseMaxOpenConns    = flag.Int("clickhouse-max-open-conns", 10, "Maximum number of open ClickHouse connections")
	clickhouseMaxIdleConns    = flag.Int("clickhouse-max-idle-conns", 5, "Maximum number of idle ClickHouse connections")
	clickhouseConnMaxLifetime = flag.Duration("clickhouse-conn-max-lifetime", time.Hour, "Maximum lifetime of a ClickHouse connection")
	clickhouseMaxRetries      = flag.Int("clickhouse-max-retries", 3, "Maximum insert retry attempts on transient ClickHouse errors")
	dataRetentionDays         = flag.Int("data-retention-days", 90, "TTL in days for all metric datapoint and metadata tables")
	logLevel                  = flag.String("log-level", "INFO", "Minimum log level (DEBUG, INFO, WARN, ERROR)")
)

const name = "otlp-metrics-processor-backend"

var (
	meter                  = otel.Meter(name)
	tracer                 trace.Tracer
	metricsReceivedCounter metric.Int64Counter
)

func init() {
	tracer = otel.Tracer(name)

	var err error
	metricsReceivedCounter, err = meter.Int64Counter("otlp.metrics.received",
		metric.WithDescription("The number of metric datapoints received by otlp-metrics-processor-backend"),
		metric.WithUnit("{datapoint}"))
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
	flag.Parse()

	var level slog.Level
	if err = level.UnmarshalText([]byte(*logLevel)); err != nil {
		return fmt.Errorf("invalid --log-level %q: %w", *logLevel, err)
	}
	slog.SetDefault(slog.New(newFanOutHandler(
		slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: level}),
		otelslog.NewHandler(name),
	)))
	slog.Info("Starting application")

	ctx := context.Background()

	// Set up OpenTelemetry.
	otelShutdown, err := setupOTelSDK(ctx)
	if err != nil {
		return
	}
	defer func() {
		err = errors.Join(err, otelShutdown(ctx))
	}()

	store, err := NewClickHouseMetricsStore(ctx, ClickHouseConfig{
		Addr:              *clickhouseAddr,
		Database:          *clickhouseDB,
		Username:          *clickhouseUser,
		Password:          *clickhousePassword,
		MaxOpenConns:      *clickhouseMaxOpenConns,
		MaxIdleConns:      *clickhouseMaxIdleConns,
		ConnMaxLifetime:   *clickhouseConnMaxLifetime,
		MaxRetries:        *clickhouseMaxRetries,
		DataRetentionDays: *dataRetentionDays,
	})
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

	creds, err := resolveTransportCredentials()
	if err != nil {
		return fmt.Errorf("loading TLS credentials: %w", err)
	}

	healthSrv := health.NewServer()

	grpcServer := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		grpc.MaxRecvMsgSize(*maxReceiveMessageSize),
		grpc.Creds(creds),
	)
	colmetricspb.RegisterMetricsServiceServer(grpcServer, newServer(*listenAddr, store))
	grpc_health_v1.RegisterHealthServer(grpcServer, healthSrv)

	healthSrv.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
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
		healthSrv.SetServingStatus("", grpc_health_v1.HealthCheckResponse_NOT_SERVING)
		gracefulStop(grpcServer, *shutdownTimeout)
	case err = <-serverErr:
		return err
	}
	return nil
}

// resolveTransportCredentials returns TLS credentials when both cert and key flags are set,
// and insecure credentials otherwise (with a warning logged).
func resolveTransportCredentials() (credentials.TransportCredentials, error) {
	if *tlsCertFile != "" && *tlsKeyFile != "" {
		creds, err := credentials.NewServerTLSFromFile(*tlsCertFile, *tlsKeyFile)
		if err != nil {
			return nil, err
		}
		slog.Info("TLS enabled", slog.String("cert", *tlsCertFile))
		return creds, nil
	}
	slog.Warn("TLS not configured — using insecure transport. Set --tls-cert-file and --tls-key-file for production use.")
	return insecure.NewCredentials(), nil
}

// fanOutHandler is a slog.Handler that forwards each record to all provided handlers.
// This lets the application write to stderr (always visible) and the OTel log bridge
// (for trace-correlated log export) simultaneously.
type fanOutHandler struct {
	handlers []slog.Handler
}

func newFanOutHandler(handlers ...slog.Handler) slog.Handler {
	return &fanOutHandler{handlers: handlers}
}

func (f *fanOutHandler) Enabled(ctx context.Context, level slog.Level) bool {
	for _, h := range f.handlers {
		if h.Enabled(ctx, level) {
			return true
		}
	}
	return false
}

func (f *fanOutHandler) Handle(ctx context.Context, r slog.Record) error {
	var errs []error
	for _, h := range f.handlers {
		if h.Enabled(ctx, r.Level) {
			if err := h.Handle(ctx, r.Clone()); err != nil {
				errs = append(errs, err)
			}
		}
	}
	return errors.Join(errs...)
}

func (f *fanOutHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	handlers := make([]slog.Handler, len(f.handlers))
	for i, h := range f.handlers {
		handlers[i] = h.WithAttrs(attrs)
	}
	return &fanOutHandler{handlers: handlers}
}

func (f *fanOutHandler) WithGroup(name string) slog.Handler {
	handlers := make([]slog.Handler, len(f.handlers))
	for i, h := range f.handlers {
		handlers[i] = h.WithGroup(name)
	}
	return &fanOutHandler{handlers: handlers}
}

// gracefulStop attempts a graceful gRPC shutdown within the given timeout, then forces a hard stop.
func gracefulStop(srv *grpc.Server, timeout time.Duration) {
	done := make(chan struct{})
	go func() {
		srv.GracefulStop()
		close(done)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	select {
	case <-done:
	case <-ctx.Done():
		slog.Warn("Graceful shutdown timed out, forcing stop", slog.Duration("timeout", timeout))
		srv.Stop()
	}
}
