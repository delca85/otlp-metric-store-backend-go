//go:build ignore

// send-test-metric sends a single test OTLP Export request to a running
// instance of the metrics store. It is intentionally excluded from all
// builds (//go:build ignore) and is meant to be run directly:
//
//	go run tools/send-test-metric/main.go
//	go run tools/send-test-metric/main.go -addr localhost:4317
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var addr = flag.String("addr", "localhost:4317", "metrics store gRPC address")

func main() {
	flag.Parse()

	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("connecting to %s: %v", *addr, err)
	}
	defer conn.Close()

	client := colmetricspb.NewMetricsServiceClient(conn)

	now := uint64(time.Now().UnixNano())
	startTime := now - uint64(time.Minute)

	req := &colmetricspb.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricspb.ResourceMetrics{
			{
				Resource: &resourcepb.Resource{
					Attributes: []*commonpb.KeyValue{
						strAttr("service.name", "my-test-service"),
						strAttr("host.name", "local"),
					},
				},
				ScopeMetrics: []*metricspb.ScopeMetrics{
					{
						Scope: &commonpb.InstrumentationScope{
							Name:    "send-test-metric",
							Version: "0.0.1",
						},
						Metrics: []*metricspb.Metric{
							{
								Name:        "cpu.utilization",
								Description: "CPU utilization percentage",
								Unit:        "%",
								Data: &metricspb.Metric_Gauge{
									Gauge: &metricspb.Gauge{
										DataPoints: []*metricspb.NumberDataPoint{
											{
												Attributes:        []*commonpb.KeyValue{strAttr("cpu", "0")},
												StartTimeUnixNano: startTime,
												TimeUnixNano:      now,
												Value:             &metricspb.NumberDataPoint_AsDouble{AsDouble: 42.5},
											},
										},
									},
								},
							},
							{
								Name:        "requests.total",
								Description: "Total number of HTTP requests",
								Unit:        "{requests}",
								Data: &metricspb.Metric_Sum{
									Sum: &metricspb.Sum{
										AggregationTemporality: metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE,
										IsMonotonic:            true,
										DataPoints: []*metricspb.NumberDataPoint{
											{
												Attributes:        []*commonpb.KeyValue{strAttr("method", "GET"), strAttr("status", "200")},
												StartTimeUnixNano: startTime,
												TimeUnixNano:      now,
												Value:             &metricspb.NumberDataPoint_AsInt{AsInt: 1234},
											},
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

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := client.Export(ctx, req)
	if err != nil {
		log.Fatalf("Export: %v", err)
	}

	if ps := resp.GetPartialSuccess(); ps != nil && ps.GetRejectedDataPoints() > 0 {
		fmt.Printf("partial success: %d data points rejected — %s\n", ps.GetRejectedDataPoints(), ps.GetErrorMessage())
	} else {
		fmt.Println("OK — 2 metrics exported (1 gauge, 1 sum)")
	}
}

func strAttr(key, value string) *commonpb.KeyValue {
	return &commonpb.KeyValue{
		Key:   key,
		Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: value}},
	}
}
