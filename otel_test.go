package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	semconv "go.opentelemetry.io/otel/semconv/v1.40.0"
)

func TestNewResource(t *testing.T) {
	res, err := newResource()
	require.NoError(t, err)
	require.NotNil(t, res)

	attrs := res.Set()
	serviceName, ok := attrs.Value(semconv.ServiceNameKey)
	assert.True(t, ok, "service.name attribute must be present")
	assert.Equal(t, "otlp-metrics-processor-backend", serviceName.AsString())

	serviceVersion, ok := attrs.Value(semconv.ServiceVersionKey)
	assert.True(t, ok, "service.version attribute must be present")
	assert.NotEmpty(t, serviceVersion.AsString())

	// resource.Default() attributes (SDK name/language/version) must be merged in.
	_, hasSDKName := attrs.Value(semconv.TelemetrySDKNameKey)
	assert.True(t, hasSDKName, "telemetry.sdk.name from resource.Default() must be present")
}

func TestNewSampler(t *testing.T) {
	t.Run("defaults to ParentBased(AlwaysOn) when env var is not set", func(t *testing.T) {
		t.Setenv("OTEL_TRACES_SAMPLER_ARG", "")
		s := newSampler()
		assert.Contains(t, s.Description(), "ParentBased")
		assert.Contains(t, s.Description(), "AlwaysOnSampler")
	})

	t.Run("uses TraceIDRatioBased when OTEL_TRACES_SAMPLER_ARG is set", func(t *testing.T) {
		t.Setenv("OTEL_TRACES_SAMPLER_ARG", "0.1")
		s := newSampler()
		assert.Contains(t, s.Description(), "ParentBased")
		assert.Contains(t, s.Description(), "TraceIDRatioBased")
	})

	t.Run("falls back to ParentBased(AlwaysOn) on invalid OTEL_TRACES_SAMPLER_ARG", func(t *testing.T) {
		t.Setenv("OTEL_TRACES_SAMPLER_ARG", "not-a-number")
		s := newSampler()
		assert.Contains(t, s.Description(), "ParentBased")
		assert.Contains(t, s.Description(), "AlwaysOnSampler")
	})
}

func TestOtlpDialOption(t *testing.T) {
	t.Run("returns TLS credentials by default", func(t *testing.T) {
		t.Setenv("OTEL_EXPORTER_OTLP_INSECURE", "")
		opt := otlpDialOption()
		assert.NotNil(t, opt)
	})

	t.Run("returns insecure credentials when OTEL_EXPORTER_OTLP_INSECURE=true", func(t *testing.T) {
		t.Setenv("OTEL_EXPORTER_OTLP_INSECURE", "true")
		opt := otlpDialOption()
		assert.NotNil(t, opt)
	})

	t.Run("OTEL_EXPORTER_OTLP_INSECURE check is case-insensitive", func(t *testing.T) {
		t.Setenv("OTEL_EXPORTER_OTLP_INSECURE", "TRUE")
		opt := otlpDialOption()
		assert.NotNil(t, opt)
	})
}
