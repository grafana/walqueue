package v3

import (
	"context"
	"time"
)

// ConnectionConfig represents the configuration for network connections
type ConnectionConfig struct {
	Endpoint       string
	Headers        map[string]string
	Timeout        time.Duration
	RetryConfig    RetryConfig
	BatchConfig    BatchConfig
	TLSConfig      TLSConfig
	Authentication AuthConfig
}

// RetryConfig configures retry behavior
type RetryConfig struct {
	MaxRetries     int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	BackoffFactor  float64
}

// BatchConfig configures batching behavior
type BatchConfig struct {
	MaxSize       int
	MaxItems      int
	FlushInterval time.Duration
}

// TLSConfig configures TLS settings
type TLSConfig struct {
	Enabled            bool
	InsecureSkipVerify bool
	CertFile           string
	KeyFile            string
	CAFile             string
	ServerName         string
}

// AuthConfig configures authentication
type AuthConfig struct {
	Type     string // e.g., "bearer", "basic", "oauth2"
	Token    string
	Username string
	Password string
}

// NetworkClient defines the interface for sending telemetry data
type NetworkClient interface {
	// SendTelemetry sends any type of telemetry data
	SendTelemetry(ctx context.Context, datum telemetryDatum) error

	// UpdateConfig updates the client configuration
	UpdateConfig(ctx context.Context, cfg ConnectionConfig) error

	// Start starts the client
	Start(ctx context.Context)

	// Stop stops the client
	Stop()
}

// NetworkStats tracks network-related statistics
type NetworkStats struct {
	BytesSent        uint64
	ItemsSent        uint64
	Retries          uint64
	Errors           uint64
	LastSendDuration time.Duration
	LastError        error
}

// StatsCollector collects statistics for different signal types
type StatsCollector interface {
	// RecordMetricStats records metric-related statistics
	RecordMetricStats(stats NetworkStats)

	// RecordTraceStats records trace-related statistics
	RecordTraceStats(stats NetworkStats)

	// RecordLogStats records log-related statistics
	RecordLogStats(stats NetworkStats)
}
