package types

import (
	"context"
	"reflect"
	"time"
)

type NetworkClient interface {
	Start()
	Stop()
	// SendSeries will block if the network caches are full.
	SendSeries(ctx context.Context, d *Metric) error
	// SendMetadata will block if the network caches are full.
	SendMetadata(ctx context.Context, d *Metric) error
	// UpdateConfig is a synchronous call and will only return once the config
	// is applied or an error occurs.
	UpdateConfig(ctx context.Context, cfg ConnectionConfig) (bool, error)
}

// ConnectionConfig holds configuration details for network connections.
// It includes various options such as authentication, timeouts, retry policies,
// batching, and connection management settings.
type ConnectionConfig struct {
	// URL is the URL of the Prometheus server.
	URL string
	// BasicAuth holds the username and password for basic HTTP authentication.
	BasicAuth *BasicAuth
	// BearerToken is the bearer token for the Prometheus server.
	BearerToken string
	// UserAgent is the User-Agent header sent to the Prometheus server.
	UserAgent string
	// Timeout specifies the duration for which the connection will wait for a response before timing out.
	Timeout time.Duration
	// RetryBackoff is the duration between retries when a network request fails.
	// The next retry will happen after RetryBackoff + (RetryBackoff * attempt number).
	RetryBackoff time.Duration
	// MaxRetryAttempts specifies the maximum number of times a request will be retried
	// if it fails. The next retry will happen after RetryBackoff + (RetryBackoff * attempt number).
	// If this is set to 0, no retries are attempted.
	MaxRetryAttempts uint
	// BatchCount is the number of time series to batch together before sending to the network.
	BatchCount int
	// FlushInterval specifies the duration between each flush of the network
	// buffer. If no data is available, the buffer is not flushed.
	FlushInterval time.Duration
	// ExternalLabels specifies the external labels to be added to all samples
	// sent to the Prometheus server.
	ExternalLabels map[string]string
	// Connections is the number of concurrent connections to use for sending data.
	Connections uint
	// TLSCert is the PEM-encoded certificate string for TLS client authentication
	TLSCert string
	// TLSKey is the PEM-encoded private key string for TLS client authentication
	TLSKey string
	// TLSCACert is the PEM-encoded CA certificate string for server verification
	TLSCACert string
	// InsecureSkipVerify controls whether the client verifies the server's certificate chain and host name
	InsecureSkipVerify bool
}

type BasicAuth struct {
	Username string
	Password string
}

func (cc ConnectionConfig) Equals(bb ConnectionConfig) bool {
	return reflect.DeepEqual(cc, bb)
}
