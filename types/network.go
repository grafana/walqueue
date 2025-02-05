package types

import (
	"context"
	"reflect"
	"time"

	"github.com/prometheus/common/config"
)

type NetworkClient interface {
	Start(ctx context.Context)
	Stop()
	// SendSeries will block if the network caches are full.
	SendSeries(ctx context.Context, d MetricDatum) error
	// SendMetadata will block if the network caches are full.
	SendMetadata(ctx context.Context, d MetadataDatum) error
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

	// TLSCert is the PEM-encoded certificate string for TLS client authentication
	TLSCert string
	// TLSKey is the PEM-encoded private key string for TLS client authentication
	TLSKey string
	// TLSCACert is the PEM-encoded CA certificate string for server verification
	TLSCACert string
	// InsecureSkipVerify controls whether the client verifies the server's certificate chain and host name
	InsecureSkipVerify bool
	// UseRoundRobin
	UseRoundRobin bool
	// ParralelismConfig determines how many concurrent connections to have.
	Parralelism ParralelismConfig
}
type ParralelismConfig struct {
	// AllowedDriftSeconds is the maximum amount that is allowed before the connections scales up.
	AllowedDriftSeconds int64
	// MinimumScaleDownDriftSeconds is the amount if we go below that we can scale down.
	MinimumScaleDownDriftSeconds int64
	// MaxConnections is the maximum number of concurrent connections to use.
	MaxConnections uint
	// MinConnections is the minimum number of concurrent connections to use.
	MinConnections uint
	// ResetInterval is how long to keep network successes and errors in memory for calculations.
	ResetInterval time.Duration
	// Lookback is how far to lookback for previous desired values. This is to prevent flapping.
	// In a situation where in the past 5 minutes you have desired [1,2,1,1] and desired is 1 it will
	// choose 2 since that was the greatest. This determines how fast you can scale down.
	Lookback time.Duration
	// CheckInterval is how long to check for desired values.
	CheckInterval time.Duration
	// AllowedNetworkErrorPercent is the percentage of failed network requests that are allowable. This will
	// trigger a scaled down if exceeded.
	AllowedNetworkErrorPercent float64
}
type Parralelism struct {
}

// ToPrometheusConfig converts a ConnectionConfig to a config.HTTPClientConfig
func (cc ConnectionConfig) ToPrometheusConfig() config.HTTPClientConfig {
	var cfg config.HTTPClientConfig
	if cc.BasicAuth != nil {
		cfg.BasicAuth = &config.BasicAuth{
			Username: cc.BasicAuth.Username,
			Password: config.Secret(cc.BasicAuth.Password),
		}
	}
	if len(cc.BearerToken) > 0 {
		cfg.BearerToken = config.Secret(cc.BearerToken)
	}
	if cc.TLSCert != "" {
		cfg.TLSConfig.Cert = cc.TLSCert
	}
	if cc.TLSKey != "" {
		cfg.TLSConfig.Key = config.Secret(cc.TLSKey)
	}
	if cc.TLSCACert != "" {
		cfg.TLSConfig.CA = cc.TLSCACert
	}
	cfg.TLSConfig.InsecureSkipVerify = cc.InsecureSkipVerify
	return cfg
}

type BasicAuth struct {
	Username string
	Password string
}

func (cc ConnectionConfig) Equals(bb ConnectionConfig) bool {
	return reflect.DeepEqual(cc, bb)
}
