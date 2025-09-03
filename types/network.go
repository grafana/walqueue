package types

import (
	"context"
	"fmt"
	"net/url"
	"time"

	"github.com/prometheus/common/config"
	promconfig "github.com/prometheus/prometheus/config"
)

type NetworkClient interface {
	Start(ctx context.Context)
	Stop()
	// UpdateConfig is a synchronous call and will only return once the config
	// is applied or an error occurs.
	UpdateConfig(ctx context.Context, cfg ConnectionConfig) (bool, error)
}

// ConnectionConfig holds configuration details for network connections.
// It includes various options such as authentication, timeouts, retry policies,
// batching, and connection management settings.
type ConnectionConfig struct { //nolint:govet // fieldalignment
	// URL is the URL of the Prometheus server.
	URL string
	// BasicAuth holds the username and password for basic HTTP authentication.
	BasicAuth *BasicAuth
	// BearerToken is the bearer token for the Prometheus server.
	BearerToken string
	// UserAgent is the User-Agent header sent to the Prometheus server.
	UserAgent string
	// ProtobufMessage is the Prometheus protobuf message to send.
	ProtobufMessage promconfig.RemoteWriteProtoMsg
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
	// Headers specifies the HTTP headers to be added to all requests
	// sent to the server.
	Headers map[string]string

	// ProxyURL is the URL of the HTTP proxy to use for requests.
	// If empty, no proxy is used.
	ProxyURL string
	// ProxyFromEnvironment determines whether to read proxy configuration from environment
	// variables HTTP_PROXY, HTTPS_PROXY and NO_PROXY.
	// If true, environment proxy settings will be used even if ProxyURL is set.
	ProxyFromEnvironment bool
	// ProxyConnectHeaders specify the headers to send to proxies during CONNECT requests.
	ProxyConnectHeaders map[string]string

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
	// ParallelismConfig determines how many concurrent connections to have.
	Parallelism ParallelismConfig
}
type ParallelismConfig struct {
	// AllowedDrift is the maximum amount of time that is allowed for the Newest Timestamp Serializer - Newest Timestamp Sent via Network before the connections scales up.
	// If Newest TS In Serializer sees 100s and Newest TS Out Network sees 20s then we have a drift of 80s. If AllowedDrift is 60s that would
	// trigger a scaling up event.
	AllowedDrift time.Duration
	// MinimumScaleDownDrift is the amount if we go below that we can scale down. Using the above if In is 100s and Out is 70s and MinimumScaleDownDrift is 30 then we wont scale
	// down even though we are below the 60s. This is to keep the number of connections from flapping. In practice we should consider 30s MinimumScaleDownDrift and 60s AllowedDrift to be a sweet spot
	// for general usage.
	MinimumScaleDownDrift time.Duration
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
	// AllowedNetworkErrorFraction is the fraction of failed network requests that are allowable. This will
	// trigger a decrease in connections if exceeded.
	AllowedNetworkErrorFraction float64
}

// ToPrometheusConfig converts a ConnectionConfig to a config.HTTPClientConfig and returns any error encountered
func (cc ConnectionConfig) ToPrometheusConfig() (config.HTTPClientConfig, error) {
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

	// Configure proxy settings
	if cc.ProxyURL != "" {
		proxyURL, err := url.Parse(cc.ProxyURL)
		if err != nil {
			return cfg, fmt.Errorf("invalid proxy URL %q: %w", cc.ProxyURL, err)
		}
		cfg.ProxyURL = config.URL{URL: proxyURL}
	}
	cfg.ProxyFromEnvironment = cc.ProxyFromEnvironment

	// Set proxy connect headers if provided
	if len(cc.ProxyConnectHeaders) > 0 {
		cfg.ProxyConnectHeader = make(config.ProxyHeader)
		for key, value := range cc.ProxyConnectHeaders {
			cfg.ProxyConnectHeader[key] = []config.Secret{config.Secret(value)}
		}
	}

	return cfg, nil
}

type BasicAuth struct {
	Username string
	Password string
}
