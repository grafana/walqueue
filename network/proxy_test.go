package network

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/go-kit/log"
	"github.com/grafana/walqueue/types"
	"github.com/stretchr/testify/require"
)

func TestProxyCreation(t *testing.T) {
	// Test that proxy settings are correctly configured in HTTP transport
	t.Run("http client with proxy", func(t *testing.T) {
		config := types.ConnectionConfig{
			URL:      "https://example.com",
			Timeout:  30,
			ProxyURL: "http://proxy.example.com:8080",
			Parallelism: types.ParallelismConfig{
				MinConnections: 1,
				MaxConnections: 1,
			},
			ProxyConnectHeaders: map[string]string{
				"Proxy-Authorization": "Basic dXNlcjpwYXNz",
				"X-Proxy-Custom":      "custom-value",
			},
		}

		// Create HTTP client with proxy configuration
		cfg, err := config.ToPrometheusConfig()
		require.NoError(t, err)

		// Verify proxy URL is set correctly
		expectedURL, err := url.Parse("http://proxy.example.com:8080")
		require.NoError(t, err)
		require.Equal(t, expectedURL.String(), cfg.ProxyURL.String())

		// Verify ProxyConnectHeader is set correctly
		require.Len(t, cfg.ProxyConnectHeader, 2)
		require.Contains(t, cfg.ProxyConnectHeader, "Proxy-Authorization")
		require.Contains(t, cfg.ProxyConnectHeader, "X-Proxy-Custom")

		// Verify ProxyFromEnvironment
		config.ProxyFromEnvironment = true
		cfg, err = config.ToPrometheusConfig()
		require.NoError(t, err)
		require.True(t, cfg.ProxyFromEnvironment)

		// Create a proxy function
		proxyFunc := cfg.Proxy()
		require.NotNil(t, proxyFunc)
	})
}

func TestInvalidProxyURL(t *testing.T) {
	// Test with various invalid proxy URLs
	testCases := []struct {
		name        string
		proxyURL    string
		errorPrefix string
	}{
		{
			name:        "invalid port",
			proxyURL:    "http://proxy.example.com:invalid",
			errorPrefix: "invalid proxy URL",
		},
		{
			name:        "invalid characters in host",
			proxyURL:    "http://proxy with spaces.com:8080",
			errorPrefix: "invalid proxy URL",
		},
		// For a truly invalid URL, we need to use characters that are truly illegal
		{
			name:        "URL with control characters",
			proxyURL:    "http://proxy.example.com\u0000:8080",
			errorPrefix: "invalid proxy URL",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create a manager to test client creation
			logger := log.NewNopLogger()

			// Create a config with the invalid proxy URL
			config := types.ConnectionConfig{
				URL:      "https://example.com",
				Timeout:  30,
				ProxyURL: tc.proxyURL,
				Parallelism: types.ParallelismConfig{
					MinConnections: 1,
					MaxConnections: 1,
				},
			}

			// Try to create a client - should fail with an error
			// Create a simple stats hub for testing
			statshub := &testStatsHub{}
			noopChan := make(chan types.RequestMoreSignals[types.Datum], 1)

			// Use the New function which internally creates an HTTP client
			_, err := New(config, logger, statshub, noopChan)
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.errorPrefix,
				"Expected error to contain '%s' but got: %v", tc.errorPrefix, err)
		})
	}

	// Test that ToPrometheusConfig doesn't fail with invalid proxy URL
	t.Run("ToPrometheusConfig ignores invalid URL", func(t *testing.T) {
		// Test that ToPrometheusConfig returns an error for invalid URL
		t.Run("ToPrometheusConfig returns error for invalid URL", func(t *testing.T) {
			config := types.ConnectionConfig{
				URL:      "https://example.com",
				Timeout:  30,
				ProxyURL: "http://proxy.example.com\u0000:8080", // Null character makes this invalid
				Parallelism: types.ParallelismConfig{
					MinConnections: 1,
					MaxConnections: 1,
				},
			}

			// This should now return an error
			_, err := config.ToPrometheusConfig()
			require.Error(t, err, "Expected error for invalid proxy URL")
			require.Contains(t, err.Error(), "invalid proxy URL")
		})
		config := types.ConnectionConfig{
			URL:      "https://example.com",
			Timeout:  30,
			ProxyURL: "invalid://proxy:with:too:many:colons",
			Parallelism: types.ParallelismConfig{
				MinConnections: 1,
				MaxConnections: 1,
			},
		}

		// This should not panic or error, it just won't set the proxy URL
		cfg, err := config.ToPrometheusConfig()
		require.Error(t, err)
		require.Nil(t, cfg.ProxyURL.URL, "Expected ProxyURL to be nil for invalid URL")
	})
}

func TestProxyFromEnvironment(t *testing.T) {
	// Set environment variables for the test
	t.Setenv("HTTP_PROXY", "http://env-proxy.example.com:8080")
	t.Setenv("HTTPS_PROXY", "http://env-proxy-secure.example.com:8443")
	t.Setenv("NO_PROXY", "localhost,127.0.0.1")

	t.Run("proxy from environment variables", func(t *testing.T) {
		// Create a connection config with ProxyFromEnvironment enabled
		config := types.ConnectionConfig{
			URL:                  "https://example.com",
			Timeout:              30,
			ProxyFromEnvironment: true,
			Parallelism: types.ParallelismConfig{
				MinConnections: 1,
				MaxConnections: 1,
			},
		}

		// Convert to Prometheus config
		cfg, err := config.ToPrometheusConfig()
		require.NoError(t, err)
		require.True(t, cfg.ProxyFromEnvironment)

		// Get the proxy function from the config
		proxyFunc := cfg.Proxy()
		require.NotNil(t, proxyFunc)

		// Test HTTPS request - should use HTTPS_PROXY
		httpsReq, err := http.NewRequest("GET", "https://example.com", nil)
		require.NoError(t, err)
		httpsProxyURL, err := proxyFunc(httpsReq)
		require.NoError(t, err)
		require.NotNil(t, httpsProxyURL, "HTTPS request should use a proxy")
		require.Equal(t, "http://env-proxy-secure.example.com:8443", httpsProxyURL.String())

		// Test HTTP request - should use HTTP_PROXY
		httpReq, err := http.NewRequest("GET", "http://example.com", nil)
		require.NoError(t, err)
		httpProxyURL, err := proxyFunc(httpReq)
		require.NoError(t, err)
		require.NotNil(t, httpProxyURL, "HTTP request should use a proxy")
		require.Equal(t, "http://env-proxy.example.com:8080", httpProxyURL.String())

		// Test no-proxy host - should bypass proxy
		noProxyReq, err := http.NewRequest("GET", "http://localhost", nil)
		require.NoError(t, err)
		noProxyURL, err := proxyFunc(noProxyReq)
		require.NoError(t, err)
		require.Nil(t, noProxyURL, "localhost should not use a proxy due to NO_PROXY setting")
	})

	t.Run("environment proxies take precedence over explicit URL", func(t *testing.T) {
		// Create connection config with both ProxyURL and ProxyFromEnvironment set
		config := types.ConnectionConfig{
			URL:                  "https://example.com",
			Timeout:              30,
			ProxyURL:             "http://explicit-proxy.example.com:9090",
			ProxyFromEnvironment: true,
			Parallelism: types.ParallelismConfig{
				MinConnections: 1,
				MaxConnections: 1,
			},
		}

		// Convert to Prometheus config
		cfg, err := config.ToPrometheusConfig()
		require.NoError(t, err)
		require.True(t, cfg.ProxyFromEnvironment)

		// Get the proxy function
		proxyFunc := cfg.Proxy()
		require.NotNil(t, proxyFunc)

		// Test that environment proxy is used despite having explicit ProxyURL
		req, err := http.NewRequest("GET", "https://example.com", nil)
		require.NoError(t, err)
		proxyURL, err := proxyFunc(req)
		require.NoError(t, err)
		require.NotNil(t, proxyURL)

		// Environment variables should be used (HTTPS_PROXY for https requests)
		require.Equal(t, "http://env-proxy-secure.example.com:8443", proxyURL.String())
	})
}

type testStatsHub struct{}

func (s *testStatsHub) SendSeriesNetworkStats(stats types.NetworkStats)   {}
func (s *testStatsHub) SendSerializerStats(stats types.SerializerStats)   {}
func (s *testStatsHub) SendMetadataNetworkStats(stats types.NetworkStats) {}
func (s *testStatsHub) SendParralelismStats(stats types.ParralelismStats) {}
func (s *testStatsHub) RegisterSeriesNetwork(f func(types.NetworkStats)) types.NotificationRelease {
	return func() {}
}

func (s *testStatsHub) RegisterMetadataNetwork(f func(types.NetworkStats)) types.NotificationRelease {
	return func() {}
}

func (s *testStatsHub) RegisterSerializer(f func(types.SerializerStats)) types.NotificationRelease {
	return func() {}
}

func (s *testStatsHub) RegisterParralelism(f func(types.ParralelismStats)) types.NotificationRelease {
	return func() {}
}
