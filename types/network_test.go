package types

import (
	"net/url"
	"testing"

	"github.com/prometheus/common/config"
	"github.com/stretchr/testify/require"
)

func TestProxyConfiguration(t *testing.T) {
	tests := []struct {
		name                string
		proxyURL           string
		fromEnv            bool
		proxyConnectHeaders map[string]string
		expectedProxyURL   string
		expectHeaders      bool
		expectError        bool
	}{
		{
			name:              "no proxy",
			proxyURL:          "",
			fromEnv:           false,
			expectedProxyURL:  "",
			expectHeaders:     false,
			expectError:       false,
		},
		{
			name:              "with proxy url",
			proxyURL:          "http://proxy.example.com:8080",
			fromEnv:           false,
			expectedProxyURL:  "http://proxy.example.com:8080",
			expectHeaders:     false,
			expectError:       false,
		},
		{
			name:              "with proxy from environment",
			proxyURL:          "",
			fromEnv:           true,
			expectedProxyURL:  "",
			expectHeaders:     false,
			expectError:       false,
		},
		{
			name:              "with proxy url and environment",
			proxyURL:          "http://proxy.example.com:8080",
			fromEnv:           true,
			expectedProxyURL:  "http://proxy.example.com:8080",
			expectHeaders:     false,
			expectError:       false,
		},
		{
			name:                "with proxy connect headers",
			proxyURL:            "http://proxy.example.com:8080",
			fromEnv:             false,
			proxyConnectHeaders: map[string]string{
				"Proxy-Authorization": "Basic dXNlcjpwYXNz",
				"X-Custom-Header":     "value",
			},
			expectedProxyURL:    "http://proxy.example.com:8080",
			expectHeaders:       true,
			expectError:         false,
		},
		{
			name:                "with invalid proxy url",
			proxyURL:            "http://proxy.example.com\u0000:8080", // Contains a null character
			fromEnv:             false,
			proxyConnectHeaders: nil,
			expectedProxyURL:    "",
			expectHeaders:       false,
			expectError:         true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cc := ConnectionConfig{
				ProxyURL:            tt.proxyURL,
				ProxyFromEnvironment: tt.fromEnv,
				ProxyConnectHeaders:  tt.proxyConnectHeaders,
			}

			cfg, err := cc.ToPrometheusConfig()
			
			if tt.expectError {
				require.Error(t, err, "Expected an error for invalid proxy URL")
				return // No need to check other conditions for error cases
			} else {
				require.NoError(t, err)
			}

			// Check proxy URL
			if tt.expectedProxyURL == "" {
				require.Nil(t, cfg.ProxyURL.URL)
			} else {
				expectedURL, parseErr := url.Parse(tt.expectedProxyURL)
				require.NoError(t, parseErr)
				require.Equal(t, expectedURL.String(), cfg.ProxyURL.URL.String())
			}

			// Check proxy from environment
			require.Equal(t, tt.fromEnv, cfg.ProxyFromEnvironment)

			// Check proxy connect headers
			if tt.expectHeaders {
				require.NotNil(t, cfg.ProxyConnectHeader)
				require.Equal(t, len(tt.proxyConnectHeaders), len(cfg.ProxyConnectHeader))
				
				for key, value := range tt.proxyConnectHeaders {
					secrets, ok := cfg.ProxyConnectHeader[key]
					require.True(t, ok)
					require.Len(t, secrets, 1)
					require.Equal(t, config.Secret(value), secrets[0])
				}
			} else {
				require.Empty(t, cfg.ProxyConnectHeader)
			}
		})
	}
}
