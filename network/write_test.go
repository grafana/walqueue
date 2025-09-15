package network

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/golang/snappy"
	"github.com/grafana/walqueue/types"
	"github.com/prometheus/common/config"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/require"
)

func TestTLSConnection(t *testing.T) {
	// Generate test certificates
	caCert, _, serverCert, serverKey := generateTestCertificates(t)

	// Create TLS config for test server
	cert, err := tls.X509KeyPair(serverCert, serverKey)
	require.NoError(t, err)

	// Create CA cert pool for client verification
	clientCAs := x509.NewCertPool()
	clientCAs.AppendCertsFromPEM(caCert)

	serverTLSConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientAuth:   tls.VerifyClientCertIfGiven,
		ClientCAs:    clientCAs,
	}

	// Create test server with TLS
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify request headers
		contentType := r.Header.Get("Content-Type")
		contentEncoding := r.Header.Get("Content-Encoding")
		if contentType != "application/x-protobuf" || contentEncoding != "snappy" {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprintf(w, "unexpected content type or encoding: %s, %s", contentType, contentEncoding)
			return
		}

		defer r.Body.Close()
		data, err := io.ReadAll(r.Body)
		require.NoError(t, err)

		data, err = snappy.Decode(nil, data)
		require.NoError(t, err)

		var req prompb.WriteRequest
		err = req.Unmarshal(data)
		require.NoError(t, err)
		ts := req.GetTimeseries()
		require.True(t, len(ts) > 0)
		w.WriteHeader(http.StatusOK)
	}))
	server.TLS = serverTLSConfig
	server.StartTLS()
	t.Cleanup(func() {
		server.Close()
	})
	tests := []struct {
		name      string
		tlsConfig types.ConnectionConfig
		wantErr   bool
	}{
		{
			name: "Valid TLS configuration with CA cert",
			tlsConfig: types.ConnectionConfig{
				URL:           server.URL,
				TLSCert:       string(serverCert),
				TLSKey:        string(serverKey),
				TLSCACert:     string(caCert),
				BatchCount:    10,
				FlushInterval: time.Second,
				Timeout:       time.Second,
				UserAgent:     "test-client",
			},
			wantErr: false,
		},
		{
			name: "Skip verify without CA cert",
			tlsConfig: types.ConnectionConfig{
				URL:                server.URL,
				TLSCert:            string(serverCert),
				TLSKey:             string(serverKey),
				InsecureSkipVerify: true,
				BatchCount:         10,
				FlushInterval:      time.Second,
				Timeout:            time.Second,
				UserAgent:          "test-client",
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := log.NewNopLogger()
			var httpOpts []config.HTTPClientOption
			cfg, err := tt.tlsConfig.ToPrometheusConfig()
			require.NoError(t, err)
			httpClient, err := config.NewClientFromConfig(cfg, "remote_write", httpOpts...)
			require.NoError(t, err)
			l, newErr := newWrite(tt.tlsConfig, logger, func(r sendResult) {}, httpClient)
			if tt.wantErr {
				require.Error(t, newErr)
				require.Nil(t, l, "newWrite should return nil for invalid TLS config")
				return
			}
			require.NoError(t, newErr)

			require.NotNil(t, l, "newWrite should not return nil for valid TLS config")

			// Create a test series for sending
			pending := []types.MetricDatum{createSeries(1, t)}

			// Test connection by sending a request
			ctx := context.Background()
			snappyBuf, _, werr := buildWriteRequest[types.MetricDatum](pending, nil, nil)
			require.NoError(t, werr)
			result := l.send(snappyBuf, ctx, 0)
			if !tt.wantErr {
				require.NoError(t, result.err, "request should not return error")
				require.True(t, result.successful, "request should be successful")
			}
		})
	}
}

func TestTLSConfigValidation(t *testing.T) {
	logger := log.NewNopLogger()
	tests := []struct {
		name      string
		tlsConfig types.ConnectionConfig
		wantLoop  bool
	}{
		{
			name: "No TLS config",
			tlsConfig: types.ConnectionConfig{
				URL:           "http://example.com",
				BatchCount:    10,
				FlushInterval: time.Second,
				Timeout:       time.Second,
				UserAgent:     "test-client",
			},
			wantLoop: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var httpOpts []config.HTTPClientOption
			cfg, err := tt.tlsConfig.ToPrometheusConfig()
			require.NoError(t, err)
			httpClient, err := config.NewClientFromConfig(cfg, "remote_write", httpOpts...)
			require.NoError(t, err)
			l, newErr := newWrite(tt.tlsConfig, logger, func(r sendResult) {}, httpClient)
			if tt.wantLoop {
				require.NoError(t, newErr)
				require.NotNil(t, l, "newWrite should return a valid loop")
			} else {
				require.Error(t, newErr)
				require.Nil(t, l, "newWrite should return nil for invalid config")
			}
		})
	}
}

// generateTestCertificates creates a CA certificate and a server certificate for testing
func generateTestCertificates(t *testing.T) (caCert, caKey, serverCert, serverKey []byte) {
	// Generate CA key pair
	caPrivKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	serialNumberLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	serialNumber, err := rand.Int(rand.Reader, serialNumberLimit)
	require.NoError(t, err)

	// Create CA certificate
	ca := &x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			CommonName: "Test CA",
		},
		NotBefore:             time.Now().Add(-1 * 2 * time.Second),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
		DNSNames:              []string{"localhost", "127.0.0.1"},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
	}

	// Create CA certificate
	caBytes, err := x509.CreateCertificate(rand.Reader, ca, ca, &caPrivKey.PublicKey, caPrivKey)
	require.NoError(t, err)

	// Generate server key pair
	serverPrivKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	// Create server certificate
	server := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject: pkix.Name{
			CommonName: "localhost",
		},
		NotBefore:             time.Now().Add(-1 * 2 * time.Second),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		DNSNames:              []string{"localhost", "127.0.0.1"},
		BasicConstraintsValid: true,
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
	}

	// Create server certificate signed by CA
	serverBytes, err := x509.CreateCertificate(rand.Reader, server, ca, &serverPrivKey.PublicKey, caPrivKey)
	require.NoError(t, err)

	// Encode certificates and keys to PEM
	caCertPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caBytes})
	caPrivateBytes, err := x509.MarshalPKCS8PrivateKey(caPrivKey)
	require.NoError(t, err)
	caKeyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: caPrivateBytes})
	serverCertPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: serverBytes})
	serverPrivateBytes, err := x509.MarshalPKCS8PrivateKey(serverPrivKey)
	require.NoError(t, err)
	serverKeyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: serverPrivateBytes})

	return caCertPEM, caKeyPEM, serverCertPEM, serverKeyPEM
}

// Note: We're using createSeries, randSeq and the metric type from manager_test.go
func TestCustomHeaders(t *testing.T) {
	// Create a test HTTP server that inspects request headers
	headerReceived := make(chan string, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Check for our custom header
		customValue := r.Header.Get("X-Custom-Header")
		headerReceived <- customValue

		// Verify other request headers as usual
		contentType := r.Header.Get("Content-Type")
		contentEncoding := r.Header.Get("Content-Encoding")
		if contentType != "application/x-protobuf" || contentEncoding != "snappy" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		defer r.Body.Close()
		data, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		data, err = snappy.Decode(nil, data)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		var req prompb.WriteRequest
		err = req.Unmarshal(data)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		ts := req.GetTimeseries()
		if len(ts) == 0 {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	logger := log.NewNopLogger()

	// Test with custom header
	t.Run("With custom header", func(t *testing.T) {
		config := types.ConnectionConfig{
			URL:       server.URL,
			UserAgent: "test-client",
			Timeout:   time.Second,
			Headers: map[string]string{
				"X-Custom-Header": "test-value",
			},
		}

		// Create HTTP client directly rather than through ToPrometheusConfig
		// as the Headers field doesn't map to Prometheus config
		httpClient := &http.Client{
			Timeout: config.Timeout,
		}

		l, err := newWrite(config, logger, func(r sendResult) {}, httpClient)
		require.NoError(t, err)
		require.NotNil(t, l)

		// Create a test series for sending
		pending := []types.MetricDatum{createSeries(1, t)}

		// Send the request
		ctx := context.Background()
		snappyBuf, _, werr := buildWriteRequest[types.MetricDatum](pending, nil, nil)
		require.NoError(t, werr)
		result := l.send(snappyBuf, ctx, 0)
		require.True(t, result.successful, "request should be successful")
		require.NoError(t, result.err, "request should not return error")

		// Verify the header was received by the test server
		select {
		case value := <-headerReceived:
			require.Equal(t, "test-value", value, "Custom header should be sent with request")
		case <-time.After(time.Second):
			t.Fatal("Timed out waiting for request with header")
		}
	})

	// Test with multiple custom headers
	t.Run("With multiple custom headers", func(t *testing.T) {
		multiHeadersReceived := make(map[string]string)
		multiHeaderServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Collect all headers
			multiHeadersReceived["X-Custom-1"] = r.Header.Get("X-Custom-1")
			multiHeadersReceived["X-Custom-2"] = r.Header.Get("X-Custom-2")
			multiHeadersReceived["X-Custom-3"] = r.Header.Get("X-Custom-3")

			// Always respond with success for this test
			w.WriteHeader(http.StatusOK)
		}))
		defer multiHeaderServer.Close()

		config := types.ConnectionConfig{
			URL:       multiHeaderServer.URL,
			UserAgent: "test-client",
			Timeout:   time.Second,
			Headers: map[string]string{
				"X-Custom-1": "value1",
				"X-Custom-2": "value2",
				"X-Custom-3": "value3",
			},
		}

		httpClient := &http.Client{
			Timeout: config.Timeout,
		}

		l, err := newWrite(config, logger, func(r sendResult) {}, httpClient)
		require.NoError(t, err)
		require.NotNil(t, l)

		// Create and send a minimal request - for this test we just care about headers
		ctx := context.Background()
		pending := []types.MetricDatum{createSeries(1, t)}
		snappyBuf, _, werr := buildWriteRequest[types.MetricDatum](pending, nil, nil)
		require.NoError(t, werr)
		result := l.send(snappyBuf, ctx, 0)
		require.True(t, result.successful)

		// Verify all headers were sent
		require.Equal(t, "value1", multiHeadersReceived["X-Custom-1"])
		require.Equal(t, "value2", multiHeadersReceived["X-Custom-2"])
		require.Equal(t, "value3", multiHeadersReceived["X-Custom-3"])
	})

	// Test header precedence - built-in headers should override custom headers
	t.Run("Header precedence", func(t *testing.T) {
		headersReceived := make(map[string]string)
		precedenceServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Collect critical headers
			headersReceived["Content-Type"] = r.Header.Get("Content-Type")
			headersReceived["Content-Encoding"] = r.Header.Get("Content-Encoding")
			headersReceived["User-Agent"] = r.Header.Get("User-Agent")

			// Always respond with success for this test
			w.WriteHeader(http.StatusOK)
		}))
		defer precedenceServer.Close()

		config := types.ConnectionConfig{
			URL:       precedenceServer.URL,
			UserAgent: "test-client",
			Timeout:   time.Second,
			// Override some critical headers
			Headers: map[string]string{
				"Content-Type": "text/plain",
				"User-Agent":   "override-agent",
			},
		}

		httpClient := &http.Client{
			Timeout: config.Timeout,
		}

		l, err := newWrite(config, logger, func(r sendResult) {}, httpClient)
		require.NoError(t, err)

		ctx := context.Background()
		pending := []types.MetricDatum{createSeries(1, t)}
		snappyBuf, _, werr := buildWriteRequest[types.MetricDatum](pending, nil, nil)
		require.NoError(t, werr)
		result := l.send(snappyBuf, ctx, 0)
		require.True(t, result.successful)

		// Built-in headers should override custom headers with the same names
		// since they're set after the custom headers in the code
		require.Equal(t, "application/x-protobuf", headersReceived["Content-Type"])
		require.Equal(t, "snappy", headersReceived["Content-Encoding"])
		require.Equal(t, "test-client", headersReceived["User-Agent"])
	})
}
