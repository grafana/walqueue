package network

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/walqueue/types"
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
			name: "Invalid certificate",
			tlsConfig: types.ConnectionConfig{
				URL:           server.URL,
				TLSCert:       "invalid cert",
				TLSKey:        "invalid key",
				BatchCount:    10,
				FlushInterval: time.Second,
				Timeout:       time.Second,
				UserAgent:     "test-client",
			},
			wantErr: true,
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
			l, newErr := newLoop(tt.tlsConfig, false, logger, func(s types.NetworkStats) {})

			if tt.wantErr {
				require.Error(t, newErr)
				require.Nil(t, l, "newLoop should return nil for invalid TLS config")
				return
			}
			require.NoError(t, newErr)

			require.NotNil(t, l, "newLoop should not return nil for valid TLS config")

			// Create a test series for sending
			l.series = append(l.series, createSeries(t))

			// Test connection by sending a request
			ctx := context.Background()
			result := l.send(ctx, 0)
			if !tt.wantErr {
				require.True(t, result.successful, "request should be successful")
				require.NoError(t, result.err, "request should not return error")
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
		{
			name: "Partial TLS config (cert only)",
			tlsConfig: types.ConnectionConfig{
				URL:           "https://example.com",
				TLSCert:       "cert",
				BatchCount:    10,
				FlushInterval: time.Second,
				Timeout:       time.Second,
				UserAgent:     "test-client",
			},
			wantLoop: true,
		},
		{
			name: "Invalid CA cert with valid cert/key",
			tlsConfig: types.ConnectionConfig{
				URL:           "https://example.com",
				TLSCert:       "valid cert",
				TLSKey:        "valid key",
				TLSCACert:     "invalid ca",
				BatchCount:    10,
				FlushInterval: time.Second,
				Timeout:       time.Second,
				UserAgent:     "test-client",
			},
			wantLoop: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l, newErr := newLoop(tt.tlsConfig, false, logger, func(s types.NetworkStats) {})
			if tt.wantLoop {
				require.NoError(t, newErr)
				require.NotNil(t, l, "newLoop should return a valid loop")
			} else {
				require.Error(t, newErr)
				require.Nil(t, l, "newLoop should return nil for invalid config")
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
