package prometheus

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang/snappy"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/prompb"
	"go.uber.org/atomic"

	"github.com/go-kit/log"
	"github.com/grafana/walqueue/types"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"
)

func TestQueue_Appender(t *testing.T) {
	logger := log.NewNopLogger()
	// Test cases
	tests := []struct {
		name        string
		testFunc    func(t *testing.T, ctx context.Context, app storage.Appender)
		metricCount int32
	}{
		{
			name: "append single metric",
			testFunc: func(t *testing.T, ctx context.Context, app storage.Appender) {
				lbls := labels.FromStrings("__name__", "test_metric", "label1", "value1")
				_, err := app.Append(0, lbls, time.Now().UnixMilli(), 42.0)
				require.NoError(t, err)
				require.NoError(t, app.Commit())
			},
			metricCount: 1,
		},
		{
			name: "append multiple metrics",
			testFunc: func(t *testing.T, ctx context.Context, app storage.Appender) {
				lbls1 := labels.FromStrings("__name__", "test_metric1", "label1", "value1")
				lbls2 := labels.FromStrings("__name__", "test_metric2", "label2", "value2")

				_, err := app.Append(0, lbls1, time.Now().UnixMilli(), 42.0)
				require.NoError(t, err)
				_, err = app.Append(0, lbls2, time.Now().UnixMilli(), 84.0)
				require.NoError(t, err)
				require.NoError(t, app.Commit())
			},
			metricCount: 2,
		},
		{
			name: "append metric with TTL",
			testFunc: func(t *testing.T, ctx context.Context, app storage.Appender) {
				lbls := labels.FromStrings("__name__", "test_metric_ttl", "label1", "value1")
				ts := time.Now().Add(-2 * time.Hour).UnixMilli() // 2 hours old timestamp
				_, err := app.Append(0, lbls, ts, 42.0)
				require.NoError(t, err)
				require.NoError(t, app.Commit())
			},
			metricCount: 0, // Should be 0 since the metric is older than TTL
		},
	}

	// Run test cases
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reg := prometheus.NewRegistry()
			recordsFound := atomic.NewInt32(0)

			svr := httptest.NewServer(handler(t, http.StatusOK, func(wr *prompb.WriteRequest) {
				recordsFound.Add(int32(len(wr.Timeseries)))
			}))
			dir := t.TempDir()

			// Create a new queue
			q, err := NewQueue("test",
				types.ConnectionConfig{
					URL:              svr.URL,
					Timeout:          1 * time.Second,
					BatchCount:       1,
					FlushInterval:    1 * time.Second,
					RetryBackoff:     100 * time.Millisecond,
					MaxRetryAttempts: 1,
					Parallelism: types.ParallelismConfig{
						AllowedDrift:                60 * time.Second,
						MaxConnections:              1,
						MinConnections:              1,
						ResetInterval:               5 * time.Minute,
						Lookback:                    5 * time.Minute,
						CheckInterval:               10 * time.Second,
						AllowedNetworkErrorFraction: 0.05,
					},
				},
				dir,
				1,
				time.Second,
				1*time.Hour,
				reg,
				"test",
				logger,
			)
			require.NoError(t, err)
			ctx, cncl := context.WithCancel(context.Background())
			defer cncl()
			q.Start(ctx)
			defer q.Stop()

			defer svr.Close()
			app := q.Appender(ctx)
			tt.testFunc(t, ctx, app)
			require.Eventually(t, func() bool {
				return recordsFound.Load() == tt.metricCount
			}, 10*time.Second, 1*time.Second)
		})
	}
}

func TestStats(t *testing.T) {
	reg := prometheus.NewRegistry()
	end, err := NewQueue("test", types.ConnectionConfig{
		URL: "example.com",
	}, t.TempDir(), 1, 1*time.Minute, 5*time.Second, reg, "test", log.NewNopLogger())
	require.NoError(t, err)
	// This will unregister the metrics
	ctx, cncl := context.WithCancel(context.Background())
	defer cncl()
	end.Start(ctx)
	// This sleep is to give the goroutines time to spin up.
	time.Sleep(1 * time.Second)
	end.Stop()

	// This will trigger a panic if duplicate metrics found.
	end2, err := NewQueue("test", types.ConnectionConfig{
		URL: "example.com",
	}, t.TempDir(), 1, 1*time.Minute, 5*time.Second, reg, "test", log.NewNopLogger())
	require.NoError(t, err)

	end2.Start(ctx)
	// This sleep is to give the goroutines time to spin up.
	time.Sleep(1 * time.Second)
	end2.Stop()
}

func handler(t *testing.T, code int, callback func(wr *prompb.WriteRequest)) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		buf, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		defer r.Body.Close()
		decoded, err := snappy.Decode(nil, buf)
		require.NoError(t, err)

		wr := &prompb.WriteRequest{}
		err = wr.Unmarshal(decoded)
		require.NoError(t, err)
		callback(wr)
		w.WriteHeader(code)
	})
}
