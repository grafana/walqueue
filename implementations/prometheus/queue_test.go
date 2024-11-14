package prometheus

import (
	"context"
	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"
	"go.uber.org/atomic"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/walqueue/v2/types"
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
	}

	// Run test cases
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			recordsFound := atomic.NewInt32(0)

			svr := httptest.NewServer(handler(t, http.StatusOK, func(wr *prompb.WriteRequest) {
				recordsFound.Add(int32(len(wr.Timeseries)))
			}))
			dir := t.TempDir()

			// Create a new queue
			q, err := NewQueue(
				types.ConnectionConfig{
					URL:              svr.URL,
					Timeout:          1 * time.Second,
					BatchCount:       1,
					FlushInterval:    1 * time.Second,
					RetryBackoff:     100 * time.Millisecond,
					MaxRetryAttempts: 1,
					Connections:      1,
				},
				dir,
				1,
				time.Second,
				1*time.Hour,
				logger,
				func(stats types.NetworkStats) {},
				func(stats types.NetworkStats) {},
				func(stats types.SerializerStats) {},
			)
			require.NoError(t, err)
			q.Start()
			defer q.Stop()

			defer svr.Close()
			ctx := context.Background()
			app := q.Appender(ctx)
			tt.testFunc(t, ctx, app)
			require.Eventually(t, func() bool {
				return recordsFound.Load() == tt.metricCount
			}, 10*time.Second, 100*time.Millisecond)
		})
	}

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
