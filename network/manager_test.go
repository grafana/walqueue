package network

import (
	"context"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/golang/snappy"
	"github.com/grafana/walqueue/types"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

func TestSending(t *testing.T) {
	tsMap := map[float64]struct{}{}
	mut := sync.Mutex{}
	recordsFound := atomic.Uint32{}
	svr := httptest.NewServer(handler(t, http.StatusOK, func(wr *prompb.WriteRequest) {
		recordsFound.Add(uint32(len(wr.Timeseries)))
		mut.Lock()
		for _, ts := range wr.Timeseries {
			_, found := tsMap[ts.Samples[0].Value]
			if found {
				require.Truef(t, false, "found duplicate value %f", ts.Samples[0].Value)
			}
			tsMap[ts.Samples[0].Value] = struct{}{}
		}
		defer mut.Unlock()
	}))

	defer svr.Close()
	ctx := context.Background()
	ctx, cncl := context.WithCancel(ctx)
	defer cncl()

	cc := types.ConnectionConfig{
		URL:            svr.URL,
		Timeout:        1 * time.Second,
		BatchCount:     10,
		FlushInterval:  1 * time.Second,
		MinConnections: 4,
		MaxConnections: 4,
	}

	logger := log.NewNopLogger()
	wr, err := New(cc, logger, func(s types.NetworkStats) {}, func(s types.NetworkStats) {})
	wr.Start(ctx)
	defer wr.Stop()

	require.NoError(t, err)
	for i := 0; i < 100; i++ {
		send(t, i, wr, ctx)
	}
	require.Eventuallyf(t, func() bool {
		return recordsFound.Load() == 100
	}, 10*time.Second, 100*time.Millisecond, "expected 100 records but got %d", recordsFound.Load())
}

func TestUpdatingConfig(t *testing.T) {
	recordsFound := atomic.Uint32{}
	lastBatchSize := atomic.Uint32{}
	svr := httptest.NewServer(handler(t, http.StatusOK, func(wr *prompb.WriteRequest) {
		lastBatchSize.Store(uint32(len(wr.Timeseries)))
		recordsFound.Add(uint32(len(wr.Timeseries)))
	}))

	defer svr.Close()

	cc := types.ConnectionConfig{
		URL:            svr.URL,
		Timeout:        1 * time.Second,
		BatchCount:     10,
		FlushInterval:  5 * time.Second,
		MinConnections: 1,
		MaxConnections: 1,
	}

	logger := log.NewNopLogger()

	wr, err := New(cc, logger, func(s types.NetworkStats) {}, func(s types.NetworkStats) {})
	require.NoError(t, err)
	ctx := context.Background()
	wr.Start(ctx)
	defer wr.Stop()

	cc2 := types.ConnectionConfig{
		URL:            svr.URL,
		Timeout:        1 * time.Second,
		BatchCount:     20,
		FlushInterval:  5 * time.Second,
		MinConnections: 1,
		MaxConnections: 1,
	}

	success, err := wr.UpdateConfig(ctx, cc2)
	require.NoError(t, err)
	require.True(t, success)
	time.Sleep(1 * time.Second)
	for i := 0; i < 40; i++ {
		send(t, i, wr, ctx)
	}
	require.Eventuallyf(t, func() bool {
		return recordsFound.Load() == 40
	}, 20*time.Second, 1*time.Second, "record count should be 40 but is %d", recordsFound.Load())

	require.Truef(t, lastBatchSize.Load() == 20, "batch_count should be 20 but is %d", lastBatchSize.Load())
}

func TestDrain(t *testing.T) {
	recordsFound := atomic.Uint32{}
	headerVal := atomic.Int32{}
	valueSent := atomic.Bool{}
	valueSent.Store(false)
	// This will cause it to buffer requests.
	headerVal.Store(http.StatusTooManyRequests)
	svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		buf, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		defer r.Body.Close()
		decoded, err := snappy.Decode(nil, buf)
		require.NoError(t, err)

		wr := &prompb.WriteRequest{}
		err = wr.Unmarshal(decoded)
		require.NoError(t, err)
		valueSent.Store(true)
		w.WriteHeader(int(headerVal.Load()))
		// Only add if we are in OK mode.
		if headerVal.Load() == http.StatusOK {
			recordsFound.Add(uint32(len(wr.Timeseries)))
		}
	}))

	defer svr.Close()

	cc := types.ConnectionConfig{
		URL:              svr.URL,
		Timeout:          1 * time.Second,
		BatchCount:       1,
		FlushInterval:    5 * time.Second,
		MinConnections:   1,
		MaxConnections:   1,
		MaxRetryAttempts: 100,
		RetryBackoff:     10 * time.Second,
	}

	logger := log.NewNopLogger()

	wr, err := New(cc, logger, func(s types.NetworkStats) {}, func(s types.NetworkStats) {})
	require.NoError(t, err)
	ctx := context.Background()
	wr.Start(ctx)
	defer wr.Stop()
	// Kick these off in the background.
	go func() {
		for i := 0; i < 40; i++ {
			send(t, i, wr, ctx)
		}
	}()
	// Make sure we get a request sent so that it is in the queue.
	require.Eventually(t, func() bool {
		return valueSent.Load()
	}, 5*time.Second, 100*time.Millisecond)
	cc2 := types.ConnectionConfig{
		URL:              svr.URL,
		Timeout:          1 * time.Second,
		BatchCount:       1,
		FlushInterval:    5 * time.Second,
		MinConnections:   4,
		MaxConnections:   4,
		MaxRetryAttempts: 100,
		RetryBackoff:     10 * time.Second,
	}
	// Update the config which should NOT lose any data
	wr.UpdateConfig(ctx, cc2)
	// Once the update comes through ensure ok so that data can flow.
	headerVal.Store(http.StatusOK)
	require.Eventuallyf(t, func() bool {
		return recordsFound.Load() == 40
	}, 20*time.Second, 1*time.Second, "record count should be 40 but is %d", recordsFound.Load())
}

func TestRetry(t *testing.T) {
	retries := atomic.Uint32{}
	var previous *prompb.WriteRequest
	svr := httptest.NewServer(handler(t, http.StatusTooManyRequests, func(wr *prompb.WriteRequest) {
		retries.Add(1)
		// Check that we are getting the same sample back.
		if previous == nil {
			previous = wr
		} else {
			require.True(t, previous.Timeseries[0].Labels[0].Value == wr.Timeseries[0].Labels[0].Value)
		}
	}))
	defer svr.Close()
	ctx := context.Background()
	ctx, cncl := context.WithCancel(ctx)
	defer cncl()

	cc := types.ConnectionConfig{
		URL:            svr.URL,
		Timeout:        1 * time.Second,
		BatchCount:     1,
		FlushInterval:  1 * time.Second,
		RetryBackoff:   100 * time.Millisecond,
		MinConnections: 1,
		MaxConnections: 1,
	}

	logger := log.NewNopLogger()
	wr, err := New(cc, logger, func(s types.NetworkStats) {}, func(s types.NetworkStats) {})
	require.NoError(t, err)
	wr.Start(ctx)
	defer wr.Stop()
	send(t, 1, wr, ctx)

	require.Eventually(t, func() bool {
		done := retries.Load() > 5
		return done
	}, 10*time.Second, 1*time.Second)
}

func TestRetryBounded(t *testing.T) {

	sends := atomic.Uint32{}
	svr := httptest.NewServer(handler(t, http.StatusTooManyRequests, func(wr *prompb.WriteRequest) {
		sends.Add(1)
	}))

	defer svr.Close()
	ctx := context.Background()
	ctx, cncl := context.WithCancel(ctx)
	defer cncl()

	cc := types.ConnectionConfig{
		URL:              svr.URL,
		Timeout:          1 * time.Second,
		BatchCount:       1,
		FlushInterval:    1 * time.Second,
		RetryBackoff:     100 * time.Millisecond,
		MaxRetryAttempts: 1,
		MinConnections:   1,
		MaxConnections:   1,
	}

	logger := log.NewNopLogger()
	wr, err := New(cc, logger, func(s types.NetworkStats) {}, func(s types.NetworkStats) {})
	wr.Start(ctx)
	defer wr.Stop()
	require.NoError(t, err)
	for i := 0; i < 10; i++ {
		send(t, i, wr, ctx)
	}
	require.Eventuallyf(t, func() bool {
		// We send 10 but each one gets retried once so 20 total.
		return sends.Load() == 10*2
	}, 5*time.Second, 100*time.Millisecond, "expected 20 records but got %d", sends.Load())
	time.Sleep(5 * time.Second)
	// Ensure we dont get any more.
	require.True(t, sends.Load() == 10*2)
}

func TestRecoverable(t *testing.T) {
	recoverable := atomic.Uint32{}
	svr := httptest.NewServer(handler(t, http.StatusInternalServerError, func(wr *prompb.WriteRequest) {
	}))
	defer svr.Close()
	ctx := context.Background()

	cc := types.ConnectionConfig{
		URL:              svr.URL,
		Timeout:          100 * time.Millisecond,
		BatchCount:       1,
		FlushInterval:    10 * time.Second,
		RetryBackoff:     100 * time.Millisecond,
		MaxRetryAttempts: 1,
		MinConnections:   10,
		MaxConnections:   10,
	}

	logger := log.NewNopLogger()
	wr, err := New(cc, logger, func(s types.NetworkStats) {
		recoverable.Add(uint32(s.Total5XX()))
	}, func(s types.NetworkStats) {})
	require.NoError(t, err)
	wr.Start(ctx)
	defer wr.Stop()
	for i := 0; i < 10; i++ {
		send(t, i, wr, ctx)
	}
	require.Eventuallyf(t, func() bool {
		// We send 10 but each one gets retried once so 20 total.
		return recoverable.Load() == 10*2
	}, 40*time.Second, 100*time.Millisecond, "recoverable should be 20 but is %d", recoverable.Load())
	time.Sleep(2 * time.Second)
	// Ensure we dont get any more.
	require.True(t, recoverable.Load() == 10*2)
}

func TestNonRecoverable(t *testing.T) {

	nonRecoverable := atomic.Uint32{}
	svr := httptest.NewServer(handler(t, http.StatusBadRequest, func(wr *prompb.WriteRequest) {
	}))

	defer svr.Close()
	ctx := context.Background()
	ctx, cncl := context.WithCancel(ctx)
	defer cncl()

	cc := types.ConnectionConfig{
		URL:              svr.URL,
		Timeout:          1 * time.Second,
		BatchCount:       1,
		FlushInterval:    1 * time.Second,
		RetryBackoff:     100 * time.Millisecond,
		MaxRetryAttempts: 1,
		MinConnections:   1,
		MaxConnections:   1,
	}

	logger := log.NewNopLogger()
	wr, err := New(cc, logger, func(s types.NetworkStats) {
		nonRecoverable.Add(uint32(s.TotalFailed()))
	}, func(s types.NetworkStats) {})
	wr.Start(ctx)
	defer wr.Stop()
	require.NoError(t, err)
	for i := 0; i < 10; i++ {
		send(t, i, wr, ctx)
	}
	require.Eventuallyf(t, func() bool {
		return nonRecoverable.Load() == 10
	}, 10*time.Second, 100*time.Millisecond, "non recoverable should be 10 but is %d", nonRecoverable.Load())
	time.Sleep(2 * time.Second)
	// Ensure we dont get any more.
	require.True(t, nonRecoverable.Load() == 10)
}

func send(t *testing.T, i int, wr types.NetworkClient, ctx context.Context) {
	ts := createSeries(i, t)
	// The actual hash is only used for queueing into different buckets.
	err := wr.SendSeries(ctx, ts)
	require.NoError(t, err)
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

func createSeries(i int, _ *testing.T) types.MetricDatum {
	ts := &prompb.TimeSeries{}
	ts.Samples = make([]prompb.Sample, 1)
	ts.Samples[0] = prompb.Sample{
		Timestamp: time.Now().Unix(),
		Value:     float64(i),
	}
	ts.Labels = make([]prompb.Label, 1)
	ts.Labels[0] = prompb.Label{
		Name:  "__name__",
		Value: randSeq(10),
	}
	bb, _ := ts.Marshal()
	return &metric{
		hash:        uint64(i),
		ts:          ts.Samples[0].Timestamp,
		buf:         bb,
		isHistogram: false,
	}
}

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randSeq(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

var _ types.MetricDatum = (*metric)(nil)

type metric struct {
	hash        uint64
	ts          int64
	buf         []byte
	isHistogram bool
}

func (m metric) Hash() uint64 {
	return m.hash
}

func (m metric) TimeStampMS() int64 {
	return m.ts
}

func (m metric) IsHistogram() bool {
	return m.isHistogram
}

// Bytes represents the underlying data and should not be handled aside from
// Build* functions that understand the Type.
func (m metric) Bytes() []byte {
	return m.buf
}

func (m metric) Type() types.Type {
	return types.PrometheusMetricV1
}

func (m metric) FileFormat() types.FileFormat {
	return types.AlloyFileVersionV2
}

func (m *metric) Free() {
}
