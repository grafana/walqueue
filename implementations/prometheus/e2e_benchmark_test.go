package prometheus

import (
	"context"
	"github.com/grafana/walqueue/types"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"
)

func BenchmarkE2E(b *testing.B) {
	/*
		This adds 10_000 prometheus metrics and checks them. 70% of the allocs are building the test series. 14k are the actual work.
			cpu: 13th Gen Intel(R) Core(TM) i5-13500
			2025-01-31 BenchmarkE2E/normal-20         	       1	10009671619 ns/op	10864704 B/op	   67452 allocs/op
	*/
	type e2eTest struct {
		name   string
		maker  func(index int, app storage.Appender)
		tester func(samples []prompb.TimeSeries)
	}
	tests := []e2eTest{
		{
			name: "normal",
			maker: func(index int, app storage.Appender) {
				ts, v, lbls := makeSeries(index)
				_, _ = app.Append(0, lbls, ts, v)
			},
			tester: func(samples []prompb.TimeSeries) {
				b.Helper()
				for _, s := range samples {
					require.True(b, len(s.Samples) == 1)
				}
			},
		},
	}
	for _, test := range tests {
		b.Run(test.name, func(t *testing.B) {
			runBenchmark(t, test.maker, test.tester)
		})
	}
}

func runBenchmark(t *testing.B, add func(index int, appendable storage.Appender), _ func(samples []prompb.TimeSeries)) {
	t.ReportAllocs()
	l := log.NewNopLogger()
	done := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
	}))
	c, err := newComponentBenchmark(t, l, srv.URL)
	require.NoError(t, err)
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	c.Start()
	defer c.Stop()

	app := c.Appender(ctx)

	for i := 0; i < 10_000; i++ {
		add(i, app)
	}
	require.NoError(t, app.Commit())

	tm := time.NewTimer(10 * time.Second)
	select {
	case <-done:
	case <-tm.C:
	}
	cancel()
}

func newComponentBenchmark(t *testing.B, l log.Logger, url string) (Queue, error) {
	return NewQueue("test", types.ConnectionConfig{
		URL:              url,
		Timeout:          20 * time.Second,
		RetryBackoff:     1 * time.Second,
		MaxRetryAttempts: 1,
		BatchCount:       2000,
		FlushInterval:    1 * time.Second,
		Connections:      20,
	}, t.TempDir(), 10_000, 1*time.Second, 1*time.Hour, prometheus.NewRegistry(), "alloy", l)

}

var _ prometheus.Registerer = (*fakeRegistry)(nil)

type fakeRegistry struct{}

func (f fakeRegistry) Register(collector prometheus.Collector) error {
	return nil
}

func (f fakeRegistry) MustRegister(collector ...prometheus.Collector) {
}

func (f fakeRegistry) Unregister(collector prometheus.Collector) bool {
	return true
}
