package prometheus

import (
	"context"
	"github.com/grafana/walqueue/types"
	"github.com/prometheus/prometheus/model/labels"
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
		go test -bench="BenchmarkE2E" -run="BenchmarkE2E" -benchmem -benchtime "5s";
		cpu: 13th Gen Intel(R) Core(TM) i5-13500
		2025-01-31 BenchmarkE2E/normal-20                 1        10008482261 ns/op       10114520 B/op      17934 allocs/op
	*/
	type e2eTest struct {
		name   string
		maker  func(b *testing.B, app storage.Appender)
		tester func(samples []prompb.TimeSeries)
	}
	type item struct {
		ts    int64
		value float64
		lbls  labels.Labels
	}
	tests := []e2eTest{
		{
			name: "normal",
			maker: func(t *testing.B, app storage.Appender) {
				t.StopTimer()
				itemList := make([]item, 10_000)
				for i := range itemList {
					ts, val, lbls := makeSeries(i)
					itemList[i] = item{
						ts:    ts,
						value: val,
						lbls:  lbls,
					}
				}
				t.StartTimer()
				for _, it := range itemList {
					_, _ = app.Append(0, it.lbls, it.ts, it.value)
				}
			},
			tester: func(samples []prompb.TimeSeries) {
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

func runBenchmark(t *testing.B, add func(b *testing.B, appendable storage.Appender), _ func(samples []prompb.TimeSeries)) {
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

	for i := 0; i < t.N; i++ {
		add(t, app)
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
