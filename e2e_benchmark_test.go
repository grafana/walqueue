package walqueue

import (
	"context"
	"fmt"
	"github.com/go-kit/log"
	"github.com/golang/snappy"
	prom "github.com/grafana/walqueue/implementations/prometheus"
	"github.com/grafana/walqueue/types"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"
)

func TestV2E2E(b *testing.T) {
	// BenchmarkV2E2E-20    	    2504	    451484 ns/op
	dir := b.TempDir()
	totalSeries := atomic.NewInt32(0)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		data, err := io.ReadAll(r.Body)
		require.NoError(b, err)
		data, err = snappy.Decode(nil, data)
		require.NoError(b, err)

		var req prompb.WriteRequest
		err = req.Unmarshal(data)
		require.NoError(b, err)
		for _, x := range req.GetTimeseries() {
			totalSeries.Add(int32(len(x.Samples)))
		}
	}))
	cc := types.ConnectionConfig{
		URL:           srv.URL,
		BatchCount:    1000,
		FlushInterval: 1 * time.Second,
		Connections:   10,
		Timeout:       10 * time.Second,
	}
	q, err := prom.NewQueue("test", cc, dir, 10000, 1*time.Second, 1*time.Hour, prometheus.NewRegistry(), "alloy", log.NewLogfmtLogger(os.Stderr))

	require.NoError(b, err)
	go q.Start()
	defer q.Stop()

	metricCount := 1_000
	sends := 1_000
	metrics := make([]*types.Metric, 0)
	for k := 0; k < metricCount; k++ {
		lblsMap := make(map[string]string)
		for j := 0; j < 10; j++ {
			key := fmt.Sprintf("key_%d", j)
			v := randString()
			lblsMap[key] = v
		}
		m := &types.Metric{}
		m.Labels = labels.FromMap(lblsMap)
		m.TS = time.Now().UnixMilli()
		metrics = append(metrics, m)
	}
	for n := 0; n < sends; n++ {
		app := q.Appender(context.Background())
		for _, m := range metrics {
			app.Append(0, m.Labels, m.TS, m.Value)
		}
		app.Commit()
	}
	require.Eventually(b, func() bool {
		return totalSeries.Load() == int32(metricCount*sends)
	}, 50*time.Second, 50*time.Millisecond)
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randString() string {
	b := make([]rune, rand.Intn(20))
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}
