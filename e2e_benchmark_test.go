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
	"sync"
	"testing"
	"time"
)

func TestV2E2E(b *testing.T) {
	// BenchmarkV2E2E-20    	    2504	    451484 ns/op
	dir := b.TempDir()
	totalSeries := atomic.NewInt32(0)
	mut := sync.Mutex{}
	set := make(map[float64]struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mut.Lock()
		defer mut.Unlock()
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
			for _, sample := range x.Samples {
				_, found := set[sample.Value]
				if found {
					panic("found duplicate sample")
				}
				set[sample.Value] = struct{}{}
			}
		}
	}))
	cc := types.ConnectionConfig{
		URL:           srv.URL,
		BatchCount:    10,
		FlushInterval: 1 * time.Second,
		Connections:   3,
		Timeout:       10 * time.Second,
	}
	q, err := prom.NewQueue("test", cc, dir, 10000, 1*time.Second, 1*time.Hour, prometheus.NewRegistry(), "alloy", types.AlloyFileVersionV2, log.NewLogfmtLogger(os.Stderr))

	require.NoError(b, err)
	go q.Start()
	defer q.Stop()

	metricCount := 100
	sends := 2
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
	index := 1
	for n := 0; n < sends; n++ {
		app := q.Appender(context.Background())
		for _, m := range metrics {
			app.Append(0, m.Labels, m.TS, float64(index))
			index++
		}
		app.Commit()
	}
	require.Eventually(b, func() bool {
		return totalSeries.Load() == int32(metricCount*sends)
	}, 50*time.Second, 50*time.Millisecond)
	require.Truef(b, types.OutstandingIndividualMetrics.Load() == 0, "outstanding indicidual metrics are %d", types.OutstandingIndividualMetrics.Load())
}

func TestV3E2E(b *testing.T) {
	dir := b.TempDir()
	totalSeries := atomic.NewInt32(0)
	mut := sync.Mutex{}
	set := make(map[float64]struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mut.Lock()
		defer mut.Unlock()
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
			for _, sample := range x.Samples {
				_, found := set[sample.Value]
				if found {
					panic("found duplicate sample")
				}
				set[sample.Value] = struct{}{}
			}
		}
	}))
	cc := types.ConnectionConfig{
		URL:           srv.URL,
		BatchCount:    10,
		FlushInterval: 1 * time.Second,
		Connections:   3,
		Timeout:       10 * time.Second,
	}
	q, err := prom.NewQueue("test", cc, dir, 10000, 1*time.Second, 1*time.Hour, prometheus.NewRegistry(), "alloy", types.AlloyFileVersionV3, log.NewLogfmtLogger(os.Stderr))

	require.NoError(b, err)
	go q.Start()
	defer q.Stop()

	metricCount := 100
	sends := 2
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
	index := 1
	for n := 0; n < sends; n++ {
		app := q.Appender(context.Background())
		for _, m := range metrics {
			app.Append(0, m.Labels, m.TS, float64(index))
			index++
		}
		app.Commit()
	}
	require.Eventually(b, func() bool {
		return totalSeries.Load() == int32(metricCount*sends)
	}, 50*time.Second, 50*time.Millisecond)
	require.Truef(b, types.OutstandingIndividualMetrics.Load() == 0, "outstanding indicidual metrics are %d", types.OutstandingIndividualMetrics.Load())
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randString() string {
	b := make([]rune, rand.Intn(20))
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}
