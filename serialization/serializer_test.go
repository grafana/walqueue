//go:build !race

package serialization

import (
	"context"
	"fmt"
	"github.com/grafana/walqueue/types/v2"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/golang/snappy"
	"github.com/grafana/walqueue/types"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
)

func TestRoundTripSerialization(t *testing.T) {
	totalSeries := atomic.Int64{}
	f := &fqq{t: t}
	l := log.NewNopLogger()
	start := time.Now().Add(-1 * time.Minute).Unix()

	s, err := NewSerializer(types.SerializerConfig{
		MaxSignalsInBatch: 10,
		FlushFrequency:    5 * time.Second,
	}, f, func(stats types.SerializerStats) {
		totalSeries.Add(int64(stats.SeriesStored))
		require.True(t, stats.SeriesStored == 10)
		require.True(t, stats.Errors == 0)
		require.True(t, stats.MetadataStored == 0)
		require.True(t, stats.NewestTimestampSeconds > start)
	}, types.AlloyFileVersionV2, l)
	require.NoError(t, err)

	s.Start()
	defer s.Stop()
	for i := 0; i < 10; i++ {
		tss := types.GetMetricFromPool(true)
		tss.Labels = make(labels.Labels, 10)
		for j := 0; j < 10; j++ {
			tss.Labels[j] = labels.Label{
				Name:  fmt.Sprintf("name_%d_%d", i, j),
				Value: fmt.Sprintf("value_%d_%d", i, j),
			}
			tss.Value = float64(i)
			tss.TS = time.Now().UnixMilli()
		}
		sendErr := s.SendSeries(context.Background(), tss)
		require.NoError(t, sendErr)
	}
	require.Eventually(t, func() bool {
		return f.total.Load() == 10
	}, 5*time.Second, 100*time.Millisecond)
	// 10 series send from the above for loop
	require.Truef(t, totalSeries.Load() == 10, "total series load does not equal 10 currently %d", totalSeries.Load())
}

func TestUpdateConfig(t *testing.T) {
	f := &fqq{t: t}
	l := log.NewNopLogger()
	s, err := NewSerializer(types.SerializerConfig{
		MaxSignalsInBatch: 10,
		FlushFrequency:    5 * time.Second,
	}, f, func(stats types.SerializerStats) {}, types.AlloyFileVersionV2, l)
	require.NoError(t, err)
	s.Start()
	defer s.Stop()
	success, err := s.UpdateConfig(context.Background(), types.SerializerConfig{
		MaxSignalsInBatch: 1,
		FlushFrequency:    1 * time.Second,
	})
	require.NoError(t, err)
	require.True(t, success)
	require.Eventually(t, func() bool {
		return s.(*serializer).maxItemsBeforeFlush == 1 && s.(*serializer).flushFrequency == 1*time.Second
	}, 5*time.Second, 100*time.Millisecond)
}

var _ types.FileStorage = (*fqq)(nil)

type fqq struct {
	t     *testing.T
	buf   []byte
	total atomic.Int64
}

func (f *fqq) Start() {

}

func (f *fqq) Stop() {

}

func (f *fqq) Store(ctx context.Context, meta map[string]string, value []byte) error {
	f.buf, _ = snappy.Decode(nil, value)
	sg := &v2.SeriesGroup{}
	metrics, _, err := v2.DeserializeToSeriesGroup(sg, f.buf)
	require.NoError(f.t, err)
	require.Len(f.t, sg.Series, 10)
	for _, series := range metrics.M {
		require.Len(f.t, series.Labels, 10)
		for j := 0; j < 10; j++ {
			series.Labels[j].Name = fmt.Sprintf("name_%d_%d", int(series.Value), j)
			series.Labels[j].Value = fmt.Sprintf("value_%d_%d", int(series.Value), j)
		}
	}
	f.total.Add(int64(len(sg.Series)))
	return nil
}
