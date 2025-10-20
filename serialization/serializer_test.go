//go:build !race

package serialization

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"go.uber.org/atomic"

	"github.com/go-kit/log"
	"github.com/grafana/walqueue/types"
	v2 "github.com/grafana/walqueue/types/v2"
	"github.com/klauspost/compress/zstd"
	"github.com/prometheus/prometheus/model/exemplar"
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
		require.True(t, stats.ExemplarsStored == 1)
		require.True(t, stats.Errors == 0)
		require.True(t, stats.MetadataStored == 0)
		require.True(t, stats.NewestTimestampSeconds > start)
	}, l)
	require.NoError(t, err)

	s.Start(context.TODO())
	defer s.Stop()
	for i := 0; i < 10; i++ {
		ex := exemplar.Exemplar{}
		builder := labels.NewScratchBuilder(20)
		for j := 0; j < 10; j++ {
			builder.Add(fmt.Sprintf("name_%d_%d", i, j), fmt.Sprintf("value_%d_%d", i, j))
		}

		builder.Sort()
		lbls := builder.Labels()

		if i == 0 {
			// Add an exemplar to only the first series
			ex.Value = rand.Float64()
			ex.Labels = lbls
		}
		sendErr := s.SendMetrics(context.Background(), []*types.PrometheusMetric{
			{
				L: lbls,
				T: time.Now().UnixMilli(),
				V: rand.Float64(),
				E: ex,
			},
		}, labels.EmptyLabels())
		require.NoError(t, sendErr)
	}
	require.Eventually(t, func() bool {
		return f.total.Load() == 10
	}, 10*time.Second, 1*time.Second)
	// 10 series send from the above for loop
	require.Truef(t, totalSeries.Load() == 10, "total series load does not equal 10 currently %d", totalSeries.Load())
}

func TestUpdateConfig(t *testing.T) {
	f := &fqq{t: t}
	l := log.NewNopLogger()
	s, err := NewSerializer(types.SerializerConfig{
		MaxSignalsInBatch: 10,
		FlushFrequency:    5 * time.Second,
	}, f, func(stats types.SerializerStats) {}, l)
	require.NoError(t, err)
	s.Start(context.TODO())
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

func (f *fqq) Start(_ context.Context) {

}

func (f *fqq) Stop() {

}

func (f *fqq) Store(_ context.Context, meta map[string]string, value []byte) error {
	zstdDecoder, _ := zstd.NewReader(nil)
	f.buf, _ = zstdDecoder.DecodeAll(value, f.buf)
	sg := v2.NewFormat()
	items, err := sg.Unmarshal(meta, f.buf)
	require.NoError(f.t, err)
	f.total.Add(int64(len(items)))
	return nil
}
