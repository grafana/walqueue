package serialization

import (
	"context"
	"fmt"
	"github.com/grafana/walqueue/types/v2"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/walqueue/types"
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/metadata"
	"github.com/prometheus/prometheus/storage"
)

var metricSinglePool = sync.Pool{
	New: func() any {
		return &types.Metric{}
	},
}

func GetMetricFromPool() *types.Metric {
	return metricSinglePool.Get().(*types.Metric)
}

func PutMetricSliceIntoPool(m []*types.Metric) {
	for _, mt := range m {
		PutMetricIntoPool(mt)
	}
}

func PutMetricIntoPool(m *types.Metric) {
	m.Hash = 0
	m.TS = 0
	m.Value = 0
	// We dont reuse these labels since they are owned by the scraper.
	m.Labels = nil
	m.Histogram = nil
	m.FloatHistogram = nil

	metricSinglePool.Put(m)
}

type appender struct {
	ctx          context.Context
	ttl          time.Duration
	s            types.Serializer
	logger       log.Logger
	incrementTTL func()
}

func (a *appender) AppendCTZeroSample(ref storage.SeriesRef, l labels.Labels, t, ct int64) (storage.SeriesRef, error) {
	// TODO @mattdurham figure out what to do here later. This mirrors what we do elsewhere.
	return ref, nil
}

// NewAppender returns an Appender that writes to a given serializer. NOTE the returned Appender writes
// data immediately, discards data older than `ttl` and does not honor commit or rollback.
func NewAppender(ctx context.Context, ttl time.Duration, s types.Serializer, logger log.Logger) storage.Appender {
	app := &appender{
		ttl:    ttl,
		s:      s,
		logger: logger,
		ctx:    ctx,
	}
	return app
}

// Append metric
func (a *appender) Append(ref storage.SeriesRef, l labels.Labels, t int64, v float64) (storage.SeriesRef, error) {
	ts := types.GetMetricFromPool(false)
	ts.Labels = l
	ts.TS = t
	ts.Value = v
	ts.Hash = l.Hash()
	err := a.s.SendSeries(a.ctx, ts)
	return ref, err
}

// Commit is a no op since we always write.
func (a *appender) Commit() (_ error) {
	return nil
}

// Rollback is a no op since we write all the data.
func (a *appender) Rollback() error {
	return nil
}

// AppendExemplar appends exemplar to cache. The passed in labels is unused, instead use the labels on the exemplar.
func (a *appender) AppendExemplar(ref storage.SeriesRef, _ labels.Labels, e exemplar.Exemplar) (_ storage.SeriesRef, _ error) {
	endTime := time.Now().Unix() - int64(a.ttl.Seconds())
	if e.HasTs && e.Ts < endTime {
		return ref, nil
	}
	ts := types.GetMetricFromPool(false)
	ts.Hash = e.Labels.Hash()
	ts.TS = e.Ts
	ts.Labels = e.Labels.Copy()
	ts.Hash = e.Labels.Hash()
	err := a.s.SendSeries(a.ctx, ts)
	return ref, err
}

// AppendHistogram appends histogram
func (a *appender) AppendHistogram(ref storage.SeriesRef, l labels.Labels, t int64, h *histogram.Histogram, fh *histogram.FloatHistogram) (_ storage.SeriesRef, _ error) {
	endTime := time.Now().Unix() - int64(a.ttl.Seconds())
	if t < endTime {
		return ref, nil
	}
	ts := types.GetMetricFromPool(false)
	ts.Labels = l.Copy()
	ts.TS = t
	if h != nil {
		ts.Histogram = h
	} else {
		ts.FloatHistogram = fh
	}
	ts.Hash = l.Hash()
	err := a.s.SendSeries(a.ctx, ts)
	return ref, err
}

// UpdateMetadata updates metadata.
func (a *appender) UpdateMetadata(ref storage.SeriesRef, l labels.Labels, m metadata.Metadata) (_ storage.SeriesRef, _ error) {
	if !l.Has("__name__") {
		return ref, fmt.Errorf("missing __name__ label for metadata")
	}
	ts := &types.Metric{}

	// We are going to handle converting some strings to hopefully not reused label names. TimeSeriesBinary has a lot of work
	// to ensure its efficient it makes sense to encode metadata into it.
	combinedLabels := labels.EmptyLabels()
	combinedLabels = append(combinedLabels, labels.Label{
		Name:  v2.MetaType,
		Value: string(m.Type),
	})
	combinedLabels = append(combinedLabels, labels.Label{
		Name:  v2.MetaHelp,
		Value: m.Help,
	})
	combinedLabels = append(combinedLabels, labels.Label{
		Name:  v2.MetaUnit,
		Value: m.Unit,
	})
	// We ONLY want __name__ from labels
	combinedLabels = append(combinedLabels, labels.Label{
		Name:  "__name__",
		Value: l.Get("__name__"),
	})
	ts.Labels = combinedLabels
	err := a.s.SendMetadata(a.ctx, ts)
	return ref, err
}
