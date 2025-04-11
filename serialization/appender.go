package serialization

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/walqueue/types"
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	md "github.com/prometheus/prometheus/model/metadata"
	"github.com/prometheus/prometheus/storage"
)

var metricPool = sync.Pool{
	New: func() interface{} {
		return &types.PrometheusMetric{}
	},
}

type appender struct {
	ctx            context.Context
	s              types.PrometheusSerializer
	logger         log.Logger
	externalLabels map[string]string
	metrics        []*types.PrometheusMetric
	ttl            time.Duration
}

// NewAppender returns an Appender that writes to a given serializer. NOTE the returned Appender writes
// data immediately, discards data older than `ttl` and does not honor commit or rollback.
func NewAppender(ctx context.Context, ttl time.Duration, s types.PrometheusSerializer, externalLabels map[string]string, logger log.Logger) storage.Appender {
	app := &appender{
		ttl:            ttl,
		s:              s,
		logger:         logger,
		ctx:            ctx,
		externalLabels: externalLabels,
		metrics:        make([]*types.PrometheusMetric, 0),
	}
	return app
}

func (a *appender) AppendCTZeroSample(ref storage.SeriesRef, l labels.Labels, t, ct int64) (storage.SeriesRef, error) {
	// TODO @mattdurham figure out what to do here later. This mirrors what we do elsewhere.
	return ref, nil
}

// Append metric
func (a *appender) Append(ref storage.SeriesRef, l labels.Labels, t int64, v float64) (storage.SeriesRef, error) {
	endTime := time.Now().Unix() - int64(a.ttl.Seconds())
	if t < endTime {
		return ref, nil
	}
	pm := metricPool.Get().(*types.PrometheusMetric)
	pm.L = l
	pm.T = t
	pm.V = v
	a.metrics = append(a.metrics, pm)
	return ref, nil
}

func (a *appender) Commit() error {
	defer putMetrics(a.metrics)
	return a.s.SendMetrics(a.ctx, a.metrics, a.externalLabels)
}

func (a *appender) Rollback() error {
	defer putMetrics(a.metrics)
	return nil
}

func putMetrics(metrics []*types.PrometheusMetric) {
	for _, m := range metrics {
		m.FH = nil
		m.H = nil
		m.L = nil
		m.T = 0
		m.V = 0
		metricPool.Put(m)
	}
}

// AppendExemplar appends exemplar to cache. The passed in labels is unused, instead use the labels on the exemplar.
func (a *appender) AppendExemplar(ref storage.SeriesRef, _ labels.Labels, e exemplar.Exemplar) (_ storage.SeriesRef, _ error) {
	// Exemplars dont really work due to the relabelling issue, they need to be sent with the metric itself.
	return ref, nil
}

// AppendHistogram appends histogram
func (a *appender) AppendHistogram(ref storage.SeriesRef, l labels.Labels, t int64, h *histogram.Histogram, fh *histogram.FloatHistogram) (_ storage.SeriesRef, _ error) {
	endTime := time.Now().Unix() - int64(a.ttl.Seconds())
	if t < endTime {
		return ref, nil
	}
	pm := metricPool.Get().(*types.PrometheusMetric)
	pm.L = l
	pm.T = t
	pm.H = h
	pm.FH = fh
	a.metrics = append(a.metrics, pm)
	return ref, nil
}

// UpdateMetadata updates metadata.
func (a *appender) UpdateMetadata(ref storage.SeriesRef, l labels.Labels, m md.Metadata) (_ storage.SeriesRef, _ error) {
	// NOTE: This will never get called unless a hidden non exposed setting is enabled in the scraper to send metadata via the appender.
	if !l.Has("__name__") {
		return ref, fmt.Errorf("missing __name__ label for metadata")
	}
	name := l.Get("__name__")
	err := a.s.SendMetadata(a.ctx, name, m.Unit, m.Help, string(m.Type))
	return ref, err
}
