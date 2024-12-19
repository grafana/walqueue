package types

import (
	"sync"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"go.uber.org/atomic"
)

// Serialization provides the ability to read and write for a given schema defined by the FileFormat.
type Serialization interface {
	// Serialize is used to convert metrics and metadata to a before that is passed
	// into handle. The []byte slice is only garaunteed within the func.
	// The handler will only be called if there is no error.
	Serialize(metrics []*Metric, metadata []*Metric, handle func([]byte)) error
	Deserialize([]byte) (metrics []*Metric, metadata []*Metric, err error)
}

type Metric struct {
	Labels         labels.Labels
	TS             int64
	Value          float64
	Hash           uint64
	Histogram      *histogram.Histogram
	FloatHistogram *histogram.FloatHistogram
}

// IsMetadata is used because it's easier to store metadata as a set of labels.
func (m Metric) IsMetadata() bool {
	return m.Labels.Has("__alloy_metadata_type__")
}

var OutstandingMetrics = atomic.Int32{}
var metricPool = sync.Pool{
	New: func() any {
		return make([]*Metric, 0)
	},
}

func GetMetricsFromPool() []*Metric {
	OutstandingMetrics.Inc()
	return metricPool.Get().([]*Metric)
}

func PutMetricsIntoPool(m []*Metric) {
	OutstandingMetrics.Dec()
	for _, met := range m {
		met.Hash = 0
		met.TS = 0
		met.Value = 0
		met.Labels = nil
		met.Histogram = nil
		met.FloatHistogram = nil
	}
	m = m[:0]
	metricPool.Put(m)
}

var OutstandingIndividualMetrics = atomic.Int32{}
var metricSinglePool = sync.Pool{
	New: func() any {
		return &Metric{}
	},
}

func GetMetricFromPool() *Metric {
	OutstandingIndividualMetrics.Inc()
	return metricSinglePool.Get().(*Metric)
}

func PutMetricIntoPool(m *Metric) {
	OutstandingIndividualMetrics.Dec()

	m.Hash = 0
	m.TS = 0
	m.Value = 0
	m.Labels = nil
	m.Histogram = nil
	m.FloatHistogram = nil

	metricSinglePool.Put(m)
}
