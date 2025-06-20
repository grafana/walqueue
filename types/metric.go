package types

import (
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
)

type FileFormat string

const AlloyFileVersionV1 = FileFormat("alloy.metrics.queue.v1")
const AlloyFileVersionV2 = FileFormat("alloy.metrics.queue.v2")

type Type string

// PrometheusMetricV1 corresponds to prompb.TimeSeries byte format.
const PrometheusMetricV1 = Type("prometheus.metric.v1")

// PrometheusMetadataV1	corresponds to prompb.MetricMetadata byte format.
const PrometheusMetadataV1 = Type("prometheus.metadata.v1")

// Datum represent one item of data.
type Datum interface {
	// Bytes represents the underlying data and should only be used in conjunction with the type.
	Bytes() []byte
	Type() Type
	FileFormat() FileFormat
	// Free  datums are often pooled and this should be called when the datum is no longer needed.
	Free()
}

type MetricDatum interface {
	Datum
	Hash() uint64
	TimeStampMS() int64
	IsHistogram() bool
}

type MetadataDatum interface {
	Datum
	IsMeta() bool
}

type PrometheusMetric struct {
	H  *histogram.Histogram
	FH *histogram.FloatHistogram
	L  labels.Labels
	E  exemplar.Exemplar
	T  int64
	V  float64
}
