package types

import (
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
	// Bytes represents the underlying data and should only be used in conjuction with the type.
	Bytes() []byte
	Type() Type
	FileFormat() FileFormat
	// Free: datums are often pooled and this should be called when the datum is no longer needed.
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

// Marshaller provides the ability to read and write for a given schema defined by the FileFormat.
type Marshaller interface {
	// Unmarshal is called to create a list of datums.
	// Metadata will be passed via the map.
	// The buffer passed in is SAFE for reuse/unsafe strings.
	Unmarshal(map[string]string, []byte) (items []Datum, err error)
	// Marshal handler passes in the buffer to be written. The buffer is only valid for the lifecycle of the function call.
	// Metadata is passed via the map and should be encoded into the underlying storage. The same keys and values should be returned
	// on Deserialize.
	Marshal(handle func(map[string]string, []byte) error) error
}

type PrometheusMarshaller interface {
	Marshaller
	AddPrometheusMetric(ts int64, value float64, lbls labels.Labels, h *histogram.Histogram, fh *histogram.FloatHistogram, externalLabels map[string]string) error
	AddPrometheusMetadata(name string, unit string, help string, pType string) error
}
