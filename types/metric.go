package types

import (
	"fmt"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
)

type FileFormat string

const AlloyFileVersionV1 = FileFormat("alloy.metrics.queue.v1")

const AlloyFileVersionV2 = FileFormat("alloy.metrics.queue.v2")

type Type string

const PrometheusMetricV1 = Type("prometheus.metric.v1")
const PrometheusMetadataV1 = Type("prometheus.metadata.v1")

// Datum represent one item of data.
type Datum interface {
	// Bytes represents the underlying data and should not be handled aside from
	// Build* functions that understand the Type.
	Bytes() []byte
	Type() Type
	FileFormat() FileFormat
	// Free datums are often pooled, this should be called. Note calling free multiple times may cause bad behavior.
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

// Serialization provides the ability to read and write for a given schema defined by the FileFormat.
type Serialization interface {
	// Deserialize is called to create a list of datums.
	// Metadata will be passed via the map.
	// The buffer passed in is SAFE for reuse/unsafe strings.
	Deserialize(map[string]string, []byte) (items []Datum, err error)
	// Serialize handler passes in the buffer to be written. The buffer is only valid for the lifecycle of the function call.
	// Metadata is passed via the map and should be encoded into the underlying storage. The same keys and values should be returned
	// on Deserialize.
	Serialize(handle func(map[string]string, []byte) error) error
}

type PrometheusSerialization interface {
	Serialization
	AddPrometheusMetric(ts int64, value float64, lbls labels.Labels, h *histogram.Histogram, fh *histogram.FloatHistogram, externalLabels map[string]string) error
	AddPrometheusMetadata(name string, unit string, help string, pType string) error
}

func BuildPrometheusTimeSeries(d Datum) ([]byte, error) {
	switch d.Type() {
	case PrometheusMetricV1:
		return d.Bytes(), nil
	default:
		return nil, fmt.Errorf("invalid type %s", d.Type())
	}
}
