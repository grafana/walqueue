package types

import (
	"fmt"

	"github.com/prometheus/prometheus/model/labels"
)

type FileFormat string

const AlloyFileVersionV1 = FileFormat("alloy.metrics.queue.v1")

const AlloyFileVersionV2 = FileFormat("alloy.metrics.queue.v2")

type Type string

const PrometheusMetricV1 = Type("prometheus.metric.v1")

// Datum represent one item of data.
type Datum interface {
	Hash() int64
	TimeStampMS() uint64
	// Bytes represents the underlying data and should not be handled aside from
	// Build* functions that understand the Type.
	Bytes() []byte
	Type() Type
	FileFormat() FileFormat
}

// Serialization provides the ability to read and write for a given schema defined by the FileFormat.
type Serialization interface {
	AddPrometheusMetric(ts int64, value float64, lbls labels.Labels) error
	Deserialize([]byte) (items []Datum, err error)
	Count() int
	Serialize(handle func([]byte))
}

func BuildPrometheusTimeSeries(d Datum) ([]byte, error) {
	switch d.Type() {
	case PrometheusMetricV1:
		return d.Bytes(), nil
	default:
		return nil, fmt.Errorf("invalid type %s", d.Type())
	}
}
