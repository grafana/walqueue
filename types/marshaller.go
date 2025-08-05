package types

import (
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
)

// Marshaller provides the ability to write for a given schema defined by the FileFormat.
// These are NOT threadsafe.
type Marshaller interface {

	// Marshal handler passes in the buffer to be written. The buffer is only valid for the lifecycle of the function call.
	// Metadata is passed via the map and should be encoded into the underlying storage. The same keys and values should be returned
	// on Deserialize.
	Marshal(handle func(map[string]string, []byte) error) error
}

type PrometheusMarshaller interface {
	Marshaller
	// AddPrometheusMetric adds a metric to the list of metrics. External Labels are passed in and added to the raw byte representation.
	// They are not added to lbls since that array may not be owned by the caller. Metric labels will override external labels.
	AddPrometheusMetric(ts int64, value float64, lbls labels.Labels, h *histogram.Histogram, fh *histogram.FloatHistogram, e exemplar.Exemplar, externalLabels labels.Labels) error
	AddPrometheusMetadata(name string, unit string, help string, pType string) error
}

// Unmarshaller allows reading of a given FileFormat.
type Unmarshaller interface {
	// Unmarshal is called to create a list of datums.
	// Metadata will be passed via the map.
	// The buffer passed in is SAFE for reuse/unsafe strings.
	Unmarshal(map[string]string, []byte) (items []Datum, err error)
}
