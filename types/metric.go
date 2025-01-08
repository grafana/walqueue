package types

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
