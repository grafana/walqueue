package v2

import "github.com/grafana/walqueue/types"

var _ types.MetricDatum = (*Metric)(nil)

func (m Metric) Hash() uint64 {
	return m.Hashvalue
}

func (m Metric) TimeStampMS() int64 {
	return m.Timestampmsvalue
}

func (m Metric) IsHistogram() bool {
	return m.IsHistogramvalue
}

// Bytes represents the underlying data and should not be handled aside from
// Build* functions that understand the Type.
func (m Metric) Bytes() []byte {
	return m.Buf
}

func (m Metric) Type() types.Type {
	return types.PrometheusMetricV1
}

func (m Metric) FileFormat() types.FileFormat {
	return types.AlloyFileVersionV2
}

func (m *Metric) Free() {
	m.Hashvalue = 0
	m.Timestampmsvalue = 0
	m.IsHistogramvalue = false
	m.Buf = m.Buf[:0]
	metricPool.Put(m)
}

var _ types.MetadataDatum = (*Metadata)(nil)

func (m Metadata) IsMeta() bool {
	return true
}

func (m Metadata) Bytes() []byte {
	return m.Buf
}

func (m Metadata) Type() types.Type {
	return types.PrometheusMetadataV1
}

func (m Metadata) FileFormat() types.FileFormat {
	return types.AlloyFileVersionV2
}

func (m Metadata) Free() {
	// noop
}
