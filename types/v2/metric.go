package v2

import "github.com/grafana/walqueue/types"

var _ types.MetricDatum = (*metric)(nil)

type metric struct {
	hash        uint64
	ts          int64
	buf         []byte
	isHistogram bool
}

func (m metric) Hash() uint64 {
	return m.hash
}

func (m metric) TimeStampMS() int64 {
	return m.ts
}

func (m metric) IsHistogram() bool {
	return m.isHistogram
}

// Bytes represents the underlying data and should not be handled aside from
// Build* functions that understand the Type.
func (m metric) Bytes() []byte {
	return m.buf
}

func (m metric) Type() types.Type {
	return types.PrometheusMetricV1
}

func (m metric) FileFormat() types.FileFormat {
	return types.AlloyFileVersionV2
}

func (m *metric) Free() {
	datumPool.Put(m)
}

var _ types.MetadataDatum = (*metadata)(nil)

type metadata struct {
	buf  []byte
	name string
}

func (m metadata) IsMeta() bool {
	return true
}

func (m metadata) Bytes() []byte {
	return m.buf
}

func (m metadata) Type() types.Type {
	return types.PrometheusMetadataV1
}

func (m metadata) FileFormat() types.FileFormat {
	return types.AlloyFileVersionV2
}

func (m metadata) Free() {
	// noop
}
