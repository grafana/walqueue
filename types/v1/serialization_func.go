package v1

import (
	"bytes"
	"strconv"
	"sync"
	"unique"
	"unsafe"

	"github.com/grafana/walqueue/types"
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/tinylib/msgp/msgp"
	"go.uber.org/atomic"
)

func GetSerializer() *Serialization {
	return &Serialization{
		sg: &SeriesGroup{
			Series:   make([]*TimeSeriesBinary, 0),
			Metadata: make([]*TimeSeriesBinary, 0),
		},
		strMap: make(map[string]uint32),
	}
}

type Serialization struct {
	sg     *SeriesGroup
	strMap map[string]uint32
}

func (s *Serialization) AddPrometheusMetadata(name string, unit string, help string, pType string) error {
	// Metadata in v1 is not supported. Technically the plumbing was there but it required additional code changes to support.
	return nil
}

func (s *Serialization) AddPrometheusMetric(ts int64, value float64, lbls labels.Labels, h *histogram.Histogram, fh *histogram.FloatHistogram, _ exemplar.Exemplar, _ labels.Labels) error {
	tss := s.createTimeSeries(ts, value, lbls, h, fh)
	s.sg.Series = append(s.sg.Series, tss)
	return nil
}

func (s *Serialization) Unmarshal(_ map[string]string, buf []byte) (items []types.Datum, err error) {
	sg := &SeriesGroup{}
	return DeserializeToSeriesGroup(sg, buf)
}

func (s *Serialization) Marshal(handle func(map[string]string, []byte) error) error {
	defer func() {
		PutTimeSeriesSliceIntoPool(s.sg.Series)
		PutTimeSeriesSliceIntoPool(s.sg.Metadata)
	}()
	s.sg.Strings = make([]ByteString, len(s.strMap))
	for k, v := range s.strMap {
		s.sg.Strings[v] = ByteString(k)
	}
	meta := make(map[string]string)
	meta["series_count"] = strconv.Itoa(len(s.sg.Series))
	meta["meta_count"] = strconv.Itoa(len(s.sg.Metadata))
	meta["strings_count"] = strconv.Itoa(len(s.sg.Strings))
	buf, err := s.sg.MarshalMsg(nil)
	if err != nil {
		return err
	}
	return handle(meta, buf)
}

// createTimeSeries is what does the conversion from labels.Labels to LabelNames and
// LabelValues while filling in the string map, that is later converted to []string.
func (s *Serialization) createTimeSeries(t int64, value float64, lbls labels.Labels, h *histogram.Histogram, fh *histogram.FloatHistogram) *TimeSeriesBinary {
	ts := GetTimeSeriesFromPool()
	ts.LabelsNames = setSliceLength(ts.LabelsNames, lbls.Len())
	ts.LabelsValues = setSliceLength(ts.LabelsValues, lbls.Len())
	ts.TS = t
	ts.Value = value
	ts.Hash = lbls.Hash()
	if h != nil {
		ts.FromHistogram(t, h)
	}
	if fh != nil {
		ts.FromFloatHistogram(t, fh)
	}

	// This is where we deduplicate the ts.Labels into uint32 values
	// that map to a string in the strings slice via the index.
	i := 0
	lbls.Range(func(v labels.Label) {
		val, found := s.strMap[v.Name]
		if !found {
			val = uint32(len(s.strMap))
			s.strMap[v.Name] = val
		}
		ts.LabelsNames[i] = val

		val, found = s.strMap[v.Value]
		if !found {
			val = uint32(len(s.strMap))
			s.strMap[v.Value] = val
		}
		ts.LabelsValues[i] = val
		i++
	})
	return ts
}

// String returns the underlying bytes as a string without copying.
// This is a huge performance win with no side effect as long as
// the underlying byte slice is not reused. In this case
// it is not.
func (v ByteString) String() string {
	if len([]byte(v)) == 0 {
		return ""
	}
	return unsafe.String(&v[0], len([]byte(v)))
}

func (lh LabelHandles) Has(name string) bool {
	for _, l := range lh {
		if l.Name.Value() == name {
			return true
		}
	}
	return false
}

func (lh LabelHandles) Get(name string) string {
	for _, l := range lh {
		if l.Name.Value() == name {
			return l.Value.Value()
		}
	}
	return ""
}

func (h *Histogram) ToPromHistogram() prompb.Histogram {
	return prompb.Histogram{
		Count:          &prompb.Histogram_CountInt{CountInt: h.Count.IntValue},
		Sum:            h.Sum,
		Schema:         h.Schema,
		ZeroThreshold:  h.ZeroThreshold,
		ZeroCount:      &prompb.Histogram_ZeroCountInt{ZeroCountInt: h.ZeroCount.IntValue},
		NegativeSpans:  ToPromBucketSpans(h.NegativeSpans),
		NegativeDeltas: h.NegativeBuckets,
		PositiveSpans:  ToPromBucketSpans(h.PositiveSpans),
		PositiveDeltas: h.PositiveBuckets,
		ResetHint:      prompb.Histogram_ResetHint(h.ResetHint),
		Timestamp:      h.TimestampMillisecond,
	}
}

func (h *FloatHistogram) ToPromFloatHistogram() prompb.Histogram {
	return prompb.Histogram{
		Count:          &prompb.Histogram_CountFloat{CountFloat: h.Count.FloatValue},
		Sum:            h.Sum,
		Schema:         h.Schema,
		ZeroThreshold:  h.ZeroThreshold,
		ZeroCount:      &prompb.Histogram_ZeroCountFloat{ZeroCountFloat: h.ZeroCount.FloatValue},
		NegativeSpans:  ToPromBucketSpans(h.NegativeSpans),
		NegativeCounts: h.NegativeCounts,
		PositiveSpans:  ToPromBucketSpans(h.PositiveSpans),
		PositiveCounts: h.PositiveCounts,
		ResetHint:      prompb.Histogram_ResetHint(h.ResetHint),
		Timestamp:      h.TimestampMillisecond,
	}
}
func ToPromBucketSpans(bss []BucketSpan) []prompb.BucketSpan {
	spans := make([]prompb.BucketSpan, len(bss))
	for i, bs := range bss {
		spans[i] = bs.ToPromBucketSpan()
	}
	return spans
}

func (bs *BucketSpan) ToPromBucketSpan() prompb.BucketSpan {
	return prompb.BucketSpan{
		Offset: bs.Offset,
		Length: bs.Length,
	}
}

func (ts *TimeSeriesBinary) FromHistogram(timestamp int64, h *histogram.Histogram) {
	ts.Histograms.Histogram = &Histogram{
		Count:                HistogramCount{IsInt: true, IntValue: h.Count},
		Sum:                  h.Sum,
		Schema:               h.Schema,
		ZeroThreshold:        h.ZeroThreshold,
		ZeroCount:            HistogramZeroCount{IsInt: true, IntValue: h.ZeroCount},
		NegativeSpans:        FromPromSpan(h.NegativeSpans),
		NegativeBuckets:      h.NegativeBuckets,
		PositiveSpans:        FromPromSpan(h.PositiveSpans),
		PositiveBuckets:      h.PositiveBuckets,
		ResetHint:            int32(h.CounterResetHint),
		TimestampMillisecond: timestamp,
	}
}
func (ts *TimeSeriesBinary) FromFloatHistogram(timestamp int64, h *histogram.FloatHistogram) {
	ts.Histograms.FloatHistogram = &FloatHistogram{
		Count:                HistogramCount{IsInt: false, FloatValue: h.Count},
		Sum:                  h.Sum,
		Schema:               h.Schema,
		ZeroThreshold:        h.ZeroThreshold,
		ZeroCount:            HistogramZeroCount{IsInt: false, FloatValue: h.ZeroCount},
		NegativeSpans:        FromPromSpan(h.NegativeSpans),
		NegativeCounts:       h.NegativeBuckets,
		PositiveSpans:        FromPromSpan(h.PositiveSpans),
		PositiveCounts:       h.PositiveBuckets,
		ResetHint:            int32(h.CounterResetHint),
		TimestampMillisecond: timestamp,
	}
}
func FromPromSpan(spans []histogram.Span) []BucketSpan {
	bs := make([]BucketSpan, len(spans))
	for i, s := range spans {
		bs[i].Offset = s.Offset
		bs[i].Length = s.Length
	}
	return bs
}

func setSliceLength(lbls []uint32, length int) []uint32 {
	if cap(lbls) <= length {
		lbls = make([]uint32, length)
	} else {
		lbls = lbls[:length]
	}
	return lbls
}

var tsBinaryPool = sync.Pool{
	New: func() any {
		return &TimeSeriesBinary{}
	},
}

func GetTimeSeriesFromPool() *TimeSeriesBinary {
	OutStandingTimeSeriesBinary.Inc()
	return tsBinaryPool.Get().(*TimeSeriesBinary)
}

type LabelHandle struct {
	Name  unique.Handle[string]
	Value unique.Handle[string]
}

type LabelHandles []LabelHandle

func (v *ByteString) UnmarshalMsg(bts []byte) (o []byte, err error) {
	*v, o, err = msgp.ReadStringZC(bts)
	return o, err
}

var OutStandingTimeSeriesBinary = atomic.Int32{}

func PutTimeSeriesSliceIntoPool(tss []*TimeSeriesBinary) {
	for i := 0; i < len(tss); i++ {
		PutTimeSeriesIntoPool(tss[i])
	}
}

func PutTimeSeriesIntoPool(ts *TimeSeriesBinary) {
	OutStandingTimeSeriesBinary.Dec()
	ts.LabelsNames = ts.LabelsNames[:0]
	ts.LabelsValues = ts.LabelsValues[:0]
	ts.TS = 0
	ts.Value = 0
	ts.Hash = 0
	ts.Histograms.Histogram = nil
	ts.Histograms.FloatHistogram = nil
	tsBinaryPool.Put(ts)
}

// DeserializeToSeriesGroup transforms a buffer to a SeriesGroup and converts the stringmap + indexes into actual Labels.
func DeserializeToSeriesGroup(sg *SeriesGroup, buf []byte) ([]types.Datum, error) {
	nr := msgp.NewReader(bytes.NewReader(buf))
	err := sg.DecodeMsg(nr)
	if err != nil {
		return nil, err
	}
	metrics := make([]types.Datum, 0, len(sg.Series)+len(sg.Metadata))
	for _, series := range sg.Series {
		pm := prompb.TimeSeries{
			Labels: make([]prompb.Label, len(series.LabelsNames)),
		}
		for i := range series.LabelsNames {
			pm.Labels[i].Name = sg.Strings[series.LabelsNames[i]].String()
			pm.Labels[i].Value = sg.Strings[series.LabelsValues[i]].String()
		}

		if series.Histograms.Histogram == nil && series.Histograms.FloatHistogram == nil {
			pm.Samples = make([]prompb.Sample, 1)
			pm.Samples[0].Value = series.Value
			pm.Samples[0].Timestamp = series.TS
		}
		var isHistogram bool
		if series.Histograms.Histogram != nil || series.Histograms.FloatHistogram != nil {
			isHistogram = true
			pm.Histograms = make([]prompb.Histogram, 1)
			if series.Histograms.Histogram != nil {
				pm.Histograms[0] = series.Histograms.Histogram.ToPromHistogram()
			}
			if series.Histograms.FloatHistogram != nil {
				pm.Histograms[0] = series.Histograms.FloatHistogram.ToPromFloatHistogram()
			}
		}
		buf, err = pm.Marshal()
		if err != nil {
			return nil, err
		}
		metrics = append(metrics, &metric{
			hash:        series.Hash,
			ts:          series.TS,
			buf:         buf,
			isHistogram: isHistogram,
		})
	}
	for _, series := range sg.Metadata {
		pmm := prompb.MetricMetadata{}
		for i := range series.LabelsNames {
			name := sg.Strings[series.LabelsNames[i]].String()
			value := sg.Strings[series.LabelsValues[i]].String()
			switch name {
			case MetaUnit:
				pmm.Unit = value
			case MetaHelp:
				pmm.Help = value
			case MetaType:
				pmm.Unit = value
			case "__name__":
				pmm.MetricFamilyName = value
			}
		}
		metaBuf, err := pmm.Marshal()
		if err != nil {
			return nil, err
		}
		metrics = append(metrics, &metadata{buf: metaBuf})
	}
	return metrics, nil
}

var _ types.MetricDatum = (*metric)(nil)

type metric struct {
	buf         []byte
	hash        uint64
	ts          int64
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

func (m metric) Bytes() []byte {
	return m.buf
}

func (m metric) Type() types.Type {
	return types.PrometheusMetricV1
}

func (m metric) FileFormat() types.FileFormat {
	return types.AlloyFileVersionV1
}

func (m metric) Free() {
}

var _ types.MetadataDatum = (*metadata)(nil)

type metadata struct {
	buf []byte
}

func (m metadata) IsMeta() bool {
	return true
}

// Bytes represents the underlying data and should not be handled aside from
// Build* functions that understand the Type.
func (m metadata) Bytes() []byte {
	return m.buf
}

func (m metadata) Type() types.Type {
	return types.PrometheusMetadataV1
}

func (m metadata) FileFormat() types.FileFormat {
	return types.AlloyFileVersionV1
}

func (m metadata) Free() {
}
