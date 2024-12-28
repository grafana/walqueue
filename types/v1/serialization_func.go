package v1

import (
	"bytes"
	"sync"
	"unique"
	"unsafe"

	"github.com/grafana/walqueue/types"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/tinylib/msgp/msgp"
	"go.uber.org/atomic"
)

func GetSerializer() types.Serialization {
	return &Serialization{}
}

type Serialization struct{}

func (s *Serialization) Serialize(metrics *types.Metrics, metadata *types.Metrics, handle func([]byte)) error {
	sg := &SeriesGroup{}
	buf := make([]byte, 0)
	if metrics == nil {
		metrics = &types.Metrics{M: make([]*types.Metric, 0)}
	}
	if metadata == nil {
		metadata = &types.Metrics{M: make([]*types.Metric, 0)}
	}
	sg.Series = make([]*TimeSeriesBinary, 0, len(metrics.M))
	sg.Metadata = make([]*TimeSeriesBinary, 0, len(metadata.M))
	strMapToIndex := make(map[string]uint32, (len(metrics.M)+len(metadata.M))*10)

	defer func() {
		PutTimeSeriesSliceIntoPool(sg.Series)
		PutTimeSeriesSliceIntoPool(sg.Metadata)
	}()
	for _, m := range metrics.M {
		ts := createTimeSeries(m, strMapToIndex)
		sg.Series = append(sg.Series, ts)
	}
	for _, m := range metadata.M {
		ts := createTimeSeries(m, strMapToIndex)
		sg.Metadata = append(sg.Metadata, ts)
	}
	stringsSlice := make([]ByteString, len(strMapToIndex))
	for stringValue, index := range strMapToIndex {
		stringsSlice[index] = ByteString(stringValue)
	}
	sg.Strings = stringsSlice
	buf, err := sg.MarshalMsg(buf)
	if err != nil {
		return err
	}
	handle(buf)
	return nil

}

func (s *Serialization) Deserialize(i []byte) (metrics *types.Metrics, metadata *types.Metrics, err error) {
	sg := &SeriesGroup{}
	return DeserializeToSeriesGroup(sg, i)
}

// createTimeSeries is what does the conversion from labels.Labels to LabelNames and
// LabelValues while filling in the string map, that is later converted to []string.
func createTimeSeries(m *types.Metric, strMapToInt map[string]uint32) *TimeSeriesBinary {
	ts := GetTimeSeriesFromPool()
	ts.LabelsNames = setSliceLength(ts.LabelsNames, len(m.Labels))
	ts.LabelsValues = setSliceLength(ts.LabelsValues, len(m.Labels))

	// This is where we deduplicate the ts.Labels into uint32 values
	// that map to a string in the strings slice via the index.
	for i, v := range m.Labels {
		val, found := strMapToInt[v.Name]
		if !found {
			val = uint32(len(strMapToInt))
			strMapToInt[v.Name] = val
		}
		ts.LabelsNames[i] = val

		val, found = strMapToInt[v.Value]
		if !found {
			val = uint32(len(strMapToInt))
			strMapToInt[v.Value] = val
		}
		ts.LabelsValues[i] = val
	}
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
func DeserializeToSeriesGroup(sg *SeriesGroup, buf []byte) (*types.Metrics, *types.Metrics, error) {
	nr := msgp.NewReader(bytes.NewReader(buf))
	err := sg.DecodeMsg(nr)
	if err != nil {
		return nil, nil, err
	}
	// Need to fill in the labels.
	metrics := &types.Metrics{}
	metrics.Resize(len(sg.Series), true)
	for seriesIndex, series := range sg.Series {
		metric := metrics.M[seriesIndex]
		if cap(metric.Labels) < len(series.LabelsNames) {
			metric.Labels = make(labels.Labels, len(series.LabelsNames))
		} else {
			metric.Labels = metric.Labels[:len(series.LabelsNames)]
		}
		// Since the LabelNames/LabelValues are indexes into the Strings slice we can access it like the below.
		// 1 Label corresponds to two entries, one in LabelsNames and one in LabelsValues.
		for i := range series.LabelsNames {
			metric.Labels[i] = labels.Label{
				Name:  sg.Strings[series.LabelsNames[i]].String(),
				Value: sg.Strings[series.LabelsValues[i]].String(),
			}
		}
		series.LabelsNames = series.LabelsNames[:0]
		series.LabelsValues = series.LabelsValues[:0]
	}
	meta := &types.Metrics{}
	meta.Resize(len(sg.Metadata), true)
	for seriesIndex, series := range sg.Metadata {
		m := meta.M[seriesIndex]
		if cap(m.Labels) < len(series.LabelsNames) {
			m.Labels = make([]labels.Label, len(series.LabelsNames))
		} else {
			m.Labels = m.Labels[:len(series.LabelsNames)]
		}
		for i := range series.LabelsNames {
			m.Labels[i] = labels.Label{
				Name:  sg.Strings[series.LabelsNames[i]].String(),
				Value: sg.Strings[series.LabelsValues[i]].String(),
			}
		}
		// Finally ensure we reset the labelnames and labelvalues.
		series.LabelsNames = series.LabelsNames[:0]
		series.LabelsValues = series.LabelsValues[:0]
	}

	sg.Strings = sg.Strings[:0]
	return metrics, meta, err
}
