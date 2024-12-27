package v2

import "github.com/tinylib/msgp/msgp"

// TODO @mattdurham these func files are messy.
import (
	"github.com/prometheus/prometheus/model/histogram"
	"unique"
	"unsafe"

	"github.com/dolthub/swiss"
	"github.com/grafana/walqueue/types"
	"github.com/prometheus/prometheus/model/labels"
)

func (v *ByteString) UnmarshalMsg(bts []byte) (o []byte, err error) {
	*v, o, err = msgp.ReadStringZC(bts)
	return o, err
}

func (v *ByteString) MarshalMsg(bts []byte) (o []byte, err error) {
	buf := msgp.AppendString(bts, v.String())
	return buf, nil
}

func (v *ByteString) Msgsize() int {
	return msgp.StringPrefixSize + len(*v)
}

func (v *ByteString) DecodeMsg(dc *msgp.Reader) (err error) {
	s, err := dc.ReadStringAsBytes(nil)
	if err != nil {
		return err
	}
	*v = s
	return nil
}

func (v *ByteString) EncodeMsg(en *msgp.Writer) (err error) {
	return en.WriteString(v.String())
}

func (v ByteString) String() string {
	if len([]byte(v)) == 0 {
		return ""
	}
	return unsafe.String(&v[0], len([]byte(v)))
}

// fillTimeSeries is what does the conversion from labels.Labels to LabelNames and
// LabelValues while filling in the string map, that is later converted to []string.
func fillTimeSeries(ts *TimeSeriesBinary, m *types.Metric, strMapToInt *swiss.Map[string, uint32]) *TimeSeriesBinary {
	if cap(ts.LabelsNames) < len(m.Labels) {
		ts.LabelsNames = make([]uint32, len(m.Labels))
	} else {
		ts.LabelsNames = ts.LabelsNames[:len(m.Labels)]
	}

	if cap(ts.LabelsValues) < len(m.Labels) {
		ts.LabelsValues = make([]uint32, len(m.Labels))
	} else {
		ts.LabelsValues = ts.LabelsValues[:len(m.Labels)]
	}

	ts.TS = m.TS
	ts.Hash = m.Hash
	ts.Value = m.Value
	if m.Histogram != nil {
		ts.FromHistogram(m.TS, m.Histogram)
	}
	if m.FloatHistogram != nil {
		ts.FromFloatHistogram(m.TS, m.FloatHistogram)
	}

	// This is where we deduplicate the ts.Labels into uint32 values
	// that map to a string in the strings slice via the index.
	for i, v := range m.Labels {
		val, found := strMapToInt.Get(v.Name)
		if !found {
			val = uint32(strMapToInt.Count())
			strMapToInt.Put(v.Name, val)
		}
		ts.LabelsNames[i] = val

		val, found = strMapToInt.Get(v.Value)
		if !found {
			val = uint32(strMapToInt.Count())
			strMapToInt.Put(v.Value, val)
		}
		ts.LabelsValues[i] = val
	}
	return ts
}

type LabelHandle struct {
	Name  unique.Handle[string]
	Value unique.Handle[string]
}

type LabelHandles []LabelHandle

// DeserializeToSeriesGroup transforms a buffer to a SeriesGroup and converts the stringmap + indexes into actual Labels.
func DeserializeToSeriesGroup(sg *SeriesGroup, buf []byte) (*types.Metrics, *types.Metrics, error) {
	buf, err := sg.UnmarshalMsg(buf)
	if err != nil {
		return nil, nil, err
	}
	metrics := &types.Metrics{}
	metrics.Resize(len(sg.Series))

	// Need to fill in the labels.
	for seriesIndex, series := range sg.Series {
		metric := metrics.M[seriesIndex]
		if cap(metric.Labels) < len(series.LabelsNames) {
			metric.Labels = make(labels.Labels, len(series.LabelsNames))
		} else {
			metric.Labels = metric.Labels[:len(series.LabelsNames)]
		}
		metric.TS = series.TS
		metric.Value = series.Value
		metric.Hash = series.Hash
		if series.Histogram != nil {
			metric.Histogram = ConvertToHistogram(series.Histogram)
		}
		if series.FloatHistogram != nil {
			metric.FloatHistogram = ConvertToFloatHistogram(series.FloatHistogram)
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

	metadata := &types.Metrics{}
	metadata.Resize(len(sg.Metadata))
	for seriesIndex, series := range sg.Metadata {
		meta := metadata.M[seriesIndex]

		if cap(meta.Labels) < len(series.LabelsNames) {
			meta.Labels = make([]labels.Label, len(series.LabelsNames))
		} else {
			meta.Labels = meta.Labels[:len(series.LabelsNames)]
		}
		for i := range series.LabelsNames {
			meta.Labels[i] = labels.Label{
				Name:  sg.Strings[series.LabelsNames[i]].String(),
				Value: sg.Strings[series.LabelsValues[i]].String(),
			}
		}
		// Finally ensure we reset the labelnames and labelvalues.
		series.LabelsNames = series.LabelsNames[:0]
		series.LabelsValues = series.LabelsValues[:0]
	}
	sg.Strings = sg.Strings[:0]
	return metrics, metadata, err
}

// ConvertToHistogram converts a local Histogram to histogram.Histogram
func ConvertToHistogram(h *Histogram) *histogram.Histogram {
	if h == nil {
		return nil
	}

	// Convert spans
	negativeSpans := make([]histogram.Span, len(h.NegativeSpans))
	for i, span := range h.NegativeSpans {
		negativeSpans[i] = histogram.Span{
			Offset: span.Offset,
			Length: span.Length,
		}
	}

	positiveSpans := make([]histogram.Span, len(h.PositiveSpans))
	for i, span := range h.PositiveSpans {
		positiveSpans[i] = histogram.Span{
			Offset: span.Offset,
			Length: span.Length,
		}
	}

	// Determine the count type
	var count int64
	if h.Count.IsInt {
		count = int64(h.Count.IntValue)
	} else {
		count = int64(h.Count.FloatValue)
	}

	var zeroCount int64
	if h.ZeroCount.IsInt {
		zeroCount = int64(h.ZeroCount.IntValue)
	} else {
		zeroCount = int64(h.ZeroCount.FloatValue)
	}

	return &histogram.Histogram{
		Count:            uint64(count),
		Sum:              h.Sum,
		Schema:           h.Schema,
		ZeroThreshold:    h.ZeroThreshold,
		ZeroCount:        uint64(zeroCount),
		NegativeSpans:    negativeSpans,
		NegativeBuckets:  h.NegativeBuckets,
		PositiveSpans:    positiveSpans,
		PositiveBuckets:  h.PositiveBuckets,
		CounterResetHint: histogram.CounterResetHint(h.ResetHint),
	}
}

// ConvertToFloatHistogram converts a local FloatHistogram to histogram.FloatHistogram
func ConvertToFloatHistogram(h *FloatHistogram) *histogram.FloatHistogram {
	if h == nil {
		return nil
	}

	// Convert spans
	negativeSpans := make([]histogram.Span, len(h.NegativeSpans))
	for i, span := range h.NegativeSpans {
		negativeSpans[i] = histogram.Span{
			Offset: span.Offset,
			Length: span.Length,
		}
	}

	positiveSpans := make([]histogram.Span, len(h.PositiveSpans))
	for i, span := range h.PositiveSpans {
		positiveSpans[i] = histogram.Span{
			Offset: span.Offset,
			Length: span.Length,
		}
	}

	// Determine the count type
	var count float64
	if h.Count.IsInt {
		count = float64(h.Count.IntValue)
	} else {
		count = h.Count.FloatValue
	}

	var zeroCount float64
	if h.ZeroCount.IsInt {
		zeroCount = float64(h.ZeroCount.IntValue)
	} else {
		zeroCount = h.ZeroCount.FloatValue
	}

	return &histogram.FloatHistogram{
		Count:            count,
		Sum:              h.Sum,
		Schema:           h.Schema,
		ZeroThreshold:    h.ZeroThreshold,
		ZeroCount:        zeroCount,
		NegativeSpans:    negativeSpans,
		NegativeBuckets:  h.NegativeCounts,
		PositiveSpans:    positiveSpans,
		PositiveBuckets:  h.PositiveCounts,
		CounterResetHint: histogram.CounterResetHint(h.ResetHint),
	}
}

func (ts *TimeSeriesBinary) FromHistogram(timestamp int64, h *histogram.Histogram) {
	ts.Histogram = &Histogram{
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
	ts.FloatHistogram = &FloatHistogram{
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
