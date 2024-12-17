//go:generate msgp
//msgp:unmarshal ignore ByteString
//msgp:marshal ignore ByteString

package v2

import (
	"github.com/prometheus/prometheus/model/labels"
)

const MetaType = "__alloy_metadata_type__"
const MetaUnit = "__alloy_metadata_unit__"
const MetaHelp = "__alloy_metadata_help__"

// SeriesGroup is the holder for TimeSeries, Metadata, and the strings array.
// When serialized the Labels Key,Value array will be transformed into
// LabelNames and LabelsValues that point to the index in Strings.
// This deduplicates the strings and decreases the size on disk.
//
//msgp:tuple SeriesGroup
type SeriesGroup struct {
	Strings  []string
	Series   []*TimeSeriesBinary
	Metadata []*TimeSeriesBinary
}

// TimeSeriesBinary is an optimized format for handling metrics and metadata. It should never be instantiated directly
// but instead use GetTimeSeriesFromPool and PutTimeSeriesSliceIntoPool. This allows us to reuse these objects and avoid
// allocations.
//
//msgp:tuple TimeSeriesBinary
type TimeSeriesBinary struct {
	// Labels are not serialized to msgp, instead we store separately a dictionary of strings and use `LabelNames` and `LabelValues` to refer to the dictionary by ID.
	Labels       labels.Labels `msg:"-"`
	LabelsNames  []uint32
	LabelsValues []uint32
	// TS is unix milliseconds.
	TS         int64
	Value      float64
	Hash       uint64
	Histograms *Histograms
}

type Histograms struct {
	Histogram      *Histogram
	FloatHistogram *FloatHistogram
}

type Histogram struct {
	Count                HistogramCount
	Sum                  float64
	Schema               int32
	ZeroThreshold        float64
	ZeroCount            HistogramZeroCount
	NegativeSpans        []BucketSpan
	NegativeBuckets      []int64
	NegativeCounts       []float64
	PositiveSpans        []BucketSpan
	PositiveBuckets      []int64
	PositiveCounts       []float64
	ResetHint            int32
	TimestampMillisecond int64
}

type FloatHistogram struct {
	Count                HistogramCount
	Sum                  float64
	Schema               int32
	ZeroThreshold        float64
	ZeroCount            HistogramZeroCount
	NegativeSpans        []BucketSpan
	NegativeDeltas       []int64
	NegativeCounts       []float64
	PositiveSpans        []BucketSpan
	PositiveDeltas       []int64
	PositiveCounts       []float64
	ResetHint            int32
	TimestampMillisecond int64
}

type HistogramCount struct {
	IsInt      bool
	IntValue   uint64
	FloatValue float64
}

type HistogramZeroCount struct {
	IsInt      bool
	IntValue   uint64
	FloatValue float64
}

type BucketSpan struct {
	Offset int32
	Length uint32
}
