package network

import (
	"github.com/grafana/walqueue/types"
	"github.com/prometheus/prometheus/prompb"
)

type seriesSlice struct {
	index int
	m     []prompb.TimeSeries
}

func (ss *seriesSlice) Add(t *types.Metric, external map[string]string) {
	if len(ss.m) <= ss.index {
		ss.m = append(ss.m, prompb.TimeSeries{})
	}
	// Why not pass the prompb.TimeSeries directly, this is because we want to reuse the prompb.timeseries in the slice
	// to reduce allocations.
	ss.m[ss.index] = toSeries(t, ss.m[ss.index], external)
	ss.index++
}

func (ss *seriesSlice) Len() int {
	return ss.index
}

func (ss *seriesSlice) Reset() {
	// Its possible we want to roll reset into slice.
	ss.index = 0
}

func (ss *seriesSlice) Slice() []prompb.TimeSeries {
	return ss.m[:ss.index]
}

type metaSlice struct {
	index int
	m     []prompb.MetricMetadata
}

func (ss *metaSlice) Add(t prompb.MetricMetadata) {
	if len(ss.m) <= ss.index {
		ss.m = append(ss.m, t)
	} else {
		ss.m[ss.index] = t
	}
	ss.index++
}

func (ss *metaSlice) Len() int {
	return ss.index
}

func (ss *metaSlice) Reset() {
	ss.index = 0
}
func (ss *metaSlice) Slice() []prompb.MetricMetadata {
	return ss.m[:ss.index]
}
