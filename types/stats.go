package types

import (
	"time"
)

// TODO @mattdurham separate this into more manageable chunks, and likely 3 stats series: series, metadata and new ones.

type SerializerStats struct {
	SeriesStored           int
	MetadataStored         int
	Errors                 int
	NewestTimestampSeconds int64
	TTLDropped             int
}

type NetworkStats struct {
	Series                 CategoryStats
	Histogram              CategoryStats
	Metadata               CategoryStats
	SendDuration           time.Duration
	NewestTimestampSeconds int64
	SeriesBytes            int
	MetadataBytes          int
}

func (ns NetworkStats) TotalSent() int {
	return ns.Series.SeriesSent + ns.Histogram.SeriesSent + ns.Metadata.SeriesSent
}

func (ns NetworkStats) TotalRetried() int {
	return ns.Series.RetriedSamples + ns.Histogram.RetriedSamples + ns.Metadata.RetriedSamples
}

func (ns NetworkStats) TotalFailed() int {
	return ns.Series.FailedSamples + ns.Histogram.FailedSamples + ns.Metadata.FailedSamples
}

func (ns NetworkStats) Total429() int {
	return ns.Series.RetriedSamples429 + ns.Histogram.RetriedSamples429 + ns.Metadata.RetriedSamples429
}

func (ns NetworkStats) Total5XX() int {
	return ns.Series.RetriedSamples5XX + ns.Histogram.RetriedSamples5XX + ns.Metadata.RetriedSamples5XX
}

type CategoryStats struct {
	RetriedSamples       int
	RetriedSamples429    int
	RetriedSamples5XX    int
	SeriesSent           int
	FailedSamples        int
	TTLDroppedSamples    int
	NetworkSamplesFailed int
}
