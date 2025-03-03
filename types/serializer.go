package types

import (
	"context"
	"time"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
)

type SerializerConfig struct {
	// MaxSignalsInBatch controls what the max batch size is.
	MaxSignalsInBatch uint32
	// FlushFrequency controls how often to write to disk regardless of MaxSignalsInBatch.
	FlushFrequency time.Duration
}

type PrometheusSerializer interface {
	SendMetrics(ctx context.Context, l labels.Labels, t int64, v float64, h *histogram.Histogram, fh *histogram.FloatHistogram, externalLabels map[string]string) error
	SendMetadata(ctx context.Context, name string, unit string, help string, pType string) error
	Finished()
}
