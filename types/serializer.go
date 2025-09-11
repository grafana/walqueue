package types

import (
	"context"
	"time"

	"github.com/prometheus/prometheus/model/labels"
)

type SerializerConfig struct {
	// MaxSignalsInBatch controls what the max batch size is.
	MaxSignalsInBatch uint32
	// FlushFrequency controls how often to write to disk regardless of MaxSignalsInBatch.
	FlushFrequency time.Duration
}

// Serializer handles converting a set of signals into a binary representation to be written to storage.
type Serializer interface {
	Start(ctx context.Context) error
	Stop()
	UpdateConfig(ctx context.Context, cfg SerializerConfig) (bool, error)
}

type Sender interface {
	SendMetrics(ctx context.Context, metrics []*PrometheusMetric, externalLabels labels.Labels) error
	SendMetadata(ctx context.Context, name string, unit string, help string, pType string) error
}

type PrometheusSerializer interface {
	Serializer
	Sender
}
