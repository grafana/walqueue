package types

import (
	"context"
	"time"
)

type FileFormat string

const AlloyFileVersionV1 = FileFormat("alloy.metrics.queue.v1")

// AlloyFileVersionV2 uses msgp, but instead of string keys uses field names like tuples and doesn't always
// save/restore histograms if they are nil. This can have up to 3x increase versus v1.
const AlloyFileVersionV2 = FileFormat("alloy.metrics.queue.v2")

// AlloyFileVersionV3 is similiar to v2 but doesnt do any dictionary encoding.
const AlloyFileVersionV3 = FileFormat("alloy.metrics.queue.v3")

type SerializerConfig struct {
	// MaxSignalsInBatch controls what the max batch size is.
	MaxSignalsInBatch uint32
	// FlushFrequency controls how often to write to disk regardless of MaxSignalsInBatch.
	FlushFrequency time.Duration
}

// Serializer handles converting a set of signals into a binary representation to be written to storage.
type Serializer interface {
	Start()
	Stop()
	SendSeries(ctx context.Context, data *Metric) error
	SendMetadata(ctx context.Context, data *Metric) error
	UpdateConfig(ctx context.Context, cfg SerializerConfig) (bool, error)
}
