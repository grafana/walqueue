package serialization

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/golang/snappy"
	"github.com/grafana/walqueue/types"
)

// OtelSerializer handles serialization of OpenTelemetry signals
type OtelSerializer struct {
	mu sync.RWMutex

	storage      types.FileStorage
	maxBatchSize int64
	logger       log.Logger
	timeout      time.Duration

	// Metrics for tracking serializer state
	seriesStored   int
	metadataStored int
	errors         int
	newestTS       int64
}

// NewOtelSerializer creates a new OtelSerializer
func NewOtelSerializer(storage types.FileStorage, maxBatchSize int64, timeout time.Duration, logger log.Logger) *OtelSerializer {
	return &OtelSerializer{
		storage:      storage,
		maxBatchSize: maxBatchSize,
		logger:       logger,
		timeout:      timeout,
	}
}

// Start starts the serializer
func (s *OtelSerializer) Start(ctx context.Context) {
	s.storage.Start(ctx)
}

// Stop stops the serializer
func (s *OtelSerializer) Stop() {
	s.storage.Stop()
}

// Write writes a batch of OpenTelemetry signals
func (s *OtelSerializer) Write(ctx context.Context, items []types.Datum) error {
	if len(items) == 0 {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Marshal items into a buffer
	data, err := s.marshalItems(items)
	if err != nil {
		s.errors++
		return fmt.Errorf("failed to marshal items: %w", err)
	}

	// Compress the data
	compressed := snappy.Encode(nil, data)

	// Store with metadata
	meta := map[string]string{
		"version":     string(types.OtelFileVersionV3),
		"compression": "snappy",
	}

	if err := s.storage.Store(ctx, meta, compressed); err != nil {
		s.errors++
		return fmt.Errorf("failed to store batch: %w", err)
	}

	s.seriesStored += len(items)
	return nil
}

func (s *OtelSerializer) marshalItems(items []types.Datum) ([]byte, error) {
	// Write number of items
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(len(items)))

	// Write each item's type and data
	for _, item := range items {
		// Write type
		typeBytes := []byte(item.Type())
		typeLen := uint32(len(typeBytes))
		typeLenBytes := make([]byte, 4)
		binary.LittleEndian.PutUint32(typeLenBytes, typeLen)
		buf = append(buf, typeLenBytes...)
		buf = append(buf, typeBytes...)

		// Write data
		data := item.Bytes()
		dataLen := uint32(len(data))
		dataLenBytes := make([]byte, 4)
		binary.LittleEndian.PutUint32(dataLenBytes, dataLen)
		buf = append(buf, dataLenBytes...)
		buf = append(buf, data...)
	}

	return buf, nil
}

// Stats returns current statistics about the serializer
func (s *OtelSerializer) Stats() types.SerializerStats {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return types.SerializerStats{
		SeriesStored:           s.seriesStored,
		MetadataStored:         s.metadataStored,
		Errors:                 s.errors,
		NewestTimestampSeconds: s.newestTS,
	}
}
