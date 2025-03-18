package serialization

import (
	"context"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/golang/snappy"
	"github.com/grafana/walqueue/types"
	v2 "github.com/grafana/walqueue/types/v2"
	"github.com/klauspost/compress/zstd"
	"go.uber.org/atomic"
)

// serializer collects data from multiple appenders in-memory and will periodically flush the data to file.Storage.
// serializer will flush based on configured time duration OR if it hits a certain number of items.
// CompressionType defines the compression algorithm to use
type CompressionType string

const (
	// CompressionSnappy uses Snappy compression
	CompressionSnappy CompressionType = "snappy"
	// CompressionZstd uses Zstd compression
	CompressionZstd CompressionType = "zstd"
)

type serializer struct {
	mut                 sync.Mutex
	ser                 types.PrometheusMarshaller
	maxItemsBeforeFlush int
	flushFrequency      time.Duration
	queue               types.FileStorage
	lastFlush           time.Time
	logger              log.Logger
	// Every 1 second we should check if we need to flush.
	flushTestTimer *time.Ticker
	stats          func(stats types.SerializerStats)
	fileFormat     types.FileFormat
	needsStop      atomic.Bool
	newestTS       int64
	seriesCount    int
	metadataCount  int
	compression    CompressionType
	zstdEncoder    *zstd.Encoder
}

func NewSerializer(cfg types.SerializerConfig, q types.FileStorage, stats func(stats types.SerializerStats), l log.Logger) (types.PrometheusSerializer, error) {
	compression := CompressionSnappy
	if cfg.Compression != "" {
		switch CompressionType(cfg.Compression) {
		case CompressionSnappy:
			compression = CompressionSnappy
		case CompressionZstd:
			compression = CompressionZstd
		default:
			level.Warn(l).Log("msg", "unknown compression type, using snappy", "compression", cfg.Compression)
		}
	}

	s := &serializer{
		maxItemsBeforeFlush: int(cfg.MaxSignalsInBatch),
		flushFrequency:      cfg.FlushFrequency,
		queue:               q,
		logger:              l,
		flushTestTimer:      time.NewTicker(1 * time.Second),
		lastFlush:           time.Now(),
		stats:               stats,
		fileFormat:          types.AlloyFileVersionV2,
		ser:                 v2.NewFormat(),
		compression:         compression,
	}

	// Initialize zstd encoder if needed
	if compression == CompressionZstd {
		var err error
		s.zstdEncoder, err = zstd.NewWriter(nil)
		if err != nil {
			return nil, err
		}
	}

	return s, nil
}

// SendMetrics adds a slice metric to the serializer. Note that we need to inject external labels here since once they are written to disk the prompb.TimeSeries bytes should be treated
// as immutable.
func (s *serializer) SendMetrics(ctx context.Context, metrics []*types.PrometheusMetric, externalLabels map[string]string) error {
	s.mut.Lock()
	defer s.mut.Unlock()

	for _, m := range metrics {
		if m.T > s.newestTS {
			s.newestTS = m.T
		}

		err := s.ser.AddPrometheusMetric(m.T, m.V, m.L, m.H, m.FH, externalLabels)
		if err != nil {
			level.Error(s.logger).Log("msg", "error adding metric", "err", err)
			continue
		}
		s.seriesCount++
		// If we would go over the max size then send.
		if (s.seriesCount + s.metadataCount) > s.maxItemsBeforeFlush {
			err = s.flushToDisk(ctx)
			if err != nil {
				level.Error(s.logger).Log("msg", "unable to append to serializer", "err", err)
			}
		}

	}
	return nil

}

func (s *serializer) SendMetadata(_ context.Context, name string, unit string, help string, pType string) error {
	s.mut.Lock()
	defer s.mut.Unlock()

	err := s.ser.AddPrometheusMetadata(name, unit, help, pType)
	if err != nil {
		return err
	}
	// Theoretically we should check for flushing to disk but not concerned with it for metadata.
	s.metadataCount++
	return nil
}

// Start will start a go routine to handle writing data and checking if a flush needs to occur.
func (s *serializer) Start(ctx context.Context) error {
	check := func() {
		s.mut.Lock()
		defer s.mut.Unlock()
		if time.Since(s.lastFlush) > s.flushFrequency && (s.seriesCount+s.metadataCount) > 0 {
			err := s.flushToDisk(ctx)
			// We explicitly dont want to return an error here since we want things to keep running.
			if err != nil {
				level.Error(s.logger).Log("msg", "unable to store data", "err", err)
			}
		}
	}
	go func() {
		for {
			if s.needsStop.Load() {
				return
			}
			select {
			case <-s.flushTestTimer.C:
				check()
			case <-ctx.Done():
				return
			}
		}
	}()
	return nil
}

func (s *serializer) Stop() {
	s.needsStop.Store(true)
	
	// Close the zstd encoder if it exists
	if s.compression == CompressionZstd && s.zstdEncoder != nil {
		s.zstdEncoder.Close()
	}
}

func (s *serializer) UpdateConfig(_ context.Context, cfg types.SerializerConfig) (bool, error) {
	s.mut.Lock()
	defer s.mut.Unlock()

	s.maxItemsBeforeFlush = int(cfg.MaxSignalsInBatch)
	s.flushFrequency = cfg.FlushFrequency
	
	// Update compression type if specified
	if cfg.Compression != "" {
		newCompression := CompressionType(cfg.Compression)
		
		// Only update if different from current compression
		if s.compression != newCompression {
			// Close existing zstd encoder if needed
			if s.compression == CompressionZstd && s.zstdEncoder != nil {
				s.zstdEncoder.Close()
				s.zstdEncoder = nil
			}
			
			// Initialize new encoder if needed
			if newCompression == CompressionZstd {
				var err error
				s.zstdEncoder, err = zstd.NewWriter(nil)
				if err != nil {
					return false, err
				}
			}
			
			s.compression = newCompression
		}
	}
	
	return true, nil
}

func (s *serializer) flushToDisk(ctx context.Context) error {
	var err error
	uncompressed := 0
	compressed := 0

	defer func() {
		s.lastFlush = time.Now()
		s.storeStats(err, uncompressed, compressed)
	}()

	var out []byte
	err = s.ser.Marshal(func(meta map[string]string, buf []byte) error {
		uncompressed = len(buf)
		meta["version"] = string(types.AlloyFileVersionV2)
		
		if s.compression == CompressionZstd {
			meta["compression"] = string(CompressionZstd)
			out = s.zstdEncoder.EncodeAll(buf, nil)
		} else {
			// Default to snappy
			meta["compression"] = string(CompressionSnappy)
			// TODO: reusing a buffer here likely increases performance.
			out = snappy.Encode(nil, buf)
		}
		
		compressed = len(out)
		return s.queue.Store(ctx, meta, out)
	})
	return err
}

func (s *serializer) storeStats(err error, uncompressed int, compressed int) {
	defer func() {
		s.seriesCount = 0
		s.metadataCount = 0
	}()
	hasError := 0
	if err != nil {
		hasError = 1
	}

	s.stats(types.SerializerStats{
		SeriesStored:             s.seriesCount,
		MetadataStored:           s.metadataCount,
		Errors:                   hasError,
		NewestTimestampSeconds:   time.UnixMilli(s.newestTS).Unix(),
		UncompressedBytesWritten: uncompressed,
		CompressedBytesWritten:   compressed,
	})
}
