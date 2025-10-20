package serialization

import (
	"context"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/walqueue/types"
	v2 "github.com/grafana/walqueue/types/v2"
	"github.com/klauspost/compress/zstd"
	"github.com/prometheus/prometheus/model/labels"
	"go.uber.org/atomic"
)

var (
	// zstdEncoder is a reusable encoder for zstd compression
	zstdEncoder, _ = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest))
)

// serializer collects data from multiple appenders in-memory and will periodically flush the data to file.Storage.
// serializer will flush based on configured time duration OR if it hits a certain number of items.
type serializer struct {
	lastFlush           time.Time
	logger              log.Logger
	ser                 types.PrometheusMarshaller
	queue               types.FileStorage
	stats               func(stats types.SerializerStats)
	flushTestTimer      *time.Ticker
	fileFormat          types.FileFormat
	flushFrequency      time.Duration
	maxItemsBeforeFlush int
	needsStop           atomic.Bool
	newestTS            int64
	seriesCount         int
	metadataCount       int
	exemplarCount       int
	mut                 sync.Mutex
}

func NewSerializer(cfg types.SerializerConfig, q types.FileStorage, stats func(stats types.SerializerStats), l log.Logger) (types.PrometheusSerializer, error) {
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
	}

	return s, nil
}

// SendMetrics adds a slice metric to the serializer. Note that we need to inject external labels here since once they are written to disk the prompb.TimeSeries bytes should be treated
// as immutable.
func (s *serializer) SendMetrics(ctx context.Context, metrics []*types.PrometheusMetric, externalLabels labels.Labels) error {
	s.mut.Lock()
	defer s.mut.Unlock()

	for _, m := range metrics {
		if m.T > s.newestTS {
			s.newestTS = m.T
		}

		err := s.ser.AddPrometheusMetric(m.T, m.V, m.L, m.H, m.FH, m.E, externalLabels)
		if err != nil {
			level.Error(s.logger).Log("msg", "error adding metric", "err", err)
			continue
		}
		s.seriesCount++
		if m.E.Labels.Len() > 0 {
			s.exemplarCount++
		}
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
}

func (s *serializer) UpdateConfig(_ context.Context, cfg types.SerializerConfig) (bool, error) {
	s.mut.Lock()
	defer s.mut.Unlock()

	s.maxItemsBeforeFlush = int(cfg.MaxSignalsInBatch)
	s.flushFrequency = cfg.FlushFrequency
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
		meta["compression"] = "zstd"
		// Use zstd for compression
		out = zstdEncoder.EncodeAll(buf, nil)
		compressed = len(out)
		return s.queue.Store(ctx, meta, out)
	})
	return err
}

func (s *serializer) storeStats(err error, uncompressed int, compressed int) {
	defer func() {
		s.seriesCount = 0
		s.metadataCount = 0
		s.exemplarCount = 0
	}()
	hasError := 0
	if err != nil {
		hasError = 1
	}

	s.stats(types.SerializerStats{
		SeriesStored:             s.seriesCount,
		MetadataStored:           s.metadataCount,
		ExemplarsStored:          s.exemplarCount,
		Errors:                   hasError,
		NewestTimestampSeconds:   time.UnixMilli(s.newestTS).Unix(),
		UncompressedBytesWritten: uncompressed,
		CompressedBytesWritten:   compressed,
	})
}
