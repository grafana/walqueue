package serialization

import (
	"context"
	"sync"
	"time"

	snappy "github.com/eapache/go-xerial-snappy"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/walqueue/types"
	v2 "github.com/grafana/walqueue/types/v2"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/vladopajic/go-actor/actor"
	"go.uber.org/atomic"
)

// serializer collects data from multiple appenders in-memory and will periodically flush the data to file.Storage.
// serializer will flush based on configured time duration OR if it hits a certain number of items.
// Because of how it interacts with an appender this is not an actor so all calls need to be wrapped with a mutex.
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
}

func NewSerializer(cfg types.SerializerConfig, q types.FileStorage, stats func(stats types.SerializerStats), ff types.FileFormat, l log.Logger) (types.PrometheusSerializer, error) {
	s := &serializer{
		maxItemsBeforeFlush: int(cfg.MaxSignalsInBatch),
		flushFrequency:      cfg.FlushFrequency,
		queue:               q,
		logger:              l,
		flushTestTimer:      time.NewTicker(1 * time.Second),
		lastFlush:           time.Now(),
		stats:               stats,
		fileFormat:          ff,
		// For now we only support writing v2. Note we fully support reading either version.
		ser: v2.NewMarshaller(),
	}

	return s, nil
}

// SendMetric adds a metric to the serializer. Note that we need to inject external labels here since once they are written to disk the prompb.TimeSeries bytes should be treated
// as immutable.
func (s *serializer) SendMetric(ctx context.Context, l labels.Labels, t int64, v float64, h *histogram.Histogram, fh *histogram.FloatHistogram, externalLabels map[string]string) error {
	s.mut.Lock()
	defer s.mut.Unlock()

	if t > s.newestTS {
		s.newestTS = t
	}

	err := s.ser.AddPrometheusMetric(t, v, l, h, fh, externalLabels)
	if err != nil {
		return err
	}
	s.seriesCount++
	// If we would go over the max size then send, or if we have hit the flush duration then send.
	if (s.seriesCount + s.metadataCount) > s.maxItemsBeforeFlush {
		err = s.flushToDisk(ctx)
		if err != nil {
			level.Error(s.logger).Log("msg", "unable to append to serializer", "err", err)
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

func (s *serializer) flushToDisk(ctx actor.Context) error {
	var err error
	defer func() {
		s.lastFlush = time.Now()
		s.storeStats(err)
	}()

	var out []byte
	err = s.ser.Marshal(func(meta map[string]string, buf []byte) error {
		meta["version"] = string(types.AlloyFileVersionV2)
		meta["compression"] = "snappy"
		// TODO: reusing a buffer here likely increases performance.
		out = snappy.Encode(buf)
		return s.queue.Store(ctx, meta, out)
	})
	return err
}

func (s *serializer) storeStats(err error) {
	defer func() {
		s.seriesCount = 0
		s.metadataCount = 0
	}()
	hasError := 0
	if err != nil {
		hasError = 1
	}

	s.stats(types.SerializerStats{
		SeriesStored:           s.seriesCount,
		MetadataStored:         s.metadataCount,
		Errors:                 hasError,
		NewestTimestampSeconds: time.UnixMilli(s.newestTS).Unix(),
	})
}
