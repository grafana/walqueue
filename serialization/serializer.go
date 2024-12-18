package serialization

import (
	"context"
	"fmt"
	v1 "github.com/grafana/walqueue/types/v1"
	"github.com/grafana/walqueue/types/v2"
	"strconv"
	"time"

	"github.com/go-kit/log/level"

	snappy "github.com/eapache/go-xerial-snappy"
	"github.com/go-kit/log"
	"github.com/grafana/walqueue/types"
	"github.com/vladopajic/go-actor/actor"
)

// serializer collects data from multiple appenders in-memory and will periodically flush the data to file.Storage.
// serializer will flush based on configured time duration OR if it hits a certain number of items.
type serializer struct {
	inbox               actor.Mailbox[*types.Metric]
	metaInbox           actor.Mailbox[*types.Metric]
	cfgInbox            *types.SyncMailbox[types.SerializerConfig, bool]
	maxItemsBeforeFlush int
	flushFrequency      time.Duration
	queue               types.FileStorage
	lastFlush           time.Time
	logger              log.Logger
	self                actor.Actor
	// Every 1 second we should check if we need to flush.
	flushTestTimer *time.Ticker
	series         []*types.Metric
	meta           []*types.Metric
	msgpBuffer     []byte
	stats          func(stats types.SerializerStats)
	fileFormat     types.FileFormat
}

func NewSerializer(cfg types.SerializerConfig, q types.FileStorage, stats func(stats types.SerializerStats), ff types.FileFormat, l log.Logger) (types.Serializer, error) {
	s := &serializer{
		maxItemsBeforeFlush: int(cfg.MaxSignalsInBatch),
		flushFrequency:      cfg.FlushFrequency,
		queue:               q,
		series:              make([]*types.Metric, 0),
		logger:              l,
		inbox:               actor.NewMailbox[*types.Metric](),
		metaInbox:           actor.NewMailbox[*types.Metric](),
		cfgInbox:            types.NewSyncMailbox[types.SerializerConfig, bool](),
		flushTestTimer:      time.NewTicker(1 * time.Second),
		msgpBuffer:          make([]byte, 0),
		lastFlush:           time.Now(),
		stats:               stats,
		fileFormat:          ff,
	}

	return s, nil
}
func (s *serializer) Start() {
	// All the actors and mailboxes need to start.
	s.self = actor.Combine(actor.New(s), s.inbox, s.metaInbox, s.cfgInbox).Build()
	s.self.Start()
}

func (s *serializer) Stop() {
	s.self.Stop()
}

func (s *serializer) SendSeries(ctx context.Context, data *types.Metric) error {
	return s.inbox.Send(ctx, data)
}

func (s *serializer) SendMetadata(ctx context.Context, data *types.Metric) error {
	return s.metaInbox.Send(ctx, data)
}

func (s *serializer) UpdateConfig(ctx context.Context, cfg types.SerializerConfig) (bool, error) {
	return s.cfgInbox.Send(ctx, cfg)
}

func (s *serializer) DoWork(ctx actor.Context) actor.WorkerStatus {
	// Check for config which should have priority. Selector is random but since incoming
	// series will always have a queue by explicitly checking the config here we always give it a chance.
	// By pulling the config from the mailbox we ensure it does NOT need a mutex around access.
	select {
	case <-ctx.Done():
		return actor.WorkerEnd
	case cfg, ok := <-s.cfgInbox.ReceiveC():
		var err error
		var successful bool
		defer func() {
			cfg.Notify(successful, err)
		}()

		if !ok {
			err = fmt.Errorf("failed to receive configuration")
			return actor.WorkerEnd
		}
		successful = true
		s.maxItemsBeforeFlush = int(cfg.Value.MaxSignalsInBatch)
		s.flushFrequency = cfg.Value.FlushFrequency
		return actor.WorkerContinue
	default:
	}

	select {
	case <-ctx.Done():
		return actor.WorkerEnd
	case item, ok := <-s.inbox.ReceiveC():
		if !ok {
			return actor.WorkerEnd
		}
		s.series = append(s.series, item)
		// If we would go over the max size then send, or if we have hit the flush duration then send.
		if len(s.meta)+len(s.series) >= s.maxItemsBeforeFlush {
			err := s.flushToDisk(ctx)
			if err != nil {
				level.Error(s.logger).Log("msg", "unable to append to serializer", "err", err)
			}
		}

		return actor.WorkerContinue
	case item, ok := <-s.metaInbox.ReceiveC():
		if !ok {
			return actor.WorkerEnd
		}
		s.meta = append(s.meta, item)
		if len(s.meta)+len(s.series) >= s.maxItemsBeforeFlush {
			err := s.flushToDisk(ctx)
			if err != nil {
				level.Error(s.logger).Log("msg", "unable to append metadata to serializer", "err", err)
			}
		}
		return actor.WorkerContinue
	case <-s.flushTestTimer.C:
		if time.Since(s.lastFlush) > s.flushFrequency {
			err := s.flushToDisk(ctx)
			if err != nil {
				level.Error(s.logger).Log("msg", "unable to store data", "err", err)
			}
		}
		return actor.WorkerContinue
	}
}

func (s *serializer) flushToDisk(ctx actor.Context) error {
	var err error
	defer func() {
		s.lastFlush = time.Now()
		s.storeStats(err)
		s.series = s.series[:0]
		s.meta = s.meta[:0]
	}()
	// Do nothing if there is nothing.
	if len(s.series) == 0 && len(s.meta) == 0 {
		return nil
	}
	var buf []byte
	var ser types.Serialization
	switch s.fileFormat {
	case types.AlloyFileVersionV1:
		ser = v1.GetSerializer()
	case types.AlloyFileVersionV2:
		ser = v2.GetSerializer()
	default:
		return fmt.Errorf("invalid file format %s", s.fileFormat)
	}
	buf, err = ser.Serialize(s.series, s.meta)
	if err != nil {
		return err
	}
	out := snappy.Encode(buf)
	meta := map[string]string{
		// product.signal_type.schema.version
		"version":      string(s.fileFormat),
		"compression":  "snappy",
		"series_count": strconv.Itoa(len(s.series)),
		"meta_count":   strconv.Itoa(len(s.meta)),
	}
	err = s.queue.Store(ctx, meta, out)
	return err
}

func (s *serializer) storeStats(err error) {
	hasError := 0
	if err != nil {
		hasError = 1
	}
	newestTS := int64(0)
	for _, ts := range s.series {
		if ts.TS > newestTS {
			newestTS = ts.TS
		}
	}
	s.stats(types.SerializerStats{
		SeriesStored:           len(s.series),
		MetadataStored:         len(s.meta),
		Errors:                 hasError,
		NewestTimestampSeconds: time.UnixMilli(newestTS).Unix(),
	})
}
