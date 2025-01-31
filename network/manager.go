package network

import (
	"context"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/walqueue/types"
	"golang.design/x/chann"
	"time"
)

// manager manages writeBuffers. Mostly it exists to control their lifecycle and send work to them.
type manager struct {
	writeBuffers     []*writeBuffer[types.MetricDatum]
	metadata         *writeBuffer[types.MetadataDatum]
	logger           log.Logger
	inbox            *types.Mailbox[types.MetricDatum]
	metaInbox        *types.Mailbox[types.MetadataDatum]
	configInbox      *types.SyncMailbox[types.ConnectionConfig, bool]
	cfg              types.ConnectionConfig
	stats            func(types.NetworkStats)
	metaStats        func(types.NetworkStats)
	bufferedMetric   types.MetricDatum
	bufferedMetadata types.MetadataDatum
	lastFlushTime    time.Time
}

var _ types.NetworkClient = (*manager)(nil)

func New(cc types.ConnectionConfig, logger log.Logger, seriesStats, metadataStats func(types.NetworkStats)) (types.NetworkClient, error) {
	s := &manager{
		writeBuffers: make([]*writeBuffer[types.MetricDatum], 0, cc.Connections),
		logger:       logger,
		// This provides blocking to only handle one at a time, so that if a queue blocks
		// it will stop the filequeue from feeding more. Without passing true the minimum is actually 64 instead of 1.
		inbox:            types.NewMailbox[types.MetricDatum](chann.Cap(1)),
		metaInbox:        types.NewMailbox[types.MetadataDatum](chann.Cap(1)),
		bufferedMetric:   nil,
		bufferedMetadata: nil,
		configInbox:      types.NewSyncMailbox[types.ConnectionConfig, bool](),
		stats:            seriesStats,
		metaStats:        metadataStats,
		cfg:              cc,
		lastFlushTime:    time.Now(),
	}

	// start kicks off a number of concurrent connections.
	for i := uint(0); i < s.cfg.Connections; i++ {
		l := newWriteBuffer[types.MetricDatum](cc, seriesStats, false, logger)
		s.writeBuffers = append(s.writeBuffers, l)
	}

	metadata := newWriteBuffer[types.MetadataDatum](cc, seriesStats, true, logger)
	s.metadata = metadata
	return s, nil
}

func (s *manager) Start(ctx context.Context) {
	s.configInbox.Start()
	go s.Run(ctx)
}

func (s *manager) SendSeries(ctx context.Context, data types.MetricDatum) error {
	return s.inbox.Send(ctx, data)
}

func (s *manager) SendMetadata(ctx context.Context, data types.MetadataDatum) error {
	return s.metaInbox.Send(ctx, data)
}

func (s *manager) UpdateConfig(ctx context.Context, cc types.ConnectionConfig) (bool, error) {
	return s.configInbox.Send(ctx, cc)
}

type flowcontrol int

const (
	Restart flowcontrol = iota
	Exit
	ContinueExecution
)

func (s *manager) Run(ctx context.Context) {
	// This is the primary run loop for the manager since it is no longer an actor.
	for {

		// CheckConfig is a priority to check the config. If no changes are found will default out
		// and return ContinueExecution
		flow := s.checkConfig(ctx)
		if flow == Exit {
			return
		}
		// Flush will check to see if we haven't sent data since the last flush.
		s.flushCheck(ctx)

		// The buffered checks are for when we could NOT add a metric to the write buffer.
		// In that case we CANNOT pull a new record until, this will check if we can add
		// and if it succeeds will return ContinueExecution else will return restart after a timout.
		flow = s.bufferMetricCheck(ctx)
		if flow == Restart {
			continue
		}
		flow = s.bufferMetaCheck(ctx)
		if flow == Restart {
			continue
		}

		// Finally the main work loop where we pull new data.
		flow = s.mainWork(ctx)
		if flow == Exit {
			return
		}
	}

}

func (s *manager) checkConfig(ctx context.Context) flowcontrol {
	select {
	case <-ctx.Done():
		return Exit
	case cfg, ok := <-s.configInbox.ReceiveC():
		var successful bool
		if !ok {
			level.Debug(s.logger).Log("msg", "config inbox closed")
			return Exit
		}
		var err error
		if err = s.updateConfig(ctx, cfg.Value); err == nil {
			successful = true
		}
		cfg.Notify(successful, err)
		return ContinueExecution
	default:
		return ContinueExecution
	}
}

func (s *manager) bufferMetricCheck(ctx context.Context) flowcontrol {
	if s.bufferedMetric != nil {
		added := s.queue(ctx, s.bufferedMetric)
		if !added {
			time.Sleep(100 * time.Millisecond)
			return Restart
		} else {
			s.bufferedMetric = nil
		}
	}
	return ContinueExecution
}

func (s *manager) bufferMetaCheck(ctx context.Context) flowcontrol {
	if s.bufferedMetadata != nil {
		added := s.metadata.Add(ctx, s.bufferedMetadata)
		if !added {
			time.Sleep(100 * time.Millisecond)
			return Restart
		} else {
			s.bufferedMetadata = nil
		}
	}
	return ContinueExecution

}

func (s *manager) flushCheck(ctx context.Context) {
	// This isnt an exact science but it doesnt need to be, we just need to make sure that even if batch counts arent
	// being met then data is flowing.
	if time.Since(s.lastFlushTime) > s.cfg.FlushInterval {
		for _, l := range s.writeBuffers {
			l.Send(ctx)
		}
		s.metadata.Send(ctx)
		s.lastFlushTime = time.Now()
	}
}

func (s *manager) mainWork(ctx context.Context) flowcontrol {
	// main work queue.
	select {
	case <-ctx.Done():
		return Exit
	case ts, ok := <-s.inbox.ReceiveC():
		if !ok {
			level.Debug(s.logger).Log("msg", "series inbox closed")
			return Exit
		}
		added := s.queue(ctx, ts)
		if !added {
			s.bufferedMetric = ts
			time.Sleep(100 * time.Millisecond)
			return Restart
		}
		return ContinueExecution
	case ts, ok := <-s.metaInbox.ReceiveC():
		if !ok {
			level.Debug(s.logger).Log("msg", "meta inbox closed")
			return Exit
		}
		added := s.metadata.Add(ctx, ts)
		if !added {
			s.bufferedMetadata = ts
			time.Sleep(100 * time.Millisecond)
			return Restart
		}
		return ContinueExecution
	case cfg, ok := <-s.configInbox.ReceiveC():
		var successful bool
		if !ok {
			level.Debug(s.logger).Log("msg", "config inbox closed")
			return Exit
		}
		var err error
		if err = s.updateConfig(ctx, cfg.Value); err == nil {
			successful = true
		}
		cfg.Notify(successful, err)
		return ContinueExecution
		// This is necessary so we dont starve the queue, especially with buffered items.
	case <-time.After(100 * time.Millisecond):
		return ContinueExecution
	}
}

func (s *manager) updateConfig(ctx context.Context, cc types.ConnectionConfig) error {
	// No need to do anything if the configuration is the same.
	if s.cfg.Equals(cc) {
		return nil
	}
	s.cfg = cc
	level.Debug(s.logger).Log("msg", "recreating write buffers due to configuration change.")
	// Drain then stop the current writeBuffers.
	drainedMetrics := make([]types.MetricDatum, 0, len(s.writeBuffers)*cc.BatchCount)
	for _, l := range s.writeBuffers {
		drainedMetrics = append(drainedMetrics, l.Drain()...)
	}

	drainedMeta := s.metadata.Drain()

	s.writeBuffers = make([]*writeBuffer[types.MetricDatum], 0, s.cfg.Connections)
	for i := uint(0); i < s.cfg.Connections; i++ {
		l := newWriteBuffer[types.MetricDatum](cc, s.stats, false, s.logger)
		s.writeBuffers = append(s.writeBuffers, l)
	}
	// Force adding of metrics, note this may cause the system to go above the batch count.
	for _, d := range drainedMetrics {
		s.forceQueue(ctx, d)
	}

	metadata := newWriteBuffer[types.MetadataDatum](cc, s.stats, true, s.logger)
	for _, d := range drainedMeta {
		s.metadata.ForceAdd(ctx, d)
	}
	s.metadata = metadata
	return nil
}

func (s *manager) Stop() {
	s.configInbox.Stop()
}

// queue adds anything thats not metadata to the queue.
func (s *manager) queue(ctx context.Context, ts types.MetricDatum) bool {
	// Based on a hash which is the label hash add to the queue.
	queueNum := ts.Hash() % uint64(s.cfg.Connections)
	// This will block if the queue is full.
	return s.writeBuffers[queueNum].Add(ctx, ts)
}

// forceQueue forces data to be added ignoring queue limits, this should only be used in cases where we are draining then reapplying.
func (s *manager) forceQueue(ctx context.Context, ts types.MetricDatum) {
	// Based on a hash which is the label hash add to the queue.
	queueNum := ts.Hash() % uint64(s.cfg.Connections)
	s.writeBuffers[queueNum].ForceAdd(ctx, ts)
}
