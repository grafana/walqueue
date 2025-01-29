package network

import (
	"context"
	"fmt"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/walqueue/types"
	"github.com/vladopajic/go-actor/actor"
)

// manager manages loops. Mostly it exists to control their lifecycle and send work to them.
type manager struct {
	loops          []*writeBuffer[types.MetricDatum]
	metadata       *writeBuffer[types.MetadataDatum]
	logger         log.Logger
	inbox          actor.Mailbox[types.MetricDatum]
	metaInbox      actor.Mailbox[types.MetadataDatum]
	configInbox    *types.SyncMailbox[types.ConnectionConfig, bool]
	self           actor.Actor
	cfg            types.ConnectionConfig
	stats          func(types.NetworkStats)
	metaStats      func(types.NetworkStats)
	seriesBuffer   map[int][]types.MetricDatum
	metadataBuffer []types.MetadataDatum
}

var _ types.NetworkClient = (*manager)(nil)

var _ actor.Worker = (*manager)(nil)

func New(cc types.ConnectionConfig, logger log.Logger, seriesStats, metadataStats func(types.NetworkStats)) (types.NetworkClient, error) {
	s := &manager{
		loops:  make([]*writeBuffer[types.MetricDatum], 0, cc.Connections),
		logger: logger,
		// This provides blocking to only handle one at a time, so that if a queue blocks
		// it will stop the filequeue from feeding more. Without passing true the minimum is actually 64 instead of 1.
		inbox:       actor.NewMailbox[types.MetricDatum](actor.OptCapacity(1), actor.OptAsChan()),
		metaInbox:   actor.NewMailbox[types.MetadataDatum](actor.OptCapacity(1), actor.OptAsChan()),
		configInbox: types.NewSyncMailbox[types.ConnectionConfig, bool](),
		stats:       seriesStats,
		metaStats:   metadataStats,
		cfg:         cc,
	}

	// start kicks off a number of concurrent connections.
	for i := uint(0); i < s.cfg.Connections; i++ {
		l := newWriteBuffer[types.MetricDatum](cc, seriesStats, false, logger)
		s.loops = append(s.loops, l)
	}

	metadata := newWriteBuffer[types.MetadataDatum](cc, seriesStats, true, logger)
	s.metadata = metadata
	return s, nil
}

func (s *manager) Start() {
	s.startLoops()
	s.configInbox.Start()
	s.metaInbox.Start()
	s.inbox.Start()
	s.self = actor.New(s)
	s.self.Start()
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

func (s *manager) DoWork(ctx actor.Context) actor.WorkerStatus {
	// This acts as a priority queue, always check for configuration changes first.
	select {
	case cfg, ok := <-s.configInbox.ReceiveC():
		var successful bool
		if !ok {
			level.Debug(s.logger).Log("msg", "config inbox closed")
			return actor.WorkerEnd
		}
		var err error
		defer func() {
			cfg.Notify(successful, err)
		}()
		if err = s.updateConfig(cfg.Value); err == nil {
			successful = true
		}
		return actor.WorkerContinue
	default:
	}

	// main work queue.
	select {
	case <-ctx.Done():
		return actor.WorkerEnd
	case ts, ok := <-s.inbox.ReceiveC():
		if !ok {
			level.Debug(s.logger).Log("msg", "series inbox closed")
			return actor.WorkerEnd
		}
		s.queue(ctx, ts)
		return actor.WorkerContinue
	case ts, ok := <-s.metaInbox.ReceiveC():
		if !ok {
			level.Debug(s.logger).Log("msg", "meta inbox closed")
			return actor.WorkerEnd
		}
		err := s.metadata.seriesMbx.Send(ctx, ts)
		if err != nil {
			level.Error(s.logger).Log("msg", "failed to send to metadata loop", "err", err)
		}
		return actor.WorkerContinue
	case cfg, ok := <-s.configInbox.ReceiveC():
		var successful bool
		if !ok {
			level.Debug(s.logger).Log("msg", "config inbox closed")
			return actor.WorkerEnd
		}
		var err error
		defer func() {
			cfg.Notify(successful, err)
		}()
		if err = s.updateConfig(cfg.Value); err == nil {
			successful = true
		}
		return actor.WorkerContinue
	}
}

func (s *manager) updateConfig(cc types.ConnectionConfig) error {
	// No need to do anything if the configuration is the same.
	if s.cfg.Equals(cc) {
		return nil
	}
	s.cfg = cc
	// TODO @mattdurham make this smarter, at the moment any samples in the loops are lost.
	// Ideally we would drain the queues and re add them but that is a future need.
	// In practice this shouldn't change often so data loss should be minimal.
	// For the moment we will stop all the items and recreate them.
	level.Debug(s.logger).Log("msg", "dropping all series in loops and creating queue due to config change")
	s.stopLoops()
	s.loops = make([]*write[types.MetricDatum], 0, s.cfg.Connections)
	for i := uint(0); i < s.cfg.Connections; i++ {
		l, err := newLoop[types.MetricDatum](cc, false, s.logger, s.stats)
		if err != nil {
			level.Error(s.logger).Log("msg", "failed to create series loop during config update", "err", err)
			return err
		}
		l.self = actor.New(l)
		s.loops = append(s.loops, l)
	}

	metadata, err := newLoop[types.MetadataDatum](cc, true, s.logger, s.metaStats)
	if err != nil {
		level.Error(s.logger).Log("msg", "failed to create metadata loop during config update", "err", err)
		return err
	}
	s.metadata = metadata
	s.metadata.self = actor.New(s.metadata)
	level.Debug(s.logger).Log("msg", "starting loops")
	s.startLoops()
	level.Debug(s.logger).Log("msg", "loops started")
	return nil
}

func (s *manager) Stop() {
	s.stopLoops()
	s.configInbox.Stop()
	s.metaInbox.Stop()
	s.inbox.Stop()
	s.self.Stop()
}

func (s *manager) stopLoops() {
	for _, l := range s.loops {
		l.Stop()
	}
	s.metadata.Stop()
}

func (s *manager) startLoops() {
	for _, l := range s.loops {
		l.Start()
	}
	s.metadata.Start()
}

// Queue adds anything thats not metadata to the queue.
func (s *manager) queue(ctx context.Context, ts types.MetricDatum) {
	// Based on a hash which is the label hash add to the queue.
	queueNum := ts.Hash() % uint64(s.cfg.Connections)
	// This will block if the queue is full.
	err := s.loops[queueNum].seriesMbx.Send(ctx, ts)
	if err != nil {
		level.Error(s.logger).Log("msg", "failed to send to loop", "err", err)
	}
}
