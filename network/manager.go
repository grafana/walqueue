package network

import (
	"context"
	"fmt"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/walqueue/types"
	"golang.design/x/chann"
)

// manager manages loops. Mostly it exists to control their lifecycle and send work to them.
type manager struct {
	loops       []*loop[types.MetricDatum]
	metadata    *loop[types.MetadataDatum]
	logger      log.Logger
	inbox       *types.Mailbox[types.MetricDatum]
	metaInbox   *types.Mailbox[types.MetadataDatum]
	configInbox *types.SyncMailbox[types.ConnectionConfig, bool]
	cfg         types.ConnectionConfig
	stats       func(types.NetworkStats)
	metaStats   func(types.NetworkStats)
}

var _ types.NetworkClient = (*manager)(nil)

func New(cc types.ConnectionConfig, logger log.Logger, seriesStats, metadataStats func(types.NetworkStats)) (types.NetworkClient, error) {
	s := &manager{
		loops:  make([]*loop[types.MetricDatum], 0, cc.Connections),
		logger: logger,
		// This provides blocking to only handle one at a time, so that if a queue blocks
		// it will stop the filequeue from feeding more.
		inbox:       types.NewMailbox[types.MetricDatum](chann.Cap(1)),
		metaInbox:   types.NewMailbox[types.MetadataDatum](chann.Cap(1)),
		configInbox: types.NewSyncMailbox[types.ConnectionConfig, bool](chann.Cap(1)),
		stats:       seriesStats,
		metaStats:   metadataStats,
		cfg:         cc,
	}

	// start kicks off a number of concurrent connections.
	for i := uint(0); i < s.cfg.Connections; i++ {
		l, err := newLoop[types.MetricDatum](cc, false, logger, seriesStats)
		if err != nil {
			level.Error(logger).Log("msg", "failed to create series loop", "err", err)
			return nil, fmt.Errorf("failed to create series loop: %w", err)
		}
		s.loops = append(s.loops, l)
	}

	metadata, err := newLoop[types.MetadataDatum](cc, true, logger, metadataStats)
	if err != nil {
		level.Error(logger).Log("msg", "failed to create metadata loop", "err", err)
		return nil, fmt.Errorf("failed to create metadata loop: %w", err)
	}
	s.metadata = metadata
	return s, nil
}

func (s *manager) Start(ctx context.Context) {
	s.startLoops(ctx)
	go s.run(ctx)
}

func (s *manager) SendSeries(ctx context.Context, data types.MetricDatum) error {
	return s.inbox.Send(ctx, data)
}

func (s *manager) SendMetadata(ctx context.Context, data types.MetadataDatum) error {
	return s.metaInbox.Send(ctx, data)
}

func (s *manager) UpdateConfig(ctx context.Context, cc types.ConnectionConfig) (bool, error) {
	return s.configInbox.In(ctx, cc)
}

func (s *manager) run(ctx context.Context) {
	for {
		// checkConfig returns a value if we should continue the loop or can we exit early.
		cont := s.checkConfig(ctx)
		if !cont {
			return
		}
		cont = s.mainWork(ctx)
		if !cont {
			return
		}
	}
}

func (s *manager) checkConfig(ctx context.Context) bool {
	// This acts as a priority queue, always check for configuration changes first.
	select {
	case cfg, ok := <-s.configInbox.Out():
		var successful bool
		if !ok {
			level.Debug(s.logger).Log("msg", "config inbox closed")
			return false
		}
		var err error
		if err = s.updateConfig(ctx, cfg.Value); err == nil {
			successful = true
		}
		cfg.Notify(successful, err)
		return true
	default:
	}
	return true
}

func (s *manager) mainWork(ctx context.Context) bool {
	// main work queue.
	select {
	case <-ctx.Done():
		return false
	case ts, ok := <-s.inbox.Receive():
		if !ok {
			level.Debug(s.logger).Log("msg", "series inbox closed")
			return false
		}
		s.queue(ctx, ts)
		return true
	case ts, ok := <-s.metaInbox.Receive():
		if !ok {
			level.Debug(s.logger).Log("msg", "meta inbox closed")
			return false
		}
		err := s.metadata.seriesMbx.Send(ctx, ts)
		if err != nil {
			level.Error(s.logger).Log("msg", "failed to send metadata", "err", err)
		}
	case cfg, ok := <-s.configInbox.Out():
		var successful bool
		if !ok {
			level.Debug(s.logger).Log("msg", "config inbox closed")
			return false
		}
		var err error
		if err = s.updateConfig(ctx, cfg.Value); err == nil {
			successful = true
		}
		cfg.Notify(successful, err)
	}
	return true
}

func (s *manager) updateConfig(ctx context.Context, cc types.ConnectionConfig) error {
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
	s.drainLoops()
	s.loops = make([]*loop[types.MetricDatum], 0, s.cfg.Connections)
	for i := uint(0); i < s.cfg.Connections; i++ {
		l, err := newLoop[types.MetricDatum](cc, false, s.logger, s.stats)
		if err != nil {
			level.Error(s.logger).Log("msg", "failed to create series loop during config update", "err", err)
			return err
		}
		s.loops = append(s.loops, l)
	}

	metadata, err := newLoop[types.MetadataDatum](cc, true, s.logger, s.metaStats)
	if err != nil {
		level.Error(s.logger).Log("msg", "failed to create metadata loop during config update", "err", err)
		return err
	}
	s.metadata = metadata
	level.Debug(s.logger).Log("msg", "starting loops")
	s.startLoops(ctx)
	level.Debug(s.logger).Log("msg", "loops started")
	return nil
}

// Stop is hard stop on the manager and all the loops.
func (s *manager) Stop() {
	s.stopLoops()
}

func (s *manager) stopLoops() {
	for _, l := range s.loops {
		l.Stop()
	}
	s.metadata.Stop()
}

func (s *manager) drainLoops() {
	for _, l := range s.loops {
		l.DrainStop()
	}
	s.metadata.DrainStop()
}

func (s *manager) startLoops(ctx context.Context) {
	for _, l := range s.loops {
		l.Start(ctx)
	}
	s.metadata.Start(ctx)
}

// Queue adds anything thats not metadata to the queue.
func (s *manager) queue(ctx context.Context, ts types.MetricDatum) {
	// Based on a hash which is the label hash add to the queue.
	queueNum := ts.Hash() % uint64(s.cfg.Connections)
	// This will block if the queue is full.
	err := s.loops[queueNum].seriesMbx.Send(ctx, ts)
	if err != nil {
		level.Error(s.logger).Log("msg", "failed to send series", "err", err)
	}
}
