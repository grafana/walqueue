package network

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/prometheus/common/config"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/walqueue/types"
	"github.com/panjf2000/ants/v2"
)

// manager manages writeBuffers. Mostly it exists to control their lifecycle and provide data to them via pull model.
type manager struct {
	metricBuffers                              []*writeBuffer[types.MetricDatum]
	metadataBuffer                             *writeBuffer[types.MetadataDatum]
	logger                                     log.Logger
	desiredOutbox                              *types.Mailbox[uint]
	configInbox                                *types.SyncMailbox[types.ConnectionConfig, bool]
	cfg                                        types.ConnectionConfig
	statshub                                   types.StatsHub
	lastFlushTime                              time.Time
	desiredParallelism                         *parallelism
	desiredConnections                         uint
	routinePool                                *ants.Pool
	writeBufferRequestMoreMetrics              chan types.RequestMoreSignals[types.MetricDatum]
	writeBufferRequestMoreMetadta              chan types.RequestMoreSignals[types.MetadataDatum]
	metricsHashedBuffer                        map[int][]types.MetricDatum
	metadata                                   []types.MetadataDatum
	requestSignalsFromFileQueue                chan types.RequestMoreSignals[types.Datum]
	responseFromRequestForSignalsFromFileQueue chan []types.Datum
	pending                                    []types.Datum
}

var _ types.NetworkClient = (*manager)(nil)

func New(cc types.ConnectionConfig, logger log.Logger, statshub types.StatsHub, requestSignalsFromFileQueue chan types.RequestMoreSignals[types.Datum]) (types.NetworkClient, error) {
	if requestSignalsFromFileQueue == nil || cap(requestSignalsFromFileQueue) != 1 {
		return nil, fmt.Errorf("requestSignalsFromFileQueue must be 1 or 0")
	}
	desiredOutbox := types.NewMailbox[uint]()
	goPool, err := ants.NewPool(int(cc.Parallelism.MaxConnections))
	if err != nil {
		return nil, err
	}
	p := newParallelism(cc.Parallelism, desiredOutbox, statshub, logger)
	s := &manager{
		metricBuffers:                 make([]*writeBuffer[types.MetricDatum], 0, cc.Parallelism.MinConnections),
		logger:                        logger,
		configInbox:                   types.NewSyncMailbox[types.ConnectionConfig, bool](),
		statshub:                      statshub,
		cfg:                           cc,
		lastFlushTime:                 time.Now(),
		desiredOutbox:                 desiredOutbox,
		desiredParallelism:            p,
		routinePool:                   goPool,
		metadata:                      make([]types.MetadataDatum, 0),
		metricsHashedBuffer:           make(map[int][]types.MetricDatum),
		writeBufferRequestMoreMetrics: make(chan types.RequestMoreSignals[types.MetricDatum]),
		writeBufferRequestMoreMetadta: make(chan types.RequestMoreSignals[types.MetadataDatum]),
		requestSignalsFromFileQueue:   requestSignalsFromFileQueue,
		responseFromRequestForSignalsFromFileQueue: make(chan []types.Datum),
		pending: make([]types.Datum, 0),
	}

	// Set the initial default as the middle point between min and max.
	s.desiredConnections = (s.cfg.Parallelism.MinConnections + s.cfg.Parallelism.MaxConnections) / 2

	httpClient, err := s.createClient(cc)
	if err != nil {
		return nil, err
	}
	// start kicks off a number of concurrent connections.
	for i := uint(0); i < s.desiredConnections; i++ {
		l := newWriteBuffer(int(i), cc, s.statshub.SendSeriesNetworkStats, false, logger, s.routinePool, httpClient, s.writeBufferRequestMoreMetrics)
		s.metricBuffers = append(s.metricBuffers, l)
	}

	metadata := newWriteBuffer[types.MetadataDatum](0, cc, s.statshub.SendMetadataNetworkStats, true, logger, s.routinePool, httpClient, s.writeBufferRequestMoreMetadta)

	s.metadataBuffer = metadata
	return s, nil
}

func (s *manager) Start(ctx context.Context) {
	s.desiredParallelism.Run(ctx)

	// Start each write buffer's pull routine
	for _, wb := range s.metricBuffers {
		wb.Run(ctx)
	}
	s.metadataBuffer.Run(ctx)
	s.Run(ctx)
}

func (s *manager) UpdateConfig(ctx context.Context, cc types.ConnectionConfig) (bool, error) {
	return s.configInbox.Send(ctx, cc)
}

func (s *manager) Run(ctx context.Context) {
	go s.run(ctx)
}

func (s *manager) run(ctx context.Context) {
	defer func() {
		s.desiredParallelism.Stop()
	}()

	// We only want one outstanding request for data.
	needMoreData := true
	for {
		select {
		case <-ctx.Done():
			return
		case items := <-s.responseFromRequestForSignalsFromFileQueue:
			s.pending = append(s.pending, items...)
			needMoreData = true
		case cfg, ok := <-s.configInbox.ReceiveC():
			if !ok {
				level.Debug(s.logger).Log("msg", "config inbox closed")
				return
			}
			var err error
			successful := false
			if err = s.updateConfig(ctx, cfg.Value, s.desiredConnections); err == nil {
				successful = true
			}
			cfg.Notify(successful, err)
		case desired, ok := <-s.desiredOutbox.ReceiveC():
			if !ok {
				level.Debug(s.logger).Log("msg", "desired outbox closed")
				return
			}
			err := s.updateConfig(ctx, s.cfg, desired)
			if err != nil {
				level.Debug(s.logger).Log("msg", "update config failure", "err", err)
			}
		case req := <-s.writeBufferRequestMoreMetrics:
			s.handleWriteBufferRequest(req)
		case req := <-s.writeBufferRequestMoreMetadta:
			s.handleWriteBufferRequestMetadata(req)
		}
		// If pending is empty then request more, ideally this gives us a batch count buffer of pending data.
		// That way the next file can already be in memory.
		if len(s.pending) == 0 && needMoreData {
			s.requestSignalsFromFileQueue <- types.RequestMoreSignals[types.Datum]{ID: 1, MaxCount: 0, Response: s.responseFromRequestForSignalsFromFileQueue}
			needMoreData = false
		}
	}
}

func (s *manager) handleWriteBufferRequest(req types.RequestMoreSignals[types.MetricDatum]) {
	queue, ok := s.metricsHashedBuffer[req.ID]
	// If we dont have a buffer build one
	if !ok {
		s.metricsHashedBuffer[req.ID] = make([]types.MetricDatum, 0, s.cfg.BatchCount)
		queue = s.metricsHashedBuffer[req.ID]
	}
	if len(queue) == 0 {
		tmp := s.pending[:0]
		// If our queue is empty then pull data from pending.
		for _, signal := range s.pending {
			switch m := signal.(type) {
			case types.MetricDatum:
				hash := int(m.Hash() % uint64(s.desiredConnections))
				if hash != req.ID {
					tmp = append(tmp, signal)
					continue
				}
				queue = append(queue, m)
				s.metricsHashedBuffer[hash] = queue
			default:
				tmp = append(tmp, signal)
			}
		}
		s.pending = tmp
	}
	toSend := queue[:min(len(queue), req.MaxCount)]
	s.metricsHashedBuffer[req.ID] = queue[len(toSend):]
	req.Response <- toSend
}

func (s *manager) handleWriteBufferRequestMetadata(req types.RequestMoreSignals[types.MetadataDatum]) {
	if len(s.metadata) == 0 {
		tmp := s.pending[:0]
		// If our queue is empty then pull data from pending.
		for _, signal := range s.pending {
			switch m := signal.(type) {
			case types.MetadataDatum:
				s.metadata = append(s.metadata, m)
			default:
				tmp = append(tmp, signal)
			}
		}
		s.pending = tmp
	}

	toSend := s.metadata[:min(len(s.metadata), req.MaxCount)]
	s.metadata = s.metadata[len(toSend):]
	req.Response <- toSend
}

func (s *manager) updateConfig(ctx context.Context, cc types.ConnectionConfig, desiredConnections uint) error {
	// No need to do anything if the configuration is the same or if we dont need to update connections.
	if s.cfg.Equals(cc) && s.desiredConnections == desiredConnections {
		return nil
	}

	s.desiredConnections = desiredConnections
	if cc.Parallelism.MaxConnections != s.cfg.Parallelism.MaxConnections {
		s.routinePool.Tune(int(cc.Parallelism.MaxConnections))
	}
	s.cfg = cc

	httpClient, err := s.createClient(cc)
	if err != nil {
		return err
	}

	level.Debug(s.logger).Log("msg", "recreating write buffers due to configuration change.")

	// Drain then stop the current writeBuffers.
	drainedMetrics := make([]types.MetricDatum, 0, len(s.metricBuffers)*cc.BatchCount)
	for _, l := range s.metricBuffers {
		drainedMetrics = append(drainedMetrics, l.Drain()...)
	}
	for _, dm := range drainedMetrics {
		s.pending = append(s.pending, dm)
	}

	drainedMeta := s.metadataBuffer.Drain()

	// Add drained metadata to our buffer
	s.metadata = append(s.metadata, drainedMeta...)

	// Start the metadata buffer
	s.metadataBuffer.Run(ctx)
	s.metricBuffers = make([]*writeBuffer[types.MetricDatum], 0, desiredConnections)
	for i := uint(0); i < desiredConnections; i++ {
		l := newWriteBuffer[types.MetricDatum](int(i), cc, s.statshub.SendSeriesNetworkStats, false, s.logger, s.routinePool, httpClient, s.writeBufferRequestMoreMetrics)

		// Start the buffer
		l.Run(ctx)
		s.metricBuffers = append(s.metricBuffers, l)
	}

	s.desiredParallelism.UpdateConfig(cc.Parallelism)
	return nil
}

func (s *manager) Stop() {
	s.routinePool.Release()
}

func (s *manager) createClient(cc types.ConnectionConfig) (*http.Client, error) {
	var httpOpts []config.HTTPClientOption
	if cc.UseRoundRobin {
		httpOpts = []config.HTTPClientOption{config.WithDialContextFunc(newDialContextWithRoundRobinDNS().dialContextFn())}
	}

	// Convert ConnectionConfig to PrometheusConfig
	cfg, err := cc.ToPrometheusConfig()
	if err != nil {
		return nil, err
	}

	client, err := config.NewClientFromConfig(cfg, "remote_write", httpOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create HTTP client: %w", err)
	}
	return client, nil
}
