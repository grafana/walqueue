package network

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/common/config"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/walqueue/types"
	"github.com/panjf2000/ants/v2"
	"golang.design/x/chann"
)

// manager manages writeBuffers. Mostly it exists to control their lifecycle and provide data to them via pull model.
type manager struct {
	writeBuffers       []*writeBuffer[types.MetricDatum]
	metadata           *writeBuffer[types.MetadataDatum]
	logger             log.Logger
	inbox              *types.Mailbox[types.MetricDatum]
	metaInbox          *types.Mailbox[types.MetadataDatum]
	desiredOutbox      *types.Mailbox[uint]
	configInbox        *types.SyncMailbox[types.ConnectionConfig, bool]
	cfg                types.ConnectionConfig
	statshub           types.StatsHub
	lastFlushTime      time.Time
	desiredParallelism *parallelism
	desiredConnections uint
	routinePool        *ants.Pool
	metricsBufMtx      sync.Mutex
	metadataBufMtx     sync.Mutex
	metricsBuffer      []types.MetricDatum
	metadataBuffer     []types.MetadataDatum
}

var _ types.NetworkClient = (*manager)(nil)

func New(cc types.ConnectionConfig, logger log.Logger, statshub types.StatsHub) (types.NetworkClient, error) {
	desiredOutbox := types.NewMailbox[uint]()
	goPool, err := ants.NewPool(int(cc.Parallelism.MaxConnections))
	if err != nil {
		return nil, err
	}
	p := newParallelism(cc.Parallelism, desiredOutbox, statshub, logger)
	s := &manager{
		writeBuffers:       make([]*writeBuffer[types.MetricDatum], 0, cc.Parallelism.MinConnections),
		logger:             logger,
		inbox:              types.NewMailbox[types.MetricDatum](chann.Cap(1)),
		metaInbox:          types.NewMailbox[types.MetadataDatum](chann.Cap(1)),
		configInbox:        types.NewSyncMailbox[types.ConnectionConfig, bool](),
		statshub:           statshub,
		cfg:                cc,
		lastFlushTime:      time.Now(),
		desiredOutbox:      desiredOutbox,
		desiredParallelism: p,
		routinePool:        goPool,
		metricsBuffer:      make([]types.MetricDatum, 0),
		metadataBuffer:     make([]types.MetadataDatum, 0),
	}

	// Set the initial default as the middle point between min and max.
	s.desiredConnections = (s.cfg.Parallelism.MinConnections + s.cfg.Parallelism.MaxConnections) / 2

	httpClient, err := s.createClient(cc)
	if err != nil {
		return nil, err
	}
	// start kicks off a number of concurrent connections.
	for i := uint(0); i < s.desiredConnections; i++ {
		l := newWriteBuffer[types.MetricDatum](cc, s.statshub.SendSeriesNetworkStats, false, logger, s.routinePool, httpClient)

		// Register the pull function for this buffer to pull metrics by hash
		bufferIndex := i
		l.RegisterPullFunc(func(ctx context.Context) (types.MetricDatum, bool) {
			return s.pullMetric(ctx, bufferIndex)
		})

		s.writeBuffers = append(s.writeBuffers, l)
	}

	metadata := newWriteBuffer[types.MetadataDatum](cc, s.statshub.SendMetadataNetworkStats, true, logger, s.routinePool, httpClient)
	metadata.RegisterPullFunc(func(ctx context.Context) (types.MetadataDatum, bool) {
		return s.pullMetadata(ctx)
	})

	s.metadata = metadata
	return s, nil
}

func (s *manager) Start(ctx context.Context) {
	s.configInbox.Start()
	s.desiredParallelism.Run(ctx)

	// Start each write buffer's pull routine
	for _, wb := range s.writeBuffers {
		wb.Run(ctx)
	}
	s.metadata.Run(ctx)

	s.Run(ctx)
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

// pullMetric is called by a write buffer to pull a metric for its hash
func (s *manager) pullMetric(ctx context.Context, bufferIndex uint) (types.MetricDatum, bool) {
	s.metricsBufMtx.Lock()
	defer s.metricsBufMtx.Unlock()

	// Check if there's anything in the buffer for this hash
	for i, metric := range s.metricsBuffer {
		if metric.Hash()%uint64(s.desiredConnections) == uint64(bufferIndex) {
			// Remove and return this item
			result := metric
			s.metricsBuffer = append(s.metricsBuffer[:i], s.metricsBuffer[i+1:]...)
			return result, true
		}
	}

	// Try to get a new item from the inbox (non-blocking)
	select {
	case <-ctx.Done():
		return nil, false
	case ts, ok := <-s.inbox.ReceiveC():
		if !ok {
			return nil, false
		}

		// If this is for our hash, return it
		if ts.Hash()%uint64(s.desiredConnections) == uint64(bufferIndex) {
			return ts, true
		}

		// Otherwise add it to the buffer for another buffer to pick up
		s.metricsBuffer = append(s.metricsBuffer, ts)
		return nil, false
	default:
		// No new items
		return nil, false
	}
}

// pullMetadata is called by the metadata write buffer to pull metadata
func (s *manager) pullMetadata(ctx context.Context) (types.MetadataDatum, bool) {
	s.metadataBufMtx.Lock()
	defer s.metadataBufMtx.Unlock()

	// First check if there's anything in the buffer
	if len(s.metadataBuffer) > 0 {
		result := s.metadataBuffer[0]
		s.metadataBuffer = s.metadataBuffer[1:]
		return result, true
	}

	// Try to get a new item from the inbox (non-blocking)
	select {
	case <-ctx.Done():
		return nil, false
	case ts, ok := <-s.metaInbox.ReceiveC():
		if !ok {
			return nil, false
		}
		return ts, true
	default:
		// No new items
		return nil, false
	}
}

type flowcontrol int

const (
	Restart flowcontrol = iota
	Exit
	ContinueExecution
)

func (s *manager) Run(ctx context.Context) {
	go s.run(ctx)
}

func (s *manager) run(ctx context.Context) {
	defer func() {
		s.desiredParallelism.Stop()
	}()

	// This run loop is now much simpler since write buffers pull data themselves
	for {
		select {
		case <-ctx.Done():
			return
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
		}
	}
}

func (s *manager) updateConfig(ctx context.Context, cc types.ConnectionConfig, desiredConnections uint) error {
	// No need to do anything if the configuration is the same or if we dont need to update connections.
	if s.cfg.Equals(cc) && s.desiredConnections == desiredConnections {
		return nil
	}

	// Lock buffers during the reconfiguration to prevent concurrent access
	s.metricsBufMtx.Lock()
	s.metadataBufMtx.Lock()
	defer s.metricsBufMtx.Unlock()
	defer s.metadataBufMtx.Unlock()

	s.desiredConnections = desiredConnections
	if cc.Parallelism.MaxConnections != s.cfg.Parallelism.MaxConnections {
		s.routinePool.Tune(int(cc.Parallelism.MaxConnections))
	}
	s.cfg = cc
	level.Debug(s.logger).Log("msg", "recreating write buffers due to configuration change.")

	// Drain then stop the current writeBuffers.
	drainedMetrics := make([]types.MetricDatum, 0, len(s.writeBuffers)*cc.BatchCount)
	for _, l := range s.writeBuffers {
		drainedMetrics = append(drainedMetrics, l.Drain()...)
	}

	drainedMeta := s.metadata.Drain()

	httpClient, err := s.createClient(cc)
	if err != nil {
		return err
	}
	s.writeBuffers = make([]*writeBuffer[types.MetricDatum], 0, desiredConnections)
	for i := uint(0); i < desiredConnections; i++ {
		l := newWriteBuffer[types.MetricDatum](cc, s.statshub.SendSeriesNetworkStats, false, s.logger, s.routinePool, httpClient)

		// Register the pull function for the new buffer
		bufferIndex := i
		l.RegisterPullFunc(func(ctx context.Context) (types.MetricDatum, bool) {
			return s.pullMetric(ctx, bufferIndex)
		})

		// Start the buffer
		l.Run(ctx)
		s.writeBuffers = append(s.writeBuffers, l)
	}

	// Add drained metrics to our buffer
	s.metricsBuffer = append(s.metricsBuffer, drainedMetrics...)

	metadata := newWriteBuffer[types.MetadataDatum](cc, s.statshub.SendMetadataNetworkStats, true, s.logger, s.routinePool, httpClient)
	metadata.RegisterPullFunc(func(ctx context.Context) (types.MetadataDatum, bool) {
		return s.pullMetadata(ctx)
	})

	// Add drained metadata to our buffer
	s.metadataBuffer = append(s.metadataBuffer, drainedMeta...)

	// Start the metadata buffer
	metadata.Run(ctx)
	s.metadata = metadata

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
