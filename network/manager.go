package network

import (
	"context"
	"fmt"
	"net/http"
	"reflect"
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
	requestSignalsFromFileQueue                chan types.RequestMoreSignals[types.Datum]
	responseFromRequestForSignalsFromFileQueue chan []types.Datum
	pending                                    []types.Datum
	client                                     *http.Client
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
	s.client = httpClient
	// start kicks off a number of concurrent connections.
	for i := uint(0); i < s.desiredConnections; i++ {
		l := newWriteBuffer(int(i), cc, s.statshub.SendSeriesNetworkStats, false, logger, s.routinePool, s.client, s.writeBufferRequestMoreMetrics)
		s.metricBuffers = append(s.metricBuffers, l)
	}

	s.metadataBuffer = newWriteBuffer[types.MetadataDatum](0, cc, s.statshub.SendMetadataNetworkStats, true, logger, s.routinePool, s.client, s.writeBufferRequestMoreMetadta)
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
	parkedRequests := make([]types.RequestMoreSignals[types.MetricDatum], 0)
	var parkedMetadata *types.RequestMoreSignals[types.MetadataDatum]
	for {
		select {
		case <-ctx.Done():
			return
		case items := <-s.responseFromRequestForSignalsFromFileQueue:
			s.pending = append(s.pending, items...)
			// Check out parked requests for more data
			parkedRequests = s.pullItemsFromQueueForMetrics(parkedRequests)

			if parkedMetadata != nil {
				toSend := s.handleWriteBufferRequestMetadata(*parkedMetadata)
				// This means we didnt find anything, but we want to park the request until we get more.
				if len(toSend) != 0 {
					parkedMetadata.Response <- toSend
					parkedMetadata = nil
				}

			}
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
			parked := s.pullItemsFromQueueForMetrics([]types.RequestMoreSignals[types.MetricDatum]{req})
			parkedRequests = append(parkedRequests, parked...)

		case req := <-s.writeBufferRequestMoreMetadta:
			toSend := s.handleWriteBufferRequestMetadata(req)
			// This means we didnt find anything, but we want to park the request until we get more.
			if len(toSend) == 0 {
				parkedMetadata = &req
			} else {
				req.Response <- toSend
			}
		}
		// If pending is empty then request more, ideally this gives us a batch count buffer of pending data.
		// That way the next file can already be in memory.
		if len(s.pending) == 0 && needMoreData {
			s.requestSignalsFromFileQueue <- types.RequestMoreSignals[types.Datum]{ID: 1, MaxCount: 0, Response: s.responseFromRequestForSignalsFromFileQueue}
			needMoreData = false
		}
	}
}

// pullItemsFromQueueForMetrics will pull items from queue for metrics
func (s *manager) pullItemsFromQueueForMetrics(reqs []types.RequestMoreSignals[types.MetricDatum]) []types.RequestMoreSignals[types.MetricDatum] {
	if len(s.pending) == 0 {
		return reqs
	}
	reqMap := make(map[int]types.RequestMoreSignals[types.MetricDatum])
	reqIndex := make(map[int]int)
	for _, req := range reqs {
		if req.Buffer != nil {
			req.Buffer = req.Buffer[:req.MaxCount]
		} else {
			req.Buffer = make([]types.MetricDatum, req.MaxCount)
		}
		reqMap[req.ID] = req
		reqIndex[req.ID] = 0
	}

	// Single allocation with in-place filtering
	pendingIndex := 0
	for _, signal := range s.pending {
		if m, ok := signal.(types.MetricDatum); ok {
			hash := int(m.Hash() % uint64(s.desiredConnections))
			req, found := reqMap[hash]
			if !found {
				// Keep this item
				s.pending[pendingIndex] = signal
				pendingIndex++
				continue
			}
			// We only want to feel up to our max count so if we hit that level then add back to pending.
			if reqIndex[hash] == req.MaxCount {
				s.pending[pendingIndex] = signal
				pendingIndex++
			} else {
				cnt := reqIndex[hash]
				req.Buffer[cnt] = m
				reqIndex[hash]++
			}
		} else {
			// Keep this item
			s.pending[pendingIndex] = signal
			pendingIndex++
		}

	}

	// Truncate the slice to the new size
	s.pending = s.pending[:pendingIndex]
	parked := make([]types.RequestMoreSignals[types.MetricDatum], 0)
	for id, count := range reqIndex {
		if count == 0 {
			parked = append(parked, reqMap[id])
		} else {
			buf := reqMap[id].Buffer[:count]
			for _, m := range buf {
				if m == nil {
					println("nil buffer")
				}
			}
			reqMap[id].Response <- buf
		}

	}
	return parked
}

func (s *manager) handleWriteBufferRequestMetadata(req types.RequestMoreSignals[types.MetadataDatum]) []types.MetadataDatum {
	var toSend []types.MetadataDatum
	if req.Buffer != nil {
		toSend = req.Buffer
	} else {
		toSend = make([]types.MetadataDatum, req.MaxCount)
	}

	if len(s.pending) == 0 {
		return toSend[:0]
	}

	// Single allocation with in-place filtering
	n := 0
	trimIndex := 0
	for i, signal := range s.pending {
		if req.MaxCount == trimIndex {
			// Copy the rest without filtering
			copy(s.pending[n:], s.pending[i:])
			n += len(s.pending) - i
			break
		}

		if m, ok := signal.(types.MetadataDatum); ok {
			toSend[trimIndex] = m
			trimIndex++
			continue
		}
		// Keep this item
		s.pending[n] = signal
		n++
	}

	// Truncate the slice to the new size
	s.pending = s.pending[:n]
	return toSend[:trimIndex]
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
	if !reflect.DeepEqual(cc, s.cfg) {
		httpClient, err := s.createClient(cc)
		if err != nil {
			return err
		}
		s.client = httpClient
	}
	s.cfg = cc

	level.Debug(s.logger).Log("msg", "recreating write buffers due to configuration change.")

	// Drain then stop the current writeBuffers.
	drainedMetrics := make([]types.MetricDatum, 0, len(s.metricBuffers)*cc.BatchCount)
	for _, l := range s.metricBuffers {
		drainedMetrics = append(drainedMetrics, l.Drain()...)
		l.Stop()
	}
	for _, dm := range drainedMetrics {
		s.pending = append(s.pending, dm)
	}

	drainedMeta := s.metadataBuffer.Drain()
	for _, dm := range drainedMeta {
		s.pending = append(s.pending, dm)
	}
	s.metadataBuffer.Stop()

	s.metadataBuffer = newWriteBuffer[types.MetadataDatum](0, cc, s.statshub.SendMetadataNetworkStats, true, s.logger, s.routinePool, s.client, s.writeBufferRequestMoreMetadta)
	// Start the metadata buffer
	s.metadataBuffer.Run(ctx)
	s.metricBuffers = make([]*writeBuffer[types.MetricDatum], 0, desiredConnections)
	for i := uint(0); i < desiredConnections; i++ {
		l := newWriteBuffer[types.MetricDatum](int(i), cc, s.statshub.SendSeriesNetworkStats, false, s.logger, s.routinePool, s.client, s.writeBufferRequestMoreMetrics)

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
