package network

import (
	"context"
	"fmt"
	"net/http"
	"reflect"
	"time"

	"go.uber.org/atomic"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/walqueue/types"
	"github.com/prometheus/common/config"
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
	requestSignalsFromFileQueue                chan types.RequestMoreSignals[types.Datum]
	responseFromRequestForSignalsFromFileQueue chan []types.Datum
	pendingData                                *pending
	client                                     *http.Client
	currentOutgoingConnections                 *atomic.Int32
	ctx                                        context.Context
	stop                                       chan struct{}
	requestForMoreDataPending                  *atomic.Bool
	queuePendingData                           chan struct{}
}

var _ types.NetworkClient = (*manager)(nil)

func New(cc types.ConnectionConfig, logger log.Logger, statshub types.StatsHub, requestSignalsFromFileQueue chan types.RequestMoreSignals[types.Datum]) (types.NetworkClient, error) {
	if requestSignalsFromFileQueue == nil || cap(requestSignalsFromFileQueue) != 1 {
		return nil, fmt.Errorf("requestSignalsFromFileQueue must be 1")
	}
	desiredOutbox := types.NewMailbox[uint]()
	p := newParallelism(cc.Parallelism, desiredOutbox, statshub, logger)
	s := &manager{
		metricBuffers:               make([]*writeBuffer[types.MetricDatum], 0, cc.Parallelism.MinConnections),
		logger:                      logger,
		configInbox:                 types.NewSyncMailbox[types.ConnectionConfig, bool](),
		statshub:                    statshub,
		cfg:                         cc,
		lastFlushTime:               time.Now(),
		desiredOutbox:               desiredOutbox,
		desiredParallelism:          p,
		requestSignalsFromFileQueue: requestSignalsFromFileQueue,
		responseFromRequestForSignalsFromFileQueue: make(chan []types.Datum),
		stop:                       make(chan struct{}),
		requestForMoreDataPending:  &atomic.Bool{},
		queuePendingData:           make(chan struct{}, 1),
		currentOutgoingConnections: atomic.NewInt32(0),
	}

	// Set the initial default as the middle point between min and max.
	s.desiredConnections = (s.cfg.Parallelism.MinConnections + s.cfg.Parallelism.MaxConnections) / 2
	s.pendingData = NewPending(int(s.desiredConnections), cc.BatchCount)

	httpClient, err := s.createClient(cc)
	if err != nil {
		return nil, err
	}
	s.client = httpClient
	// start kicks off a number of concurrent connections.
	for i := uint(0); i < s.desiredConnections; i++ {
		l := newWriteBuffer[types.MetricDatum](int(i), cc, s.statshub.SendSeriesNetworkStats, logger)
		s.metricBuffers = append(s.metricBuffers, l)
	}

	s.metadataBuffer = newWriteBuffer[types.MetadataDatum](0, cc, s.statshub.SendMetadataNetworkStats, logger)
	return s, nil
}

func (s *manager) Start(ctx context.Context) {
	s.ctx = ctx
	s.desiredParallelism.Run(s.ctx)
	s.Run()
}

func (s *manager) Stop() {
	s.stop <- struct{}{}
}

func (s *manager) UpdateConfig(ctx context.Context, cc types.ConnectionConfig) (bool, error) {
	return s.configInbox.Send(ctx, cc)
}

func (s *manager) Run() {
	go s.run()
}

func (s *manager) run() {
	defer func() {
		s.desiredParallelism.Stop()
	}()
	// Initially queue some request for data.
	s.requestSignalsFromFileQueue <- types.RequestMoreSignals[types.Datum]{
		Response: s.responseFromRequestForSignalsFromFileQueue,
	}
	s.requestForMoreDataPending.Store(true)

	// How often to check for the flush interval.
	// Functionally means the flush interval has no effect below 1s.
	// in reasonable volume environments this isnt a problem since we will
	// be filling the buffers and sending.
	ticker := time.NewTicker(1 * time.Second)
	for {
		select {
		case <-s.ctx.Done():
			return
		case items := <-s.responseFromRequestForSignalsFromFileQueue:
			s.requestForMoreDataPending.Store(false)
			s.addNewDatumsAndDistribute(items)
			s.checkAndSend()
		case cfg, ok := <-s.configInbox.ReceiveC():
			if !ok {
				level.Debug(s.logger).Log("msg", "config inbox closed")
				return
			}
			var err error
			successful := false
			if err = s.updateConfig(cfg.Value, s.desiredConnections); err == nil {
				successful = true
			}
			cfg.Notify(successful, err)
			s.queueCheck()
		case desired, ok := <-s.desiredOutbox.ReceiveC():
			if !ok {
				level.Debug(s.logger).Log("msg", "desired outbox closed")
				return
			}
			err := s.updateConfig(s.cfg, desired)
			if err != nil {
				level.Debug(s.logger).Log("msg", "update config failure", "err", err)
			}
			s.queueCheck()
		case <-s.stop:
			return
		case <-ticker.C:
			s.queueCheck()
		case <-s.queuePendingData:
			s.addNewDatumsAndDistribute([]types.Datum{})
			s.checkAndSend()
		}
	}
}

// queueCheck will queue a check for redistributing data if one is on already out.
// This should be called whenever a request completes.
func (s *manager) queueCheck() {
	select {
	case s.queuePendingData <- struct{}{}:
	default:
	}
}

// addNewDatumsAndDistribute will distribute the pending items to pending data and then the writeBuffers.
func (s *manager) addNewDatumsAndDistribute(items []types.Datum) {
	s.pendingData.AddItems(items)

	for _, wr := range s.metricBuffers {
		// If we are sending or there is no capacity then dont add.
		if wr.IsSending() {
			continue
		} else if wr.RemainingCapacity() == 0 {
			continue
		} else {
			t := s.pendingData.PullMetricItems(wr.id, wr.RemainingCapacity())
			wr.Add(t)
		}
	}

	if !s.metadataBuffer.IsSending() && s.metadataBuffer.RemainingCapacity() > 0 {
		s.metadataBuffer.Add(s.pendingData.PullMetadataItems(s.metadataBuffer.RemainingCapacity()))
	}

	// Have we queued enough to drop below having a full batch? If so request more, the plus one represents metadata.
	if s.pendingData.TotalLen() <= (s.cfg.BatchCount*int(s.desiredConnections+1)) && !s.requestForMoreDataPending.Load() {
		s.requestSignalsFromFileQueue <- types.RequestMoreSignals[types.Datum]{
			Response: s.responseFromRequestForSignalsFromFileQueue,
		}
		s.requestForMoreDataPending.Store(true)
	}
}

func (s *manager) finishWrite() {
	s.currentOutgoingConnections.Dec()
	s.queueCheck()
}

// checkAndSend will check each write buffer to see if it can send data.
func (s *manager) checkAndSend() {
	sendToWR := func(wr *writeBuffer[types.MetricDatum]) {
		if s.currentOutgoingConnections.Load() >= int32(s.desiredConnections) {
			return
		}
		s.currentOutgoingConnections.Inc()
		wr.Send(s.ctx, s.client, s.finishWrite)
	}

	for _, wr := range s.metricBuffers {
		if wr.IsSending() {
			continue
		} else if wr.RemainingCapacity() == 0 { // If remaining capacity is zero then the buffer is full so send
			sendToWR(wr)
		} else if time.Since(wr.LastAttemptedSend()) > s.cfg.FlushInterval && wr.Len() > 0 { // if we hit the flush interval send.
			sendToWR(wr)
		}
	}

	sendMeta := func(wr *writeBuffer[types.MetadataDatum]) {
		if s.currentOutgoingConnections.Load() >= int32(s.desiredConnections) {
			return
		}
		s.currentOutgoingConnections.Inc()
		wr.Send(s.ctx, s.client, s.finishWrite)
	}
	// Check to see if we need to send metadata
	if !s.metadataBuffer.IsSending() {
		if time.Since(s.metadataBuffer.LastAttemptedSend()) > s.cfg.FlushInterval && s.metadataBuffer.Len() > 0 {
			sendMeta(s.metadataBuffer)
		} else if s.metadataBuffer.RemainingCapacity() == 0 {
			sendMeta(s.metadataBuffer)
		}
	}
}

func (s *manager) updateConfig(cc types.ConnectionConfig, desiredConnections uint) error {
	// No need to do anything if the configuration is the same or if we dont need to update connections.
	if s.cfg.Equals(cc) && s.desiredConnections == desiredConnections {
		return nil
	}

	s.desiredConnections = desiredConnections
	// To prevent goroutine churn from new connections we only create a new client if the configuration has changed.
	if !reflect.DeepEqual(cc, s.cfg) {
		httpClient, err := s.createClient(cc)
		if err != nil {
			return err
		}
		s.client = httpClient
	}
	s.cfg = cc

	level.Debug(s.logger).Log("msg", "recreating write buffers due to configuration change.")

	// Reshard the pending data with the new desired connections.
	s.pendingData.Reshard(int(s.desiredConnections), s.cfg.BatchCount)

	// Drain then stop the current writeBuffers.
	drainedMetrics := make([]types.Datum, 0, len(s.metricBuffers)*cc.BatchCount)
	for _, l := range s.metricBuffers {
		for _, dm := range l.Drain() {
			drainedMetrics = append(drainedMetrics, dm)
		}
	}
	s.pendingData.AddItems(drainedMetrics)

	drainedMeta := s.metadataBuffer.Drain()
	for _, dm := range drainedMeta {
		s.pendingData.AddItems([]types.Datum{dm})
	}

	s.metadataBuffer = newWriteBuffer[types.MetadataDatum](0, cc, s.statshub.SendMetadataNetworkStats, s.logger)
	// Start the metadata buffer
	s.metricBuffers = make([]*writeBuffer[types.MetricDatum], 0, desiredConnections)
	for i := uint(0); i < desiredConnections; i++ {
		l := newWriteBuffer[types.MetricDatum](int(i), cc, s.statshub.SendSeriesNetworkStats, s.logger)
		s.metricBuffers = append(s.metricBuffers, l)
	}
	s.desiredParallelism.UpdateConfig(cc.Parallelism)
	return nil
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
