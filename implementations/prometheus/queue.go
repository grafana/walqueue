package prometheus

import (
	"context"
	"sync"
	"time"

	"github.com/grafana/walqueue/stats"
	v2 "github.com/grafana/walqueue/types/v2"

	snappy "github.com/eapache/go-xerial-snappy"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/walqueue/filequeue"
	"github.com/grafana/walqueue/network"
	"github.com/grafana/walqueue/serialization"
	"github.com/grafana/walqueue/types"
	v1 "github.com/grafana/walqueue/types/v1"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/storage"
	"github.com/vladopajic/go-actor/actor"
)

var pool = sync.Pool{New: func() interface{} { return make([]byte, 0) }}
var _ storage.Appendable = (*queue)(nil)
var _ Queue = (*queue)(nil)

// Queue is the interface for a prometheus compatible queue. The queue is an append only interface.
//
// Start will start the queue.
//
// Stop will stop the queue.
//
// Appender returns an Appender that writes to the queue.
type Queue interface {
	Start()
	Stop()
	Appender(ctx context.Context) storage.Appender
}

// queue is a simple example of using the wal queue.
type queue struct {
	network        types.NetworkClient
	queue          types.FileStorage
	logger         log.Logger
	serializer     types.PrometheusSerializer
	self           actor.Actor
	ttl            time.Duration
	incoming       actor.Mailbox[types.DataHandle]
	stats          *PrometheusStats
	metaStats      *PrometheusStats
	externalLabels map[string]string
	ctx            context.Context
	cncl           context.CancelFunc
}

// NewQueue creates and returns a new Queue instance, initializing its components
// such as network client, file storage queue, and serializer. It configures the
// queue with the given connection settings, directory for file storage, batching
// parameters, and logging. The function also sets up the statistics callback functions
// for network and serialization metrics.
//
// Parameters:
// - name: identifier for the endpoint, this will add a label to the prometheus metrics named endpoint:<NAME>
// - cc: ConnectionConfig for setting up the network client.
// - directory: Directory path for storing queue files.
// - maxSignalsToBatch: Maximum number of signals to batch before flushing to file storage.
// - flushInterval: Duration for how often to flush the data to file storage.
// - ttl: Time-to-live for data in the queue, this is checked in both writing to file storage and sending to the network.
// - registry: Prometheus registry to apply metrics to.
// - namespace: Namespace to use to add to the metric family names. IE `alloy` would make `alloy_queue_series_total_sent`
// - logger: Logger for logging internal operations and errors.
//
// Returns:
// - Queue: An initialized Queue instance.
// - error: An error if any of the components fail to initialize.
func NewQueue(name string, cc types.ConnectionConfig, directory string, maxSignalsToBatch uint32, flushInterval time.Duration, ttl time.Duration, registerer prometheus.Registerer, namespace string, logger log.Logger) (Queue, error) {
	sh := stats.NewStats()
	reg := prometheus.WrapRegistererWith(prometheus.Labels{"endpoint": name}, registerer)
	statsHub := NewStats(namespace, "queue_series", false, reg, sh)
	statsHub.SeriesBackwardsCompatibility(reg)
	meta := NewStats("alloy", "queue_metadata", true, reg, sh)
	meta.MetaBackwardsCompatibility(reg)

	networkClient, err := network.New(cc, logger, sh)
	if err != nil {
		return nil, err
	}
	ctx := context.Background()
	ctx, cncl := context.WithCancel(ctx)
	q := &queue{
		incoming:       actor.NewMailbox[types.DataHandle](),
		stats:          statsHub,
		metaStats:      meta,
		network:        networkClient,
		logger:         logger,
		ttl:            ttl,
		externalLabels: cc.ExternalLabels,
		ctx:            ctx,
		cncl:           cncl,
	}
	fq, err := filequeue.NewQueue(directory, func(ctx context.Context, dh types.DataHandle) {
		sendErr := q.incoming.Send(ctx, dh)
		if sendErr != nil {
			level.Error(logger).Log("msg", "failed to send to incoming", "err", sendErr)
		}
	}, logger)
	if err != nil {
		return nil, err
	}
	q.queue = fq
	serial, err := serialization.NewSerializer(types.SerializerConfig{
		MaxSignalsInBatch: maxSignalsToBatch,
		FlushFrequency:    flushInterval,
	}, q.queue, statsHub.UpdateSerializer, logger)
	if err != nil {
		return nil, err
	}
	q.serializer = serial
	return q, nil
}

func (q *queue) Start() {
	q.self = actor.New(q)
	q.self.Start()
	q.incoming.Start()
	q.network.Start(q.ctx)
	q.queue.Start()
	q.serializer.Start(q.ctx)
}

func (q *queue) Stop() {
	q.self.Stop()
	q.incoming.Stop()
	q.network.Stop()
	q.queue.Stop()
	q.serializer.Stop()
	q.cncl()

	q.stats.Unregister()
	q.metaStats.Unregister()
}

func (q *queue) DoWork(ctx actor.Context) actor.WorkerStatus {
	select {
	case <-ctx.Done():
		return actor.WorkerEnd
	case file, ok := <-q.incoming.ReceiveC():
		if !ok {
			return actor.WorkerEnd
		}
		meta, buf, err := file.Pop()
		if err != nil {
			level.Error(q.logger).Log("msg", "unable to get file contents", "name", file.Name, "err", err)
			return actor.WorkerContinue
		}
		q.deserializeAndSend(ctx, meta, buf)
		return actor.WorkerContinue
	}
}

// Appender returns a new appender for the storage.
func (q *queue) Appender(ctx context.Context) storage.Appender {
	return serialization.NewAppender(ctx, 0, q.serializer, q.externalLabels, q.logger)
}

func (q *queue) deserializeAndSend(ctx context.Context, meta map[string]string, buf []byte) {
	var err error
	uncompressedBuf := pool.Get().([]byte)
	defer pool.Put(uncompressedBuf)

	uncompressedBuf, err = snappy.DecodeInto(uncompressedBuf, buf)
	if err != nil {
		level.Debug(q.logger).Log("msg", "error snappy decoding", "err", err)
		return
	}
	// The version of each file is in the metadata. Right now there is only one version
	// supported but in the future the ability to support more. Along with different
	// compression.
	version, ok := meta["version"]
	if !ok {
		level.Error(q.logger).Log("msg", "version not found for deserialization")
		return
	}
	var items []types.Datum
	var s types.Unmarshaller
	switch types.FileFormat(version) {
	case types.AlloyFileVersionV1:
		s = v1.GetSerializer()
		items, err = s.Unmarshal(meta, uncompressedBuf)
	case types.AlloyFileVersionV2:
		s = v2.NewFormat()
		items, err = s.Unmarshal(meta, uncompressedBuf)
	default:
		level.Error(q.logger).Log("msg", "invalid version found for deserialization", "version", version)
		return
	}
	if err != nil {
		level.Error(q.logger).Log("msg", "error deserializing", "err", err, "format", version)
	}

	for _, series := range items {
		// Check that the TTL.
		mm, valid := series.(types.MetricDatum)
		if valid {
			seriesAge := time.Since(time.UnixMilli(mm.TimeStampMS()))
			// For any series that exceeds the time to live (ttl) based on its timestamp we do not want to push it to the networking layer
			// but instead drop it here by continuing.
			if seriesAge > q.ttl {
				mm.Free()
				q.stats.NetworkTTLDrops.Inc()
				continue
			}
			sendErr := q.network.SendSeries(ctx, mm)
			if sendErr != nil {
				level.Error(q.logger).Log("msg", "error sending to write client", "err", sendErr)
			}
			continue
		}
		md, valid := series.(types.MetadataDatum)
		if valid {

			sendErr := q.network.SendMetadata(ctx, md)
			if sendErr != nil {
				level.Error(q.logger).Log("msg", "error sending metadata to write client", "err", sendErr)
			}
		}

	}
}
