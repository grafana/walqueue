package walqueue

import (
	"context"
	"strconv"
	"time"

	snappy "github.com/eapache/go-xerial-snappy"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/walqueue/v2/filequeue"
	"github.com/grafana/walqueue/v2/network"
	"github.com/grafana/walqueue/v2/serialization"
	"github.com/grafana/walqueue/v2/types"
	"github.com/prometheus/prometheus/storage"
	"github.com/vladopajic/go-actor/actor"
)

var _ storage.Appendable = (*queue)(nil)
var _ Queue = (*queue)(nil)

// Queue is the interface for a queue. The queue is an append only interface.
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
	network    types.NetworkClient
	queue      types.FileStorage
	logger     log.Logger
	serializer types.Serializer
	self       actor.Actor
	ttl        time.Duration
	incoming   actor.Mailbox[types.DataHandle]
	stats      func(stats types.NetworkStats)
	metaStats  func(stats types.NetworkStats)
	buf        []byte
}

// NewQueue creates and returns a new Queue instance, initializing its components
// such as network client, file storage queue, and serializer. It configures the
// queue with the given connection settings, directory for file storage, batching
// parameters, and logging. The function also sets up the statistics callback functions
// for network and serialization metrics.
//
// Parameters:
// - cc: ConnectionConfig for setting up the network client.
// - directory: Directory path for storing queue files.
// - maxSignalsToBatch: Maximum number of signals to batch before flushing to file storage.
// - flushInterval: Duration for how often to flush the data to file storage.
// - ttl: Time-to-live for data in the queue, this is checked in both writing to file storage and sending to the network.
// - logger: Logger for logging internal operations and errors.
// - stats: Callback function for reporting network statistics.
// - metaStats: Callback function for reporting metadata-related statistics.
// - serialStats: Callback function for reporting serializer statistics.
//
// Returns:
// - Queue: An initialized Queue instance.
// - error: An error if any of the components fail to initialize.
func NewQueue(cc types.ConnectionConfig, directory string, maxSignalsToBatch uint32, flushInterval time.Duration, ttl time.Duration, logger log.Logger, stats, metaStats func(stats types.NetworkStats), serialStats func(stats types.SerializerStats)) (Queue, error) {
	network, err := network.New(cc, logger, stats, metaStats)
	if err != nil {
		return nil, err
	}
	q := &queue{
		incoming:  actor.NewMailbox[types.DataHandle](),
		stats:     stats,
		metaStats: metaStats,
		network:   network,
		logger:    logger,
		ttl:       ttl,
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
	}, q.queue, serialStats, logger)
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
	q.network.Start()
	q.queue.Start()
	q.serializer.Start()
}

func (q *queue) Stop() {
	q.self.Stop()
	q.incoming.Stop()
	q.network.Stop()
	q.queue.Stop()
	q.serializer.Stop()
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
	return serialization.NewAppender(ctx, 0, q.serializer, q.logger)
}

func (q *queue) deserializeAndSend(ctx context.Context, meta map[string]string, buf []byte) {
	var err error
	q.buf, err = snappy.DecodeInto(q.buf, buf)
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
	if version != types.AlloyFileVersion {
		level.Error(q.logger).Log("msg", "invalid version found for deserialization", "version", version)
		return
	}
	// Grab the amounts of each type and we can go ahead and alloc the space.
	seriesCount, _ := strconv.Atoi(meta["series_count"])
	metaCount, _ := strconv.Atoi(meta["meta_count"])
	stringsCount, _ := strconv.Atoi(meta["strings_count"])
	sg := &types.SeriesGroup{
		Series:   make([]*types.TimeSeriesBinary, seriesCount),
		Metadata: make([]*types.TimeSeriesBinary, metaCount),
		Strings:  make([]string, stringsCount),
	}
	// Prefill our series with items from the pool to limit allocs.
	for i := 0; i < seriesCount; i++ {
		sg.Series[i] = types.GetTimeSeriesFromPool()
	}
	for i := 0; i < metaCount; i++ {
		sg.Metadata[i] = types.GetTimeSeriesFromPool()
	}
	sg, q.buf, err = types.DeserializeToSeriesGroup(sg, q.buf)
	if err != nil {
		level.Debug(q.logger).Log("msg", "error deserializing", "err", err)
		return
	}

	for _, series := range sg.Series {
		// One last chance to check the TTL. Writing to the filequeue will check it but
		// in a situation where the network is down and writing backs up we dont want to send
		// data that will get rejected.
		seriesAge := time.Since(time.Unix(series.TS, 0))
		if seriesAge > q.ttl {
			// TODO @mattdurham add metric here for ttl expired.
			continue
		}
		sendErr := q.network.SendSeries(ctx, series)
		if sendErr != nil {
			level.Error(q.logger).Log("msg", "error sending to write client", "err", sendErr)
		}
	}

	for _, md := range sg.Metadata {
		sendErr := q.network.SendMetadata(ctx, md)
		if sendErr != nil {
			level.Error(q.logger).Log("msg", "error sending metadata to write client", "err", sendErr)
		}
	}
}
