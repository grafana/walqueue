package prometheus

import (
	"context"
	"strconv"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/golang/snappy"
	"github.com/grafana/walqueue/filequeue"
	"github.com/grafana/walqueue/network"
	"github.com/grafana/walqueue/serialization"
	"github.com/grafana/walqueue/stats"
	"github.com/grafana/walqueue/types"
	v1 "github.com/grafana/walqueue/types/v1"
	v2 "github.com/grafana/walqueue/types/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/storage"
)

var (
	_ storage.Appendable = (*queue)(nil)
	_ Queue              = (*queue)(nil)
)

// Queue is the interface for a prometheus compatible queue. The queue is an append only interface.
//
// Start will start the queue.
//
// Stop will stop the queue.
//
// Appender returns an Appender that writes to the queue.
type Queue interface {
	Start(ctx context.Context) error
	Stop()
	Appender(ctx context.Context) storage.Appender
}

// queue is a simple example of using the wal queue.
type queue struct {
	network                   types.NetworkClient
	queue                     types.FileStorage
	logger                    log.Logger
	serializer                types.PrometheusSerializer
	ttl                       time.Duration
	incoming                  *types.Mailbox[types.DataHandle]
	stats                     *Stats
	metaStats                 *Stats
	externalLabels            map[string]string
	networkRequestMoreSignals chan types.RequestMoreSignals[types.Datum]
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
	statshub := stats.NewStats()
	reg := prometheus.WrapRegistererWith(prometheus.Labels{"endpoint": name}, registerer)
	seriesStats := NewStats(namespace, "queue_series", false, reg, statshub)
	seriesStats.SeriesBackwardsCompatibility(reg)
	meta := NewStats("alloy", "queue_metadata", true, reg, statshub)
	meta.MetaBackwardsCompatibility(reg)
	// the length 3 allows a buffer of three files, setting to zero will starve and block the queue.
	networkRequestMoreSignals := make(chan types.RequestMoreSignals[types.Datum], 4)

	networkClient, err := network.New(cc, logger, statshub, networkRequestMoreSignals)
	if err != nil {
		return nil, err
	}
	q := &queue{
		incoming:                  types.NewMailbox[types.DataHandle](),
		stats:                     seriesStats,
		metaStats:                 meta,
		network:                   networkClient,
		logger:                    logger,
		ttl:                       ttl,
		networkRequestMoreSignals: networkRequestMoreSignals,
		externalLabels:            cc.ExternalLabels,
	}
	fq, err := filequeue.NewQueue(directory, func(ctx context.Context, dh types.DataHandle) {
		sendErr := q.incoming.Send(ctx, dh)
		if sendErr != nil {
			level.Error(logger).Log("msg", "failed to send to incoming", "err", sendErr)
		}
	}, statshub, logger)
	if err != nil {
		return nil, err
	}
	q.queue = fq
	serial, err := serialization.NewSerializer(types.SerializerConfig{
		MaxSignalsInBatch: maxSignalsToBatch,
		FlushFrequency:    flushInterval,
	}, q.queue, statshub.SendSerializerStats, logger)
	if err != nil {
		return nil, err
	}
	q.serializer = serial
	return q, nil
}

func (q *queue) Start(ctx context.Context) error {
	q.network.Start(ctx)
	q.queue.Start(ctx)
	err := q.serializer.Start(ctx)
	if err != nil {
		q.network.Stop()
		q.queue.Stop()
		return err
	}
	go q.run(ctx)
	return nil
}

func (q *queue) Stop() {
	q.network.Stop()
	q.queue.Stop()
	q.serializer.Stop()
	q.stats.Unregister()
	q.metaStats.Unregister()
}

func (q *queue) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case req, ok := <-q.networkRequestMoreSignals:
			if !ok {
				return
			}
			responseSent := false
			for !responseSent {
				select {
				case <-ctx.Done():
					return
				case file, fileOk := <-q.incoming.ReceiveC():
					if !fileOk {
						return
					}
					meta, buf, err := file.Pop()
					if err != nil {
						level.Error(q.logger).Log("msg", "unable to get file contents", "name", file.Name, "err", err)
						continue
					}
					items := q.deserializeAndSend(meta, buf)
					// This is to handle the case where we dont get any items, we want to move on to the next file.
					// This is generally due to some problem reading the file.
					if len(items) == 0 {
						continue
					}
					req.Response <- items
					responseSent = true
				}
			}
		}
	}
}

// Appender returns a new appender for the storage.
func (q *queue) Appender(ctx context.Context) storage.Appender {
	return serialization.NewAppender(ctx, 0, q.serializer, q.externalLabels, q.logger)
}

func (q *queue) deserializeAndSend(meta map[string]string, buf []byte) []types.Datum {
	compressedBytes := len(buf)
	uncompressedBuf, err := snappy.Decode(nil, buf)
	if err != nil {
		level.Debug(q.logger).Log("msg", "error snappy decoding", "err", err)
		return nil
	}
	uncompressedBytes := len(uncompressedBuf)
	defer func() {
		fileID := -1
		strId, found := meta["file_id"]
		if found {
			fileID, err = strconv.Atoi(strId)
			if err != nil {
				fileID = -1
			}
		}
		q.stats.UpdateSerializer(types.SerializerStats{
			FileIDRead:            fileID,
			UncompressedBytesRead: uncompressedBytes,
			CompressedBytesRead:   compressedBytes,
		})
	}()
	// The version of each file is in the metadata. Right now there is only one version
	// supported but in the future the ability to support more. Along with different
	// compression.
	version, ok := meta["version"]
	if !ok {
		level.Error(q.logger).Log("msg", "version not found for deserialization")
		return nil
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
		return nil
	}
	if err != nil {
		level.Error(q.logger).Log("msg", "error deserializing", "err", err, "format", version)
	}

	pending := make([]types.Datum, 0, len(items))

	// Process all deserialized items
	for _, series := range items {
		// Check if this is a metric datum
		mm, isMetric := series.(types.MetricDatum)
		if isMetric {
			seriesAge := time.Since(time.UnixMilli(mm.TimeStampMS()))
			// For any series that exceeds the time to live (ttl) based on its timestamp we do not want to push it to the networking layer
			// but instead drop it here by continuing.
			if seriesAge > q.ttl {
				mm.Free()
				q.stats.NetworkTTLDrops.Inc()
				continue
			}
			pending = append(pending, mm)
			continue
		}

		// Check if this is metadata
		md, isMetadata := series.(types.MetadataDatum)
		if isMetadata {
			pending = append(pending, md)
			continue
		}
	}
	return pending
}
