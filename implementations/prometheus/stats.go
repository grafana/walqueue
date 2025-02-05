package prometheus

import (
	"sync/atomic"

	"github.com/grafana/walqueue/types"
	"github.com/prometheus/client_golang/prometheus"
)

type PrometheusStats struct {
	serializerIn       atomic.Int64
	networkOut         atomic.Int64
	register           prometheus.Registerer
	stats              types.StatsHub
	isMeta             bool
	serialRelease      types.NotificationRelease
	networkRelease     types.NotificationRelease
	parralelismRelease types.NotificationRelease

	// Parralelism
	ParralelismMin     prometheus.Gauge
	ParralelismMax     prometheus.Gauge
	ParralelismDesired prometheus.Gauge

	// Network Stats
	NetworkSeriesSent                prometheus.Counter
	NetworkFailures                  prometheus.Counter
	NetworkRetries                   prometheus.Counter
	NetworkRetries429                prometheus.Counter
	NetworkRetries5XX                prometheus.Counter
	NetworkSentDuration              prometheus.Histogram
	NetworkErrors                    prometheus.Counter
	NetworkNewestOutTimeStampSeconds prometheus.Gauge
	NetworkTTLDrops                  prometheus.Counter

	// Drift between serializer input and network output
	TimestampDriftSeconds prometheus.Gauge

	// Serializer Stats
	SerializerInSeries                 prometheus.Counter
	SerializerNewestInTimeStampSeconds prometheus.Gauge
	SerializerErrors                   prometheus.Counter

	// Backwards compatibility metrics
	SamplesTotal    prometheus.Counter
	HistogramsTotal prometheus.Counter
	MetadataTotal   prometheus.Counter

	FailedSamplesTotal    prometheus.Counter
	FailedHistogramsTotal prometheus.Counter
	FailedMetadataTotal   prometheus.Counter

	RetriedSamplesTotal    prometheus.Counter
	RetriedHistogramsTotal prometheus.Counter
	RetriedMetadataTotal   prometheus.Counter

	EnqueueRetriesTotal  prometheus.Counter
	SentBatchDuration    prometheus.Histogram
	HighestSentTimestamp prometheus.Gauge

	SentBytesTotal            prometheus.Counter
	MetadataBytesTotal        prometheus.Counter
	RemoteStorageInTimestamp  prometheus.Gauge
	RemoteStorageOutTimestamp prometheus.Gauge
	RemoteStorageDuration     prometheus.Histogram
}

func NewStats(namespace, subsystem string, isMeta bool, registry prometheus.Registerer, sh types.StatsHub) *PrometheusStats {
	s := &PrometheusStats{
		stats:    sh,
		register: registry,
		isMeta:   isMeta,
		ParralelismMax: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "parralelism_max",
		}),
		ParralelismMin: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "parralelism_min",
		}),
		ParralelismDesired: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "parralelism_desired",
		}),
		SerializerInSeries: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "serializer_incoming_signals",
		}),
		SerializerNewestInTimeStampSeconds: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "serializer_incoming_timestamp_seconds",
		}),
		SerializerErrors: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "serializer_errors",
		}),
		NetworkNewestOutTimeStampSeconds: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "network_timestamp_seconds",
		}),
		NetworkTTLDrops: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "ttl_drops_total",
			Help:      "Total number of series dropped due to TTL expiration",
		}),

		TimestampDriftSeconds: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "timestamp_drift_seconds",
			Help:      "Drift between newest serializer input timestamp and newest network output timestamp",
		}),
		RemoteStorageDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "prometheus_remote_storage_queue_duration_seconds",
		}),
		NetworkSeriesSent: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "network_sent",
		}),
		NetworkFailures: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "network_failed",
		}),
		NetworkRetries: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "network_retried",
		}),
		NetworkRetries429: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "network_retried_429",
		}),
		NetworkRetries5XX: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "network_retried_5xx",
		}),
		NetworkSentDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace:                   namespace,
			Subsystem:                   subsystem,
			Name:                        "network_duration_seconds",
			NativeHistogramBucketFactor: 1.1,
		}),
		NetworkErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "network_errors",
		}),
		RemoteStorageOutTimestamp: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "prometheus_remote_storage_queue_highest_sent_timestamp_seconds",
		}),
		RemoteStorageInTimestamp: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "prometheus_remote_storage_highest_timestamp_in_seconds",
		}),
		SamplesTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "prometheus_remote_storage_samples_total",
			Help: "Total number of samples sent to remote storage.",
		}),
		HistogramsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "prometheus_remote_storage_histograms_total",
			Help: "Total number of histograms sent to remote storage.",
		}),
		MetadataTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "prometheus_remote_storage_metadata_total",
			Help: "Total number of metadata sent to remote storage.",
		}),
		FailedSamplesTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "prometheus_remote_storage_samples_failed_total",
			Help: "Total number of samples which failed on send to remote storage, non-recoverable errors.",
		}),
		FailedHistogramsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "prometheus_remote_storage_histograms_failed_total",
			Help: "Total number of histograms which failed on send to remote storage, non-recoverable errors.",
		}),
		FailedMetadataTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "prometheus_remote_storage_metadata_failed_total",
			Help: "Total number of metadata entries which failed on send to remote storage, non-recoverable errors.",
		}),

		RetriedSamplesTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "prometheus_remote_storage_samples_retried_total",
			Help: "Total number of samples which failed on send to remote storage but were retried because the send error was recoverable.",
		}),
		RetriedHistogramsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "prometheus_remote_storage_histograms_retried_total",
			Help: "Total number of histograms which failed on send to remote storage but were retried because the send error was recoverable.",
		}),
		RetriedMetadataTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "prometheus_remote_storage_metadata_retried_total",
			Help: "Total number of metadata entries which failed on send to remote storage but were retried because the send error was recoverable.",
		}),
		SentBytesTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "prometheus_remote_storage_sent_bytes_total",
			Help: "The total number of bytes of data (not metadata) sent by the queue after compression. Note that when exemplars over remote write is enabled the exemplars included in a remote write request count towards this metric.",
		}),
		MetadataBytesTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "prometheus_remote_storage_metadata_bytes_total",
			Help: "The total number of bytes of metadata sent by the queue after compression.",
		}),
	}
	if isMeta {
		s.networkRelease = s.stats.RegisterMetadataNetwork(s.UpdateNetwork)
	} else {
		s.networkRelease = s.stats.RegisterSeriesNetwork(s.UpdateNetwork)
	}
	s.serialRelease = s.stats.RegisterSerializer(s.UpdateSerializer)
	s.parralelismRelease = s.stats.RegisterParralelism(s.UpdateParralelism)
	registry.MustRegister(
		s.NetworkSentDuration,
		s.NetworkRetries5XX,
		s.NetworkRetries429,
		s.NetworkRetries,
		s.NetworkFailures,
		s.NetworkSeriesSent,
		s.NetworkErrors,
		s.NetworkNewestOutTimeStampSeconds,
		s.NetworkTTLDrops,
		s.SerializerInSeries,
		s.SerializerErrors,
		s.SerializerNewestInTimeStampSeconds,
		s.TimestampDriftSeconds,
	)
	// Metadata doesn't scale, it has one dedicated connection.
	if !isMeta {
		registry.MustRegister(
			s.ParralelismMax,
			s.ParralelismMin,
			s.ParralelismDesired)
	}
	return s
}

func (s *PrometheusStats) Unregister() {
	unregistered := []prometheus.Collector{
		s.RemoteStorageDuration,
		s.RemoteStorageInTimestamp,
		s.RemoteStorageOutTimestamp,
		s.SamplesTotal,
		s.HistogramsTotal,
		s.FailedSamplesTotal,
		s.FailedHistogramsTotal,
		s.RetriedSamplesTotal,
		s.RetriedHistogramsTotal,
		s.SentBytesTotal,
		s.MetadataTotal,
		s.FailedMetadataTotal,
		s.RetriedMetadataTotal,
		s.MetadataBytesTotal,
		s.NetworkSentDuration,
		s.NetworkRetries5XX,
		s.NetworkRetries429,
		s.NetworkRetries,
		s.NetworkFailures,
		s.NetworkSeriesSent,
		s.NetworkErrors,
		s.NetworkNewestOutTimeStampSeconds,
		s.NetworkTTLDrops,
		s.SerializerInSeries,
		s.SerializerErrors,
		s.SerializerNewestInTimeStampSeconds,
		s.TimestampDriftSeconds,
	}
	// Meta only has one connection so we dont need these for that.
	if !s.isMeta {
		unregistered = append(unregistered, s.ParralelismMin, s.ParralelismMax, s.ParralelismDesired)
	}

	for _, g := range unregistered {
		s.register.Unregister(g)
	}
	s.networkRelease()
	s.serialRelease()
}

func (s *PrometheusStats) SeriesBackwardsCompatibility(registry prometheus.Registerer) {
	registry.MustRegister(
		s.RemoteStorageDuration,
		s.RemoteStorageInTimestamp,
		s.RemoteStorageOutTimestamp,
		s.SamplesTotal,
		s.HistogramsTotal,
		s.FailedSamplesTotal,
		s.FailedHistogramsTotal,
		s.RetriedSamplesTotal,
		s.RetriedHistogramsTotal,
		s.SentBytesTotal,
	)
}

func (s *PrometheusStats) MetaBackwardsCompatibility(registry prometheus.Registerer) {
	registry.MustRegister(
		s.MetadataTotal,
		s.FailedMetadataTotal,
		s.RetriedMetadataTotal,
		s.MetadataBytesTotal,
	)
}

func (s *PrometheusStats) UpdateNetwork(stats types.NetworkStats) {
	s.NetworkSeriesSent.Add(float64(stats.TotalSent()))
	s.NetworkRetries.Add(float64(stats.TotalRetried()))
	s.NetworkFailures.Add(float64(stats.TotalFailed()))
	s.NetworkRetries429.Add(float64(stats.Total429()))
	s.NetworkRetries5XX.Add(float64(stats.Total5XX()))
	s.NetworkSentDuration.Observe(stats.SendDuration.Seconds())
	s.RemoteStorageDuration.Observe(stats.SendDuration.Seconds())
	// The newest timestamp is not always sent.
	if stats.NewestTimestampSeconds != 0 {
		s.networkOut.Store(stats.NewestTimestampSeconds)
		s.updateDrift()
		s.RemoteStorageOutTimestamp.Set(float64(stats.NewestTimestampSeconds))
		s.NetworkNewestOutTimeStampSeconds.Set(float64(stats.NewestTimestampSeconds))
	}

	s.SamplesTotal.Add(float64(stats.Series.SeriesSent))
	s.MetadataTotal.Add(float64(stats.Metadata.SeriesSent))
	s.HistogramsTotal.Add(float64(stats.Histogram.SeriesSent))

	s.FailedSamplesTotal.Add(float64(stats.Series.FailedSamples))
	s.FailedMetadataTotal.Add(float64(stats.Metadata.FailedSamples))
	s.FailedHistogramsTotal.Add(float64(stats.Histogram.FailedSamples))

	s.RetriedSamplesTotal.Add(float64(stats.Series.RetriedSamples))
	s.RetriedHistogramsTotal.Add(float64(stats.Histogram.RetriedSamples))
	s.RetriedMetadataTotal.Add(float64(stats.Metadata.RetriedSamples))

	s.MetadataBytesTotal.Add(float64(stats.MetadataBytes))
	s.SentBytesTotal.Add(float64(stats.SeriesBytes))
}

func (s *PrometheusStats) UpdateSerializer(stats types.SerializerStats) {
	s.SerializerInSeries.Add(float64(stats.SeriesStored))
	s.SerializerInSeries.Add(float64(stats.MetadataStored))
	s.SerializerErrors.Add(float64(stats.Errors))
	if stats.NewestTimestampSeconds != 0 {
		s.serializerIn.Store(stats.NewestTimestampSeconds)
		s.updateDrift()
		s.SerializerNewestInTimeStampSeconds.Set(float64(stats.NewestTimestampSeconds))
		s.RemoteStorageInTimestamp.Set(float64(stats.NewestTimestampSeconds))
	}
}

func (s *PrometheusStats) UpdateParralelism(stats types.ParralelismStats) {
	s.ParralelismMax.Set(float64(stats.MaxConnections))
	s.ParralelismMin.Set(float64(stats.MinConnections))
	s.ParralelismDesired.Set(float64(stats.DesiredConnections))
}

func (s *PrometheusStats) updateDrift() {
	// We always want to ensure that we have real values, else there is a window where this can be
	// timestamp - 0 which gives a result in the years.
	if s.serializerIn.Load() != 0 && s.networkOut.Load() != 0 {
		drift := uint(s.serializerIn.Load() - s.networkOut.Load())
		s.TimestampDriftSeconds.Set(float64(drift))
	}
}
