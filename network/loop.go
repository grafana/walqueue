package network

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"github.com/grafana/walqueue/types/v2"
	"github.com/prometheus/prometheus/model/histogram"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/grafana/walqueue/types"
	"github.com/prometheus/prometheus/prompb"
	"github.com/vladopajic/go-actor/actor"
	"go.uber.org/atomic"
)

var _ actor.Worker = (*loop)(nil)

// loop handles the low level sending of data. It's conceptually a queue.
// loop makes no attempt to save or restore signals in the queue.
// loop config cannot be updated, it is easier to recreate. This does mean we lose any signals in the queue.
type loop struct {
	isMeta         bool
	seriesMbx      actor.Mailbox[*types.Metric]
	client         *http.Client
	cfg            types.ConnectionConfig
	log            log.Logger
	lastSend       time.Time
	statsFunc      func(s types.NetworkStats)
	stopCalled     atomic.Bool
	externalLabels map[string]string
	self           actor.Actor
	ticker         *time.Ticker
	buf            *proto.Buffer
	sendBuffer     []byte
	series         *seriesSlice
	metaSlice      *metaSlice
}

func newLoop(cc types.ConnectionConfig, isMetaData bool, l log.Logger, stats func(s types.NetworkStats)) (*loop, error) {
	transport := &http.Transport{}

	// Configure TLS if certificate and key are provided
	if cc.TLSCert != "" && cc.TLSKey != "" {
		cert, err := tls.X509KeyPair([]byte(cc.TLSCert), []byte(cc.TLSKey))
		if err != nil {
			return nil, fmt.Errorf("failed to load TLS certificate and key: %w", err)
		}

		tlsConfig := &tls.Config{
			Certificates:       []tls.Certificate{cert},
			InsecureSkipVerify: cc.InsecureSkipVerify,
		}

		// Add CA cert to the cert pool if provided
		if cc.TLSCACert != "" {
			caCertPool := x509.NewCertPool()
			if !caCertPool.AppendCertsFromPEM([]byte(cc.TLSCACert)) {
				return nil, fmt.Errorf("failed to append CA certificate")
			}
			tlsConfig.RootCAs = caCertPool
		}

		transport.TLSClientConfig = tlsConfig
	}

	client := &http.Client{
		Transport: transport,
	}

	return &loop{
		isMeta:         isMetaData,
		seriesMbx:      actor.NewMailbox[*types.Metric](actor.OptCapacity(cc.BatchCount), actor.OptAsChan()),
		client:         client,
		cfg:            cc,
		log:            log.With(l, "name", "loop", "url", cc.URL),
		statsFunc:      stats,
		externalLabels: cc.ExternalLabels,
		ticker:         time.NewTicker(1 * time.Second),
		buf:            proto.NewBuffer(nil),
		sendBuffer:     make([]byte, 0),
		series:         &seriesSlice{m: make([]prompb.TimeSeries, 0)},
		metaSlice:      &metaSlice{m: make([]prompb.MetricMetadata, 0)},
	}, nil
}

func (l *loop) Start() {
	l.self = actor.Combine(l.actors()...).Build()
	l.self.Start()
}

func (l *loop) Stop() {
	l.stopCalled.Store(true)
	l.self.Stop()
}

func (l *loop) actors() []actor.Actor {
	return []actor.Actor{
		actor.New(l),
		l.seriesMbx,
	}
}

func (l *loop) DoWork(ctx actor.Context) actor.WorkerStatus {
	send := func(series *types.Metric) {
		defer types.PutMetricIntoPool(series)
		if l.isMeta {
			l.metaSlice.Add(toMetadata(series))
			if l.metaSlice.Len() >= l.cfg.BatchCount {
				l.trySend(nil, l.metaSlice.Slice(), ctx)
				l.metaSlice.Reset()
			}
		} else {
			l.series.Add(series, l.externalLabels)
			if l.series.Len() >= l.cfg.BatchCount {
				l.trySend(l.series.Slice(), nil, ctx)
				l.series.Reset()
			}
		}
	}
	// Main select loop
	select {
	case <-ctx.Done():
		return actor.WorkerEnd
	// Ticker is to ensure the flush timer is called.
	case <-l.ticker.C:
		if l.series.Len() == 0 && !l.isMeta {
			return actor.WorkerContinue
		}
		if l.metaSlice.Len() == 0 && l.isMeta {
			return actor.WorkerContinue
		}
		if time.Since(l.lastSend) > l.cfg.FlushInterval {
			if l.isMeta {
				l.trySend(nil, l.metaSlice.Slice(), ctx)
				l.metaSlice.Reset()
			} else {
				l.trySend(l.series.Slice(), nil, ctx)
				l.series.Reset()
			}

		}
		return actor.WorkerContinue
	case series, ok := <-l.seriesMbx.ReceiveC():
		if !ok {
			return actor.WorkerEnd
		}
		send(series)
		return actor.WorkerContinue
	}
}

// trySend is the core functionality for sending data to a endpoint. It will attempt retries as defined in MaxRetryAttempts.
func (l *loop) trySend(series []prompb.TimeSeries, meta []prompb.MetricMetadata, ctx context.Context) {
	attempts := 0
	defer l.sendingCleanup()
	for {
		start := time.Now()
		result := l.send(series, meta, ctx, attempts)
		duration := time.Since(start)
		l.statsFunc(types.NetworkStats{
			SendDuration: duration,
		})
		if result.err != nil {
			level.Error(l.log).Log("msg", "error in sending telemetry", "err", result.err.Error())
		}
		if result.successful {
			return
		}
		if !result.recoverableError {
			return
		}
		attempts++
		if attempts > int(l.cfg.MaxRetryAttempts) && l.cfg.MaxRetryAttempts > 0 {
			level.Debug(l.log).Log("msg", "max retry attempts reached", "attempts", attempts)
			return
		}
		// This helps us short circuit the loop if we are stopping.
		if l.stopCalled.Load() {
			return
		}
		// Sleep between attempts.
		time.Sleep(result.retryAfter)
	}
}

type sendResult struct {
	err              error
	successful       bool
	recoverableError bool
	retryAfter       time.Duration
	statusCode       int
	networkError     bool
}

func (l *loop) sendingCleanup() {
	l.sendBuffer = l.sendBuffer[:0]
	l.lastSend = time.Now()
}

// send is the main work loop of the loop.
func (l *loop) send(series []prompb.TimeSeries, meta []prompb.MetricMetadata, ctx context.Context, retryCount int) sendResult {
	result := sendResult{}
	defer func() {
		recordStats(series, meta, l.isMeta, l.statsFunc, result, len(l.sendBuffer))
	}()
	// Check to see if this is a retry and we can reuse the buffer.
	// I wonder if we should do this, its possible we are sending things that have exceeded the TTL.
	if len(l.sendBuffer) == 0 {
		var data []byte
		var wrErr error
		if l.isMeta {
			data, wrErr = createWriteRequestMetadata(meta, l.buf)
		} else {
			data, wrErr = createWriteRequest(series, l.buf)
		}
		if wrErr != nil {
			result.err = wrErr
			result.recoverableError = false
			return result
		}
		l.sendBuffer = snappy.Encode(l.sendBuffer, data)
	}

	httpReq, err := http.NewRequest("POST", l.cfg.URL, bytes.NewReader(l.sendBuffer))
	if err != nil {
		result.err = err
		result.recoverableError = true
		result.networkError = true
		return result
	}
	httpReq.Header.Add("Content-Encoding", "snappy")
	httpReq.Header.Set("Content-Type", "application/x-protobuf")
	httpReq.Header.Set("User-Agent", l.cfg.UserAgent)
	httpReq.Header.Set("X-Prometheus-Remote-Write-Version", "0.1.0")
	if l.cfg.BasicAuth != nil {
		httpReq.SetBasicAuth(l.cfg.BasicAuth.Username, l.cfg.BasicAuth.Password)
	} else if l.cfg.BearerToken != "" {
		httpReq.Header.Set("Authorization", "Bearer "+string(l.cfg.BearerToken))
	}

	if retryCount > 0 {
		httpReq.Header.Set("Retry-Attempt", strconv.Itoa(retryCount))
	}
	ctx, cncl := context.WithTimeout(ctx, l.cfg.Timeout)
	defer cncl()
	resp, err := l.client.Do(httpReq.WithContext(ctx))
	// Network errors are recoverable.
	if err != nil {
		result.err = err
		result.networkError = true
		result.recoverableError = true
		result.retryAfter = l.cfg.RetryBackoff
		return result
	}
	result.statusCode = resp.StatusCode
	defer resp.Body.Close()
	// 500 errors are considered recoverable.
	if resp.StatusCode/100 == 5 || resp.StatusCode == http.StatusTooManyRequests {
		result.err = fmt.Errorf("server responded with status code %d", resp.StatusCode)
		result.retryAfter = retryAfterDuration(l.cfg.RetryBackoff, resp.Header.Get("Retry-After"))
		result.recoverableError = true
		return result
	}
	// Status Codes that are not 500 or 200 are not recoverable and dropped.
	if resp.StatusCode/100 != 2 {
		scanner := bufio.NewScanner(io.LimitReader(resp.Body, 1_000))
		line := ""
		if scanner.Scan() {
			line = scanner.Text()
		}
		result.err = fmt.Errorf("server returned HTTP status %s: %s", resp.Status, line)
		return result
	}

	result.successful = true
	return result
}

func toSeries(m *types.Metric, ts prompb.TimeSeries, externalLabels map[string]string) prompb.TimeSeries {
	if cap(ts.Labels) < len(m.Labels) {
		ts.Labels = make([]prompb.Label, 0, len(m.Labels))
	}
	ts.Labels = ts.Labels[:len(m.Labels)]
	for k, v := range m.Labels {
		ts.Labels[k].Name = v.Name
		ts.Labels[k].Value = v.Value
	}

	// By default each sample only has a histogram, float histogram or sample.
	if cap(ts.Histograms) == 0 {
		ts.Histograms = make([]prompb.Histogram, 1)
	} else {
		ts.Histograms = ts.Histograms[:0]
	}
	if m.Histogram != nil {
		ts.Histograms = ts.Histograms[:1]
		ts.Histograms[0] = FromIntHistogram(m.TS, m.Histogram)
	}
	if m.FloatHistogram != nil {
		ts.Histograms = ts.Histograms[:1]
		ts.Histograms[0] = FromFloatHistogram(m.TS, m.FloatHistogram)

	}

	if m.Histogram == nil && m.FloatHistogram == nil {
		ts.Histograms = ts.Histograms[:0]
	}

	// Encode the external labels inside if needed.
	for k, v := range externalLabels {
		found := false
		for j, lbl := range ts.Labels {
			if lbl.Name == k {
				ts.Labels[j].Value = v
				found = true
				break
			}
		}
		if !found {
			ts.Labels = append(ts.Labels, prompb.Label{
				Name:  k,
				Value: v,
			})
		}
	}
	// By default each TimeSeries only has one sample.
	if len(ts.Samples) == 0 {
		ts.Samples = make([]prompb.Sample, 1)
	}
	ts.Samples[0].Value = m.Value
	ts.Samples[0].Timestamp = m.TS
	return ts
}

func createWriteRequest(series []prompb.TimeSeries, data *proto.Buffer) ([]byte, error) {
	wr := &prompb.WriteRequest{
		Timeseries: series,
	}

	// Reset the buffer for reuse.
	data.Reset()
	err := data.Marshal(wr)
	return data.Bytes(), err
}

func createWriteRequestMetadata(meta []prompb.MetricMetadata, data *proto.Buffer) ([]byte, error) {
	wr := &prompb.WriteRequest{
		Metadata: meta,
	}
	data.Reset()
	err := data.Marshal(wr)
	return data.Bytes(), err
}

func isMetadata(ts *types.Metric) bool {
	return ts.Labels.Has(v2.MetaType) &&
		ts.Labels.Has(v2.MetaUnit) &&
		ts.Labels.Has(v2.MetaHelp)
}

func toMetadata(ts *types.Metric) prompb.MetricMetadata {

	return prompb.MetricMetadata{
		Type:             prompb.MetricMetadata_MetricType(prompb.MetricMetadata_MetricType_value[strings.ToUpper(ts.Labels.Get(v2.MetaType))]),
		Help:             ts.Labels.Get(v2.MetaHelp),
		Unit:             ts.Labels.Get(v2.MetaUnit),
		MetricFamilyName: ts.Labels.Get("__name__"),
	}
}

func retryAfterDuration(defaultDuration time.Duration, t string) time.Duration {
	if parsedTime, err := time.Parse(http.TimeFormat, t); err == nil {
		return time.Until(parsedTime)
	}
	// The duration can be in seconds.
	d, err := strconv.Atoi(t)
	if err != nil {
		return defaultDuration
	}
	return time.Duration(d) * time.Second
}

// FromIntHistogram returns remote Histogram from the integer Histogram.
func FromIntHistogram(timestamp int64, h *histogram.Histogram) prompb.Histogram {
	return prompb.Histogram{
		Count:          &prompb.Histogram_CountInt{CountInt: h.Count},
		Sum:            h.Sum,
		Schema:         h.Schema,
		ZeroThreshold:  h.ZeroThreshold,
		ZeroCount:      &prompb.Histogram_ZeroCountInt{ZeroCountInt: h.ZeroCount},
		NegativeSpans:  spansToSpansProto(h.NegativeSpans),
		NegativeDeltas: h.NegativeBuckets,
		PositiveSpans:  spansToSpansProto(h.PositiveSpans),
		PositiveDeltas: h.PositiveBuckets,
		ResetHint:      prompb.Histogram_ResetHint(h.CounterResetHint),
		Timestamp:      timestamp,
	}
}

// FromFloatHistogram returns remote Histogram from the float Histogram.
func FromFloatHistogram(timestamp int64, fh *histogram.FloatHistogram) prompb.Histogram {
	return prompb.Histogram{
		Count:          &prompb.Histogram_CountFloat{CountFloat: fh.Count},
		Sum:            fh.Sum,
		Schema:         fh.Schema,
		ZeroThreshold:  fh.ZeroThreshold,
		ZeroCount:      &prompb.Histogram_ZeroCountFloat{ZeroCountFloat: fh.ZeroCount},
		NegativeSpans:  spansToSpansProto(fh.NegativeSpans),
		NegativeCounts: fh.NegativeBuckets,
		PositiveSpans:  spansToSpansProto(fh.PositiveSpans),
		PositiveCounts: fh.PositiveBuckets,
		ResetHint:      prompb.Histogram_ResetHint(fh.CounterResetHint),
		Timestamp:      timestamp,
	}
}

func spansToSpansProto(s []histogram.Span) []prompb.BucketSpan {
	spans := make([]prompb.BucketSpan, len(s))
	for i := 0; i < len(s); i++ {
		spans[i] = prompb.BucketSpan{Offset: s[i].Offset, Length: s[i].Length}
	}

	return spans
}

var tsPool = sync.Pool{New: func() interface{} { return &prompb.WriteRequest{} }}
