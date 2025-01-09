package network

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/grafana/walqueue/types"
	"github.com/prometheus/common/config"
	"github.com/prometheus/prometheus/prompb"
	"github.com/vladopajic/go-actor/actor"
	"go.uber.org/atomic"
)

var _ actor.Worker = (*loop[types.MetricDatum])(nil)

// loop handles the low level sending of data. It's conceptually a queue.
// loop makes no attempt to save or restore signals in the queue.
// loop config cannot be updated, it is easier to recreate. This does mean we lose any signals in the queue.
type loop[T types.Datum] struct {
	isMeta         bool
	seriesMbx      actor.Mailbox[T]
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
	items          *datumSlice[T]
}

func newLoop[T types.Datum](cc types.ConnectionConfig, isMetaData bool, l log.Logger, stats func(s types.NetworkStats)) (*loop, error) {
	var httpOpts []config.HTTPClientOption
	if cc.UseRoundRobin {
		httpOpts = []config.HTTPClientOption{config.WithDialContextFunc(newDialContextWithRoundRobinDNS().dialContextFn())}
	}

	cfg := cc.ToPrometheusConfig()
	httpClient, err := config.NewClientFromConfig(cfg, "remote_write", httpOpts...)

	if err != nil {
		return nil, err
	}
	return &loop[T]{
		isMeta:         isMetaData,
		seriesMbx:      actor.NewMailbox[T](actor.OptCapacity(cc.BatchCount), actor.OptAsChan()),
		client:         httpClient,
		cfg:            cc,
		log:            log.With(l, "name", "loop", "url", cc.URL),
		statsFunc:      stats,
		externalLabels: cc.ExternalLabels,
		ticker:         time.NewTicker(1 * time.Second),
		buf:            proto.NewBuffer(nil),
		sendBuffer:     make([]byte, 0),
		items:          &datumSlice[T]{m: make([]T, 0)},
	}, nil
}

func (l *loop[T]) Start() {
	l.self = actor.Combine(l.actors()...).Build()
	l.self.Start()
}

func (l *loop[T]) Stop() {
	l.stopCalled.Store(true)
	l.self.Stop()
}

func (l *loop[T]) actors() []actor.Actor {
	return []actor.Actor{
		actor.New(l),
		l.seriesMbx,
	}
}

func (l *loop[T]) DoWork(ctx actor.Context) actor.WorkerStatus {
	send := func(series T) {
		l.items.Add(series)
		if l.items.Len() >= l.cfg.BatchCount {
			l.trySend(l.items.SliceAndReset(), ctx)
		}

	}
	// Main select loop
	select {
	case <-ctx.Done():
		return actor.WorkerEnd
	// Ticker is to ensure the flush timer is called.
	case <-l.ticker.C:
		if l.items.Len() == 0 && !l.isMeta {
			return actor.WorkerContinue
		}
		if time.Since(l.lastSend) > l.cfg.FlushInterval && l.items.Len() > 0 {
			l.trySend(l.items.SliceAndReset(), ctx)
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
func (l *loop[T]) trySend(series []T, ctx context.Context) {

	attempts := 0
	// Ensure we return any items back to the pools they belong to.
	defer func() {
		for _, s := range series {
			s.Free()
		}
	}()
	defer l.sendingCleanup()
	for {
		start := time.Now()
		result := l.send(series, ctx, attempts)
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

func (l *loop[T]) sendingCleanup() {
	l.sendBuffer = l.sendBuffer[:0]
	l.lastSend = time.Now()
}

// send is the main work loop of the loop.
func (l *loop[T]) send(series []T, ctx context.Context, retryCount int) sendResult {
	result := sendResult{}
	defer func() {
		recordStats(series, l.isMeta, l.statsFunc, result, len(l.sendBuffer))
	}()
	// Check to see if this is a retry and we can reuse the buffer.
	// I wonder if we should do this, its possible we are sending things that have exceeded the TTL.
	if len(l.sendBuffer) == 0 {
		data, wrErr := generateWriteRequest[T](series)
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
