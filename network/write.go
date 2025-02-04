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
	"github.com/grafana/walqueue/types"
	"github.com/prometheus/common/config"
)

// write is a fire and forget client.
type write struct {
	isMeta bool
	client *http.Client
	cfg    types.ConnectionConfig
	log    log.Logger
	stats  func(r sendResult)
}

func newWrite(cc types.ConnectionConfig, l log.Logger, statsResult func(r sendResult)) (*write, error) {
	var httpOpts []config.HTTPClientOption
	if cc.UseRoundRobin {
		httpOpts = []config.HTTPClientOption{config.WithDialContextFunc(newDialContextWithRoundRobinDNS().dialContextFn())}
	}

	cfg := cc.ToPrometheusConfig()
	httpClient, err := config.NewClientFromConfig(cfg, "remote_write", httpOpts...)

	if err != nil {
		return nil, err
	}
	return &write{
		client: httpClient,
		cfg:    cc,
		log:    log.With(l, "name", "loop", "url", cc.URL),
		stats:  statsResult,
	}, nil
}

// trySend is the core functionality for sending data to an endpoint. It will attempt retries as defined in MaxRetryAttempts.
func (l *write) trySend(buf []byte, ctx context.Context) {
	attempts := 0
	for {
		result := l.send(buf, ctx, attempts)

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
	duration         time.Duration
}

// send is the main work loop of the loop.
func (l *write) send(buf []byte, ctx context.Context, retryCount int) sendResult {
	start := time.Now()
	result := sendResult{}
	defer func() {
		result.duration = time.Since(start)
		l.stats(result)
	}()
	httpReq, err := http.NewRequest("POST", l.cfg.URL, bytes.NewReader(buf))
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
