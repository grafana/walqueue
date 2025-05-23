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
)

// write is a fire and forget client.
type write struct {
	log    log.Logger
	client *http.Client
	stats  func(r sendResult)
	cfg    types.ConnectionConfig
}

//nolint:unparam // TODO error is always nil, but this should do cfg validation
func newWrite(cc types.ConnectionConfig, l log.Logger, statsResult func(r sendResult), client *http.Client) (*write, error) {
	return &write{
		client: client,
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
		select {
		case <-ctx.Done():
			return
		case <-time.After(result.retryAfter):
			continue
		}
	}
}

type sendResult struct {
	err              error
	retryAfter       time.Duration
	statusCode       int
	duration         time.Duration
	successful       bool
	recoverableError bool
	networkError     bool
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
	// Add custom headers from configuration first
	for key, value := range l.cfg.Headers {
		httpReq.Header.Set(key, value)
	}

	// Add/Set required headers, which will override custom headers with the same name
	httpReq.Header.Add("Content-Encoding", "snappy")
	httpReq.Header.Set("Content-Type", "application/x-protobuf")
	httpReq.Header.Set("User-Agent", l.cfg.UserAgent)
	httpReq.Header.Set("X-Prometheus-Remote-Write-Version", "0.1.0")

	if l.cfg.BasicAuth != nil {
		httpReq.SetBasicAuth(l.cfg.BasicAuth.Username, l.cfg.BasicAuth.Password)
	} else if l.cfg.BearerToken != "" {
		httpReq.Header.Set("Authorization", "Bearer "+l.cfg.BearerToken)
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
	defer resp.Body.Close()

	result.statusCode = resp.StatusCode
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
