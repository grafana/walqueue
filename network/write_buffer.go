package network

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/golang/snappy"
	"github.com/grafana/walqueue/types"
)

// writeBuffer handles buffering the data, keeping track if there is a write request already running and kicking off the
// write request as needed. All methods need to be called in a thread safe manner.
type writeBuffer[T types.Datum] struct {
	mut               sync.RWMutex
	id                int
	items             []T
	wrBuf             []byte
	snappyBuf         []byte
	log               log.Logger
	cfg               types.ConnectionConfig
	stats             func(stats types.NetworkStats)
	lastAttemptedSend time.Time
	requestBuffer     []T
	currentlySending  bool
}

func newWriteBuffer[T types.Datum](id int, cfg types.ConnectionConfig, stats func(networkStats types.NetworkStats), l log.Logger) *writeBuffer[T] {
	return &writeBuffer[T]{
		id:    id,
		items: make([]T, 0, cfg.BatchCount),
		cfg:   cfg,
		stats: stats,
		log:   l,
	}
}

func (w *writeBuffer[T]) Add(items []T) {
	w.mut.Lock()
	defer w.mut.Unlock()

	w.items = append(w.items, items...)
}

func (w *writeBuffer[T]) RemainingCapacity() int {
	w.mut.RLock()
	defer w.mut.RUnlock()

	return w.cfg.BatchCount - len(w.items)
}

func (w *writeBuffer[T]) Len() int {
	w.mut.RLock()
	defer w.mut.RUnlock()

	return len(w.items)
}

func (w *writeBuffer[T]) IsSending() bool {
	w.mut.RLock()
	defer w.mut.RUnlock()

	return w.currentlySending
}

func (w *writeBuffer[T]) LastAttemptedSend() time.Time {
	w.mut.RLock()
	defer w.mut.RUnlock()

	return w.lastAttemptedSend
}

// Drain returns any remaining items and sets the internal item array to 0 items.
func (w *writeBuffer[T]) Drain() []T {
	w.mut.Lock()
	defer w.mut.Unlock()

	defer func() {
		w.items = w.items[:0]
	}()

	return w.items
}

func (w *writeBuffer[T]) Send(ctx context.Context, client *http.Client, finish func()) {
	w.mut.Lock()
	defer w.mut.Unlock()

	defer func() {
		w.lastAttemptedSend = time.Now()
	}()

	if len(w.items) == 0 {
		return
	}

	s := newSignalsInfo(w.items)
	var err error
	w.snappyBuf, w.wrBuf, err = buildWriteRequest(w.items, w.snappyBuf, w.wrBuf)
	// If the build write request fails then we should still clear out the items. Since this should only trigger if
	// we get invalid item, and there is no resolution to that.

	w.items = w.items[:0]
	if err != nil {
		level.Error(w.log).Log("msg", "error building write request", "err", err)
		return
	}

	w.currentlySending = true
	go func() {
		// Regardless of what happens we need to clear out the items.
		// This will allow new items to be added that will then allow more sending.
		defer func() {
			w.mut.Lock()
			w.currentlySending = false
			w.mut.Unlock()
			finish()
		}()
		isMeta := false
		if _, ok := any(w.items).([]types.MetadataDatum); ok {
			isMeta = true
		}
		send(isMeta, w.cfg, w.log, w.snappyBuf, s, ctx, client, w.stats)
	}()
}

func send(isMeta bool, cfg types.ConnectionConfig, l log.Logger, bb []byte, s signalsInfo, ctx context.Context, client *http.Client, parentstats func(stats types.NetworkStats)) {
	bbLen := len(bb)
	stats := func(r sendResult) {
		recordStats(s.seriesCount, s.histogramCount, s.metadataCount, s.newestTS, isMeta, parentstats, r, bbLen)
	}
	nw, nlErr := newWrite(cfg, l, stats, client)
	if nlErr != nil {
		level.Error(l).Log("msg", "error creating write", "err", nlErr)
		return
	}
	nw.trySend(bb, ctx)
}

// buildWriteRequest takes returns the snappy encoded final buffer followed by the protobuf. Note even in error it returns the buffers
// for reuse.
func buildWriteRequest[T types.Datum](items []T, snappybuf []byte, protobuf []byte) ([]byte, []byte, error) {
	defer func() {
		for _, item := range items {
			item.Free()
		}
	}()
	if snappybuf == nil {
		snappybuf = make([]byte, 0)
	}
	if protobuf == nil {
		protobuf = make([]byte, 0)
	}
	data, err := generateWriteRequest(items, protobuf)
	if err != nil {
		return protobuf, snappybuf, err
	}
	snappybuf = snappy.Encode(snappybuf, data)
	return snappybuf, protobuf, nil
}

// signalsInfo allows us to preallocate what type of signals and count, since once they are
// serialized that information is lost.
type signalsInfo struct {
	seriesCount    int
	histogramCount int
	metadataCount  int
	newestTS       int64
}

func newSignalsInfo[T types.Datum](signals []T) signalsInfo {
	s := signalsInfo{}
	s.seriesCount = getSeriesCount(signals)
	s.histogramCount = getHistogramCount(signals)
	s.metadataCount = getMetaDataCount(signals)
	for _, ts := range signals {
		mm, valid := any(ts).(types.MetricDatum)
		if !valid {
			continue
		}
		if mm.TimeStampMS() > s.newestTS {
			s.newestTS = mm.TimeStampMS()
		}
	}
	return s
}
