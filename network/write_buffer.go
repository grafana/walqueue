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
	"github.com/panjf2000/ants/v2"
)

// writeBuffer handles buffering the data, keeping track if there is a write request already running and kicking off the
// write request as needed. All methods need to be called in a thread safe manner.
type writeBuffer[T types.Datum] struct {
	// mut I hate this mutex, but the only called should be on when it needs to drain which should not have any contention.
	mut               sync.RWMutex
	id                int
	items             []T
	wrBuf             []byte
	snappyBuf         []byte
	log               log.Logger
	cfg               types.ConnectionConfig
	stats             func(stats types.NetworkStats)
	isMeta            bool
	routinePool       *ants.Pool
	client            *http.Client
	lastAttemptedSend time.Time
	receive           chan []T
	moreSignals       chan types.RequestMoreSignals[T]
	requestBuffer     []T
	stop              chan struct{}
}

func newWriteBuffer[T types.Datum](id int, cfg types.ConnectionConfig, stats func(networkStats types.NetworkStats), isMeta bool, l log.Logger, pool *ants.Pool, client *http.Client, moreSignals chan types.RequestMoreSignals[T]) *writeBuffer[T] {
	return &writeBuffer[T]{
		id:                id,
		items:             make([]T, 0),
		cfg:               cfg,
		stats:             stats,
		isMeta:            isMeta,
		log:               l,
		routinePool:       pool,
		client:            client,
		lastAttemptedSend: time.Time{},
		receive:           make(chan []T),
		moreSignals:       moreSignals,
		requestBuffer:     make([]T, cfg.BatchCount),
		stop:              make(chan struct{}),
	}
}

// ForceAdd is only used when we need to force items to the queue, this is generally done as part of a config change.
func (w *writeBuffer[T]) ForceAdd(ctx context.Context, item T) {
	w.mut.Lock()
	defer w.mut.Unlock()

	w.items = append(w.items, item)
}

// Run starts the buffer processing loop.
func (w *writeBuffer[T]) Run(ctx context.Context) {
	go func() {
		// The ticker makes this slightly awkward, basically if we have an outstanding ask for more signals
		// we dont want to queue another until we have received the most recent one.
		askingForMore := false
		ticker := time.NewTicker(1 * time.Second)

		for {
			if !askingForMore && len(w.items) < w.cfg.BatchCount {
				// Signal that we are ready for more data
				w.moreSignals <- types.RequestMoreSignals[T]{
					ID:       w.id,
					MaxCount: w.cfg.BatchCount - len(w.items),
					Response: w.receive,
					Buffer:   w.requestBuffer[:w.cfg.BatchCount-len(w.items)],
				}
				askingForMore = true
			}

			select {
			case <-ctx.Done():
				return
			case items := <-w.receive:
				w.mut.Lock()
				w.items = append(w.items, items...)
				if len(w.items) >= w.cfg.BatchCount {
					w.attemptSend(ctx)
				}
				askingForMore = false
				w.requestBuffer = w.requestBuffer[:0]
				w.mut.Unlock()
			case <-ticker.C:
				if time.Since(w.lastAttemptedSend) > w.cfg.FlushInterval {
					w.mut.Lock()
					w.attemptSend(ctx)
					w.mut.Unlock()
				}

			case <-w.stop:
				return
			}
		}
	}()
}

// Drain returns any remaining items and sets the internal item array to 0 items.
func (w *writeBuffer[T]) Drain() []T {
	w.mut.Lock()
	defer w.mut.Unlock()

	items := make([]T, len(w.items))
	copy(items, w.items)
	// We could likely nil this out since this is only called when its being turned off
	// This is safer though.
	w.items = make([]T, 0)
	return items
}

func (w *writeBuffer[T]) Stop() {
	w.stop <- struct{}{}
}

func (w *writeBuffer[T]) attemptSend(ctx context.Context) {
	defer func() {
		w.lastAttemptedSend = time.Now()
	}()

	// Write in progress tells us if there is a write client in progress, if false then we can right.
	sendingItems := w.getItems()
	if len(sendingItems) == 0 {
		return
	}
	// This will block until a worker frees up.

	w.routinePool.Submit(func() {
		s := newSignalsInfo[T](sendingItems)
		var err error
		w.snappyBuf, w.wrBuf, err = buildWriteRequest[T](sendingItems, w.snappyBuf, w.wrBuf)
		// If the build write request fails then we should pretend it worked. Since this should only trigger if
		// we get invalid datums.
		if err != nil {
			level.Error(w.log).Log("msg", "error building write request", "err", err)
			return
		}
		w.send(w.snappyBuf, s, ctx)
	})
}

func (w *writeBuffer[T]) send(bb []byte, s signalsInfo, ctx context.Context) {
	bbLen := len(bb)
	stats := func(r sendResult) {
		recordStats(s.seriesCount, s.histogramCount, s.metadataCount, s.newestTS, w.isMeta, w.stats, r, bbLen)
	}
	l, nlErr := newWrite(w.cfg, w.log, stats, w.client)
	if nlErr != nil {
		level.Error(w.log).Log("msg", "error creating write", "err", nlErr)
		return
	}
	l.trySend(bb, ctx)
}

// getItems will batch up to BatchCount items and return them, then truncate the internal items array.
func (w *writeBuffer[T]) getItems() []T {
	// Always use the exact batch count from config when possible
	numberToSend := w.cfg.BatchCount
	if len(w.items) < w.cfg.BatchCount {
		numberToSend = len(w.items)
	}
	sendingItems := w.items[:numberToSend]
	w.items = w.items[numberToSend:]
	return sendingItems
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
	data, err := generateWriteRequest[T](items, protobuf)
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
		mm, valid := interface{}(ts).(types.MetricDatum)
		if !valid {
			continue
		}
		if mm.TimeStampMS() > s.newestTS {
			s.newestTS = mm.TimeStampMS()
		}
	}
	return s
}
