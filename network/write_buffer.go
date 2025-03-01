package network

import (
	"context"
	"net/http"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/golang/snappy"
	"github.com/grafana/walqueue/types"
	"github.com/panjf2000/ants/v2"
	"go.uber.org/atomic"
)

// writeBuffer handles buffering the data, keeping track if there is a write request already running and kicking off the
// write request as needed. All methods need to be called in a thread safe manner.
type writeBuffer[T types.Datum] struct {
	items []T
	// writeInProgress keeps track if there is a write request going out.
	writeInProgress   *atomic.Bool
	wrBuf             []byte
	snappyBuf         []byte
	log               log.Logger
	cfg               types.ConnectionConfig
	stats             func(stats types.NetworkStats)
	isMeta            bool
	routinePool       *ants.Pool
	client            *http.Client
	pullFunc          func(ctx context.Context) (T, bool)
	lastAttemptedSend time.Time
}

func newWriteBuffer[T types.Datum](cfg types.ConnectionConfig, stats func(networkStats types.NetworkStats), isMeta bool, l log.Logger, pool *ants.Pool, client *http.Client) *writeBuffer[T] {
	return &writeBuffer[T]{
		items:             make([]T, 0),
		writeInProgress:   atomic.NewBool(false),
		cfg:               cfg,
		stats:             stats,
		isMeta:            isMeta,
		log:               l,
		routinePool:       pool,
		client:            client,
		lastAttemptedSend: time.Time{},
	}
}

// RegisterPullFunc registers a function that will be used to pull data
func (w *writeBuffer[T]) RegisterPullFunc(pullFunc func(ctx context.Context) (T, bool)) {
	w.pullFunc = pullFunc
}

// ForceAdd is only used when we need to force items to the queue, this is generally done as part of a config change.
func (w *writeBuffer[T]) ForceAdd(ctx context.Context, item T) {
	w.items = append(w.items, item)
	w.Send(ctx)
}

// PullAndProcess attempts to pull data from the source and process it
func (w *writeBuffer[T]) PullAndProcess(ctx context.Context) {
	// Try to get some items (up to batch size)
	for len(w.items) < (w.cfg.BatchCount) {
		// Need to check if ctx is canceled in this tight loop.
		select {
		case <-ctx.Done():
			return
		default:
		}

		item, ok := w.pullFunc(ctx)
		if !ok {
			// No more items to pull
			break
		}
		w.items = append(w.items, item)
	}

	// If we have enough items, send them
	if len(w.items) >= w.cfg.BatchCount {
		w.Send(ctx)
	}
}

// Run starts the buffer processing loop
func (w *writeBuffer[T]) Run(ctx context.Context) {
	go func() {
		for {
			w.PullAndProcess(ctx)
			if time.Since(w.lastAttemptedSend) > w.cfg.FlushInterval {
				w.Send(ctx)
			}
		}
	}()
}

// Drain returns any remaining items and sets the internal item array to 0 items.
func (w *writeBuffer[T]) Drain() []T {
	items := make([]T, len(w.items))
	copy(items, w.items)
	// We could likely nil this out since this is only called when its being turned off
	// This is safer though.
	w.items = make([]T, 0)
	return items
}

// Send is the externally safe to call send method. This method like all others is NOT thread safe.
func (w *writeBuffer[T]) Send(ctx context.Context) {
	defer func() {
		w.lastAttemptedSend = time.Now()
	}()

	// Nothing to send so noop
	if len(w.items) == 0 {
		return
	}
	// Write in progress tells us if there is a write client in progress, if false then we can right.
	if !w.writeInProgress.Load() {
		sendingItems := w.getItems()
		// About to kick off a write request so the write is no longer available.
		w.writeInProgress.Store(true)
		// This will block until a worker frees up.
		w.routinePool.Submit(func() {
			defer w.writeInProgress.Store(false)
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
