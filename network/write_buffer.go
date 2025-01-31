package network

import (
	"context"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/golang/snappy"
	"github.com/grafana/walqueue/types"
	"go.uber.org/atomic"
)

// writeBuffer handles buffering the data, keeping track if there is a write request already running and kicking off the
// write request as needed. All methods need to be called in a thread safe manner.
type writeBuffer[T types.Datum] struct {
	items []T
	// writeAvailable keeps track if there is a write request going out.
	writeAvailable *atomic.Bool
	wrBuf          []byte
	snappyBuf      []byte
	log            log.Logger
	cfg            types.ConnectionConfig
	stats          func(stats types.NetworkStats)
	isMeta         bool
}

func newWriteBuffer[T types.Datum](cfg types.ConnectionConfig, stats func(networkStats types.NetworkStats), isMeta bool, l log.Logger) *writeBuffer[T] {
	return &writeBuffer[T]{
		items:          make([]T, 0),
		writeAvailable: atomic.NewBool(true),
		cfg:            cfg,
		stats:          stats,
		isMeta:         isMeta,
		log:            l,
	}
}

// ForceAdd is only used when we need to force items to the queue, this is generally done as part of a config change.
func (w *writeBuffer[T]) ForceAdd(item T) {
	w.items = append(w.items, item)
}

func (w *writeBuffer[T]) Add(ctx context.Context, item T) bool {
	// Check if adding to the buffer would put us over the batch count.
	// Having a batch count batch*2+1 means we can have more data read to go.
	if len(w.items)+1 > (w.cfg.BatchCount*2 + 1) {
		// Try and send to see if we can. If there is already an outgoing request this
		// does nothing. If not triggers a new one. We wont add the new record but this will attempt to kick
		// off a request.
		w.Send(ctx)
		return false
	}
	w.items = append(w.items, item)
	// Buffer has reached a valid size.
	if len(w.items) >= w.cfg.BatchCount {
		w.Send(ctx)
	}
	return true
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
	// Nothing to send so noop
	if len(w.items) == 0 {
		return
	}
	// Write Available tells us if there is a write client available, if true then we can right.
	if w.writeAvailable.Load() {
		sendingItems := w.getItems()
		// About to kick off a write request so the write is no longer available.
		w.writeAvailable.Store(false)
		go func() {
			s := newSignalsInfo[T](sendingItems)
			var err error
			w.snappyBuf, w.wrBuf, err = buildWriteRequest[T](sendingItems, w.snappyBuf, w.wrBuf)
			// If the write request fails then we should pretend it worked.
			if err != nil {
				level.Error(w.log).Log("msg", "error building write request", "err", err)
				return
			}
			w.send(w.snappyBuf, s, ctx)
			// Things are sent open back up for writes.
			w.writeAvailable.Store(true)
		}()
	}
}

func (w *writeBuffer[T]) send(bb []byte, s signalsInfo, ctx context.Context) {
	stats := func(r sendResult) {
		recordStats(s.seriesCount, s.histogramCount, s.metadataCount, s.newestTS, w.isMeta, w.stats, r, len(bb))
	}
	l, nlErr := newWrite(w.cfg, w.log, stats)
	if nlErr != nil {
		level.Error(w.log).Log("msg", "error creating write", "err", nlErr)
		return
	}
	l.trySend(bb, ctx)
}

// getItems will batch up to BatchCount items and return them, then truncate the internal items array.
func (w *writeBuffer[T]) getItems() []T {
	numberToSend := len(w.items)
	if len(w.items) > w.cfg.BatchCount {
		numberToSend = w.cfg.BatchCount
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

// singalsInfo allows us to preallocate what type of signals and count, since once they are
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
