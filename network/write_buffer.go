package network

import (
	"context"
	"github.com/golang/snappy"
	"github.com/grafana/walqueue/types"
	"sync"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
)

type writeBuffer[T types.Datum] struct {
	mut            sync.Mutex
	items          []T
	writeAvailable bool
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
		writeAvailable: true,
		cfg:            cfg,
		stats:          stats,
		isMeta:         isMeta,
		log:            l,
	}
}

func (b *writeBuffer[T]) Add(ctx context.Context, item T) bool {
	b.mut.Lock()
	defer b.mut.Unlock()

	b.items = append(b.items, item)
	// Buffer hasn't reached size yet.
	if len(b.items) < b.cfg.BatchCount {
		return true
	}
	if b.writeAvailable {
		s := newSignalsInfo[T](b.items)
		bb, err := b.buildWriteRequest()
		// If the write request fails then we should pretend it worked.
		if err != nil {
			level.Error(b.log).Log("msg", "error building write request", "err", err)
			return true
		}
		b.writeAvailable = false
		go b.send(bb, s, ctx)
		return true
	}
	return false
}

func (b *writeBuffer[T]) Send(ctx context.Context) bool {
	b.mut.Lock()
	defer b.mut.Unlock()

	if b.writeAvailable {
		s := newSignalsInfo[T](b.items)
		bb, err := b.buildWriteRequest()
		// If the write request fails then we should pretend it worked.
		if err != nil {
			level.Error(b.log).Log("msg", "error building write request", "err", err)
			return true
		}
		b.writeAvailable = false
		go b.send(bb, s, ctx)
		return true
	}
	return false
}

func (b *writeBuffer[T]) send(bb []byte, s signalsInfo, ctx context.Context) {
	stats := func(r sendResult) {
		recordStats(s.seriesCount, s.histogramCount, s.metadataCount, s.newestTS, b.isMeta, b.stats, r, len(bb))
	}
	l, nlErr := newLoop(b.cfg, b.log, stats)
	if nlErr != nil {
		level.Error(b.log).Log("msg", "error creating loop", "err", nlErr)
		return
	}
	l.trySend(bb, ctx)
	b.mut.Lock()
	b.writeAvailable = true
	b.mut.Unlock()
}

func (b *writeBuffer[T]) buildWriteRequest() ([]byte, error) {
	data, err := generateWriteRequest[T](b.items, b.wrBuf)
	b.items = b.items[:0]
	if err != nil {
		return nil, err
	}
	b.snappyBuf = snappy.Encode(b.snappyBuf, data)
	return b.snappyBuf, nil
}

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
