package network

import (
	"context"
	"github.com/golang/snappy"
	"github.com/grafana/walqueue/types"
	"go.uber.org/atomic"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
)

type loopBuffer[T types.Datum] struct {
	l             *write
	buffer        []T
	batchSize     int
	loopAvailable *atomic.Bool
	wrBuf         []byte
	snappyBuf     []byte
	log           log.Logger
}

func newLoopBuffer[T types.Datum](l *write, batchSize int) *loopBuffer[T] {
	return &loopBuffer[T]{
		l:             l,
		buffer:        make([]T, 0),
		batchSize:     batchSize,
		loopAvailable: atomic.NewBool(true),
	}
}

func (b *loopBuffer[T]) Add(ctx context.Context, item T) bool {
	b.buffer = append(b.buffer, item)
	// Buffer hasn't reached size yet.
	if len(b.buffer) < b.batchSize {
		return true
	}
	if b.loopAvailable.Load() {
		bb, err := b.buildWriteRequest()
		// If the write request fails then we should pretend it worked.
		if err != nil {
			level.Error(b.log).Log("msg", "error building write request", "err", err)
			return true
		}
		b.loopAvailable.Store(false)
		go func() {
			b.l.trySend(bb, ctx)
			b.loopAvailable.Store(true)
		}()
		return true
	}
	return false
}

func (b *loopBuffer[T]) Send(ctx context.Context) bool {
	if b.loopAvailable.Load() {
		bb, err := b.buildWriteRequest()
		// If the write request fails then we should pretend it worked.
		if err != nil {
			level.Error(b.log).Log("msg", "error building write request", "err", err)
			return true
		}
		b.loopAvailable.Store(false)
		go func() {
			b.l.trySend(bb, ctx)
			b.loopAvailable.Store(true)
		}()
		return true
	}
	return false
}

func (b *loopBuffer[T]) buildWriteRequest() ([]byte, error) {
	data, err := generateWriteRequest[T](b.buffer, b.wrBuf)
	if err != nil {
		return nil, err
	}
	b.snappyBuf = snappy.Encode(b.snappyBuf, data)
	return b.snappyBuf, nil
}
