package types

import (
	gocontext "context"

	"golang.design/x/chann"
)

// SyncMailbox is used to synchronously send data, and wait for it to process before returning.
type SyncMailbox[T, R any] struct {
	mbx *chann.Chann[*Callback[T, R]]
}

func NewSyncMailbox[T, R any](opts ...chann.Opt) *SyncMailbox[T, R] {
	return &SyncMailbox[T, R]{
		mbx: chann.New[*Callback[T, R]](opts...),
	}
}

func (mb *SyncMailbox[T, R]) Close() {
	mb.mbx.Close()
}

func (sm *SyncMailbox[T, R]) Out() <-chan *Callback[T, R] {
	return sm.mbx.Out()
}

func (sm *SyncMailbox[T, R]) In(ctx gocontext.Context, value T) (R, error) {
	done := make(chan callbackResult[R], 1)
	defer close(done)
	sm.mbx.In() <- &Callback[T, R]{
		Value: value,
		done:  done,
	}
	select {
	case <-ctx.Done():
		return Zero[R](), ctx.Err()
	case result := <-done:
		return result.response, result.err
	}
}

type CallbackHook[R any] interface {
	Notify(response R, err error)
}

type Callback[T any, R any] struct {
	Value    T
	Response R
	done     chan callbackResult[R]
}

type callbackResult[R any] struct {
	err      error
	response R
}

// Notify must be called to return the synchronous call.
func (c *Callback[T, R]) Notify(response R, err error) {
	c.done <- callbackResult[R]{
		err:      err,
		response: response,
	}
}

func Zero[T any]() T {
	var zero T
	return zero
}
