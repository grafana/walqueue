package types

import (
	gocontext "context"
	"golang.design/x/chann"
)

// SyncMailbox is used to synchronously send data, and wait for it to process before returning.
type SyncMailbox[T, R any] struct {
	mbx *Mailbox[*Callback[T, R]]
}

func NewSyncMailbox[T, R any](opt ...chann.Opt) *SyncMailbox[T, R] {
	return &SyncMailbox[T, R]{
		mbx: NewMailbox[*Callback[T, R]](opt...),
	}
}

func (sm *SyncMailbox[T, R]) Start() {
}

func (sm *SyncMailbox[T, R]) ReceiveC() <-chan *Callback[T, R] {
	return sm.mbx.ReceiveC()
}

func (sm *SyncMailbox[T, R]) Send(ctx gocontext.Context, value T) (R, error) {
	done := make(chan callbackResult[R], 1)
	defer close(done)
	err := sm.mbx.Send(ctx, &Callback[T, R]{
		Value: value,
		done:  done,
	})
	if err != nil {
		return Zero[R](), err
	}
	result := <-done
	return result.response, result.err
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
