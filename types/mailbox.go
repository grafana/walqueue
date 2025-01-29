package types

import (
	"context"
	"errors"
	"go.uber.org/atomic"

	"golang.design/x/chann"
)

type Mailbox[T any] struct {
	closed atomic.Bool
	ch     *chann.Chann[T]
}

func NewMailbox[T any](opts ...chann.Opt) *Mailbox[T] {
	return &Mailbox[T]{
		ch: chann.New[T](opts...),
	}
}

func (m *Mailbox[T]) Send(ctx context.Context, data T) error {
	if m.closed.Load() {
		return errors.New("mailbox is closed")
	}
	select {
	case <-ctx.Done():
		return errors.New("send cancelled")
	case m.ch.In() <- data:
		return nil
	}
}

func (m *Mailbox[T]) Receive() <-chan T {
	return m.ch.Out()
}

func (m *Mailbox[T]) AproxLen() int {
	return m.ch.Len()
}

func (m *Mailbox[T]) Close() {
	m.closed.Store(true)
	m.ch.Close()
}
