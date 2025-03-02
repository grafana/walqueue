package types

import (
	"context"

	"golang.design/x/chann"
)

// Mailbox is a backwards compatible implemention of the actor mailbox.
// It uses chann underneath the hood that mimics the behavior of actor mailboxes.
type Mailbox[T any] struct {
	ch *chann.Chann[T]
}

func NewMailbox[T any](opt ...chann.Opt) *Mailbox[T] {
	return &Mailbox[T]{
		ch: chann.New[T](opt...),
	}
}

func (m *Mailbox[T]) ReceiveC() <-chan T {
	return m.ch.Out()
}

func (m *Mailbox[T]) Send(ctx context.Context, v T) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case m.ch.In() <- v:
		return nil
	}
}

type RequestMoreSignals[T Datum] struct {
	ID       int
	MaxCount int
	Response chan []T
}
