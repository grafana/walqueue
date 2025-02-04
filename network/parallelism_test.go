package network

import (
	"context"
	"github.com/grafana/walqueue/types"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestParallelismWithNoChanges(t *testing.T) {
	out := make(chan uint)
	ctx, cncl := context.WithTimeout(context.Background(), 10*time.Second)
	defer cncl()
	drift := types.NewMailbox[uint]()
	defer drift.Close()
	p := newParallelism(1*time.Minute, 1*time.Minute, 1*time.Second, 1, 1, 1, out, drift)
	go p.Run(ctx)
	select {
	case <-out:
		require.Fail(t, "should not receive any changes")
	case <-ctx.Done():
		require.True(t, true)
		return
	}
}

func TestParallelismIncrease(t *testing.T) {
	out := make(chan uint)
	ctx, cncl := context.WithTimeout(context.Background(), 10*time.Second)
	defer cncl()
	drift := types.NewMailbox[uint]()
	defer drift.Close()
	p := newParallelism(1*time.Minute, 1*time.Minute, 1*time.Second, 1, 2, 1, out, drift)
	go p.Run(ctx)
	_ = drift.Send(context.Background(), 100)
	select {
	case desired := <-out:
		require.True(t, desired == 2)
	case <-ctx.Done():
		require.Fail(t, "should have gotten desired 2")
		return
	}
}

func TestParallelismDecrease(t *testing.T) {
	out := make(chan uint)
	ctx, cncl := context.WithTimeout(context.Background(), 20*time.Second)
	defer cncl()
	drift := types.NewMailbox[uint]()
	defer drift.Close()
	p := newParallelism(1*time.Minute, 2*time.Second, 1*time.Second, 60, 2, 1, out, drift)
	go p.Run(ctx)
	_ = drift.Send(context.Background(), 100)
	select {
	case desired := <-out:
		require.True(t, desired == 2)
	case <-ctx.Done():
		require.Fail(t, "should have gotten desired 2")
		return
	}
	// Everything is back to normal now.
	_ = drift.Send(context.Background(), 1)
	time.Sleep(3 * time.Second)
	select {
	case desired := <-out:
		require.True(t, desired == 1)
	case <-ctx.Done():
		require.Fail(t, "should have gotten desired 1")
		return
	}

}
