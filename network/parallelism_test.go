package network

import (
	"context"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestParallelismWithNoChanges(t *testing.T) {
	out := make(chan uint)
	ctx, cncl := context.WithTimeout(context.Background(), 20*time.Second)
	defer cncl()
	p := newParallelism(1*time.Minute, 1, 1, 1, out)
	go p.Run(ctx)
	select {
	case <-out:
		require.Fail(t, "should not receive any changes")
	case <-ctx.Done():
		require.True(t, true)
		return
	}
}
