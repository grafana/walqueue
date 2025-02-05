package network

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/walqueue/types"
	"github.com/stretchr/testify/require"
)

func TestParallelismWithNoChanges(t *testing.T) {
	out := make(chan uint)
	ctx, cncl := context.WithTimeout(context.Background(), 10*time.Second)
	defer cncl()
	cfg := types.ParralelismConfig{
		AllowedDriftSeconds:        1,
		MaxConnections:             1,
		MinConnections:             1,
		ResetInterval:              1 * time.Minute,
		Lookback:                   1 * time.Minute,
		CheckInterval:              1 * time.Second,
		AllowedNetworkErrorPercent: 0,
	}
	fs := &fauxstats{}

	l := log.NewLogfmtLogger(os.Stdout)
	p := newParallelism(cfg, out, fs, l)
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
	cfg := types.ParralelismConfig{
		AllowedDriftSeconds:        1,
		MaxConnections:             2,
		MinConnections:             1,
		ResetInterval:              1 * time.Second,
		Lookback:                   1 * time.Minute,
		CheckInterval:              1 * time.Second,
		AllowedNetworkErrorPercent: 0,
	}
	fs := &fauxstats{}

	l := log.NewLogfmtLogger(os.Stdout)
	p := newParallelism(cfg, out, fs, l)
	go p.Run(ctx)
	// This will create a difference of 100 seconds
	fs.SendSerializerStats(types.SerializerStats{
		NewestTimestampSeconds: 100,
	})
	fs.SendSeriesNetworkStats(types.NetworkStats{
		NewestTimestampSeconds: 1,
	})

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
	cfg := types.ParralelismConfig{
		AllowedDriftSeconds:        1,
		MaxConnections:             2,
		MinConnections:             1,
		ResetInterval:              1 * time.Second,
		Lookback:                   1 * time.Second,
		CheckInterval:              1 * time.Second,
		AllowedNetworkErrorPercent: 0,
	}
	fs := &fauxstats{}
	l := log.NewLogfmtLogger(os.Stdout)

	p := newParallelism(cfg, out, fs, l)
	go p.Run(ctx)
	// This will create a difference of 99 seconds
	fs.SendSerializerStats(types.SerializerStats{
		NewestTimestampSeconds: 100,
	})
	fs.SendSeriesNetworkStats(types.NetworkStats{
		NewestTimestampSeconds: 1,
	})

	select {
	case desired := <-out:
		require.True(t, desired == 2)
	case <-ctx.Done():
		require.Fail(t, "should have gotten desired 2")
		return
	}
	fs.SendSerializerStats(types.SerializerStats{
		NewestTimestampSeconds: 300,
	})
	fs.SendSeriesNetworkStats(types.NetworkStats{
		NewestTimestampSeconds: 300,
	})

	select {
	case desired := <-out:
		require.True(t, desired == 1)
	case <-ctx.Done():
		require.Fail(t, "should have gotten desired 1")
		return
	}
}

var _ types.StatsHub = (*fauxstats)(nil)

type fauxstats struct {
	network func(types.NetworkStats)
	serial  func(types.SerializerStats)
}

func (f fauxstats) SendParralelismStats(stats types.ParralelismStats) {

}

func (f fauxstats) RegisterParralelism(f2 func(types.ParralelismStats)) types.NotificationRelease {
	return func() {

	}
}

func (fauxstats) Start(_ context.Context) {
}

func (fauxstats) Stop() {
}

func (f *fauxstats) SendSeriesNetworkStats(ns types.NetworkStats) {
	f.network(ns)
}

func (f *fauxstats) SendSerializerStats(ss types.SerializerStats) {
	f.serial(ss)
}

func (fauxstats) SendMetadataNetworkStats(_ types.NetworkStats) {
}

func (f *fauxstats) RegisterSeriesNetwork(fn func(types.NetworkStats)) (_ types.NotificationRelease) {
	f.network = fn
	return func() {}
}

func (f *fauxstats) RegisterMetadataNetwork(fn func(types.NetworkStats)) (_ types.NotificationRelease) {
	return func() {}
}

func (f *fauxstats) RegisterSerializer(fn func(types.SerializerStats)) (_ types.NotificationRelease) {
	f.serial = fn
	return func() {}
}
