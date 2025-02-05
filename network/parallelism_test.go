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
	fs := &parStats{}

	l := log.NewLogfmtLogger(os.Stdout)
	p := newParallelism(cfg, out, fs, l)
	p.Run(ctx)
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
	fs := &parStats{}

	l := log.NewLogfmtLogger(os.Stdout)
	p := newParallelism(cfg, out, fs, l)
	p.Run(ctx)
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
		AllowedDriftSeconds:          1,
		MaxConnections:               2,
		MinConnections:               1,
		ResetInterval:                1 * time.Second,
		Lookback:                     1 * time.Second,
		CheckInterval:                1 * time.Second,
		MinimumScaleDownDriftSeconds: 1,
		AllowedNetworkErrorPercent:   0,
	}
	fs := &parStats{}
	l := log.NewLogfmtLogger(os.Stdout)

	p := newParallelism(cfg, out, fs, l)
	p.Run(ctx)
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

func TestParallelismMinimumDrift(t *testing.T) {
	out := make(chan uint)
	ctx, cncl := context.WithTimeout(context.Background(), 20*time.Second)
	defer cncl()
	cfg := types.ParralelismConfig{
		AllowedDriftSeconds:          10,
		MinimumScaleDownDriftSeconds: 5,
		MaxConnections:               2,
		MinConnections:               1,
		ResetInterval:                1 * time.Second,
		Lookback:                     1 * time.Second,
		CheckInterval:                1 * time.Second,
		AllowedNetworkErrorPercent:   0,
	}
	fs := &parStats{}
	l := log.NewLogfmtLogger(os.Stdout)

	p := newParallelism(cfg, out, fs, l)
	p.Run(ctx)
	// This will create a difference of 99 seconds
	fs.SendSerializerStats(types.SerializerStats{
		NewestTimestampSeconds: 12,
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
	// This should not trigger a scale down.
	fs.SendSerializerStats(types.SerializerStats{
		NewestTimestampSeconds: 17,
	})
	fs.SendSeriesNetworkStats(types.NetworkStats{
		NewestTimestampSeconds: 12,
	})

	select {
	case desired := <-out:
		require.Falsef(t, true, "this should not trigger got value %d", desired)
		// Allow this to trigger for 2-3 desired triggers.
	case <-time.After(3 * time.Second):
	}

	// This should trigger a scale down, since we fall below the minimum threshold.
	fs.SendSerializerStats(types.SerializerStats{
		NewestTimestampSeconds: 20,
	})
	fs.SendSeriesNetworkStats(types.NetworkStats{
		NewestTimestampSeconds: 19,
	})

	select {
	case desired := <-out:
		require.True(t, desired == 1)
	case <-ctx.Done():
		require.Fail(t, "should have gotten desired 1")
		return
	}
}

var _ types.StatsHub = (*parStats)(nil)

type parStats struct {
	network func(types.NetworkStats)
	serial  func(types.SerializerStats)
}

func (f parStats) SendParralelismStats(stats types.ParralelismStats) {

}

func (f parStats) RegisterParralelism(f2 func(types.ParralelismStats)) types.NotificationRelease {
	return func() {

	}
}

func (parStats) Start(_ context.Context) {
}

func (parStats) Stop() {
}

func (f *parStats) SendSeriesNetworkStats(ns types.NetworkStats) {
	f.network(ns)
}

func (f *parStats) SendSerializerStats(ss types.SerializerStats) {
	f.serial(ss)
}

func (parStats) SendMetadataNetworkStats(_ types.NetworkStats) {
}

func (f *parStats) RegisterSeriesNetwork(fn func(types.NetworkStats)) (_ types.NotificationRelease) {
	f.network = fn
	return func() {}
}

func (f *parStats) RegisterMetadataNetwork(fn func(types.NetworkStats)) (_ types.NotificationRelease) {
	return func() {}
}

func (f *parStats) RegisterSerializer(fn func(types.SerializerStats)) (_ types.NotificationRelease) {
	f.serial = fn
	return func() {}
}
