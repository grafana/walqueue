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
	// stage is used to simulate time or changes to parrallelism.
	type stage struct {
		// desired is what the desired value should be for a stage
		desired uint
		// noChange is used when a stage should ensure that no changes occur.
		noChange bool
		// increaseTimeStamp is how much to increase the timestamp.
		// It will add it to previous stages timestamp.
		increaseTimeStamp int
		// failurePercentile should be between 0 and 100.
		failurePercentile int
		// waitFor is used when a stage should do nothing but pass time, generally to clear network success/error values.
		waitFor time.Duration
	}

	type test struct {
		name   string
		stages []stage
		cfg    types.ParallelismConfig
	}

	tests := []test{
		{
			name: "no changes",
			stages: []stage{
				{
					noChange: true,
				},
			},
		},
		{
			name: "increase",
			cfg: types.ParallelismConfig{
				MaxConnections: 4,
			},
			stages: []stage{
				{
					desired:           3,
					increaseTimeStamp: 100,
				},
			},
		},
		{
			name: "decrease with minimum",
			cfg: types.ParallelismConfig{
				MaxConnections:        4,
				AllowedDrift:          10 * time.Second,
				MinimumScaleDownDrift: 5 * time.Second,
			},
			stages: []stage{
				{
					desired:           3,
					increaseTimeStamp: 100,
				},
				{
					// This small timestamp should trigger a lower value.
					desired:           2,
					increaseTimeStamp: 1,
				},
			},
		},
		{
			name: "network hard down",
			cfg: types.ParallelismConfig{
				MaxConnections:              4,
				AllowedDrift:                10 * time.Second,
				MinimumScaleDownDrift:       5 * time.Second,
				AllowedNetworkErrorFraction: 0.89,
				ResetInterval:               5 * time.Second,
			},
			stages: []stage{
				{
					// This will bump it up.
					desired:           3,
					increaseTimeStamp: 100,
				},
				{
					// Everything will fail, even though the timestamp is legit.
					// We fail .90 and our threshold is 0.89.
					failurePercentile: 90,
					desired:           2,
					increaseTimeStamp: 100,
				},
			},
		},
		{
			name: "network not hard down",
			cfg: types.ParallelismConfig{
				MaxConnections:              4,
				AllowedDrift:                10 * time.Second,
				MinimumScaleDownDrift:       5 * time.Second,
				AllowedNetworkErrorFraction: 0.90,
			},
			stages: []stage{
				{
					// This will bump it up.
					desired:           3,
					increaseTimeStamp: 100,
				},
				{
					// Just barely above the threshhold to not trigger the decrease but increase will trigger.
					failurePercentile: 89,
					desired:           4,
					increaseTimeStamp: 100,
				},
			},
		},
		{
			name: "network was down but errors fall off",
			cfg: types.ParallelismConfig{
				MaxConnections:              4,
				AllowedDrift:                10 * time.Second,
				MinimumScaleDownDrift:       5 * time.Second,
				AllowedNetworkErrorFraction: 0.89,
				ResetInterval:               1 * time.Second,
			},
			stages: []stage{
				{
					// This will bump it up.
					desired:           3,
					increaseTimeStamp: 100,
				},
				{
					// Everything will fail, even though the timestamp is legit.
					// We fail .90 and our threshold is 0.89.
					failurePercentile: 90,
					desired:           2,
					increaseTimeStamp: 100,
				},
				{
					// but I got better, this gives time for the reset interval to clear out the errors.
					waitFor: 2 * time.Second,
				},
				{
					// Lets get back to normal.
					desired:           1,
					increaseTimeStamp: 100,
				},
			},
		},
		{
			name: "lookback",
			cfg: types.ParallelismConfig{
				MaxConnections:              4,
				AllowedDrift:                10 * time.Second,
				MinimumScaleDownDrift:       5 * time.Second,
				AllowedNetworkErrorFraction: 0.89,
				ResetInterval:               1 * time.Second,
				Lookback:                    5 * time.Second,
			},
			stages: []stage{
				{
					// This will bump it up.
					desired:           3,
					increaseTimeStamp: 100,
				},
				{
					// Normally this would wind down due to the minimum scale down triggers,
					// but since we have lookback enabled then it will see the previous desired and keep it going.
					noChange:          true,
					increaseTimeStamp: 1,
				},
			},
		},
		{
			name: "lookback interval",
			cfg: types.ParallelismConfig{
				MaxConnections:              4,
				AllowedDrift:                10 * time.Second,
				MinimumScaleDownDrift:       5 * time.Second,
				AllowedNetworkErrorFraction: 0.89,
				ResetInterval:               1 * time.Second,
				Lookback:                    5 * time.Second,
			},
			stages: []stage{
				{
					// This will bump it up.
					desired:           3,
					increaseTimeStamp: 100,
				},
				{
					// Normally this would wind down due to the minimum scale down triggers,
					// but since we have lookback enabled then it will see the previous desired and keep it going.
					noChange:          true,
					increaseTimeStamp: 1,
				},
				{
					// This should cause the lookback to get removed.
					waitFor: 6 * time.Second,
				},
				{
					// Now that all the lookbacks are removed we can scale down again.
					desired:           2,
					increaseTimeStamp: 1,
				},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			out := types.NewMailbox[uint]()
			ctx, cncl := context.WithTimeout(context.Background(), 10*time.Second)
			defer cncl()
			cfg := types.ParallelismConfig{
				AllowedDrift:                max(tc.cfg.AllowedDrift, 1*time.Second),
				MaxConnections:              max(tc.cfg.MaxConnections, 1),
				MinConnections:              max(tc.cfg.MinConnections, 1),
				ResetInterval:               min(tc.cfg.ResetInterval, 1*time.Minute),
				Lookback:                    max(tc.cfg.Lookback, 0),
				CheckInterval:               100 * time.Millisecond,
				AllowedNetworkErrorFraction: max(tc.cfg.AllowedNetworkErrorFraction, 0),
				MinimumScaleDownDrift:       max(tc.cfg.MinimumScaleDownDrift, 1*time.Second),
			}

			fs := &parStats{}
			l := log.NewLogfmtLogger(os.Stdout)
			p := newParallelism(cfg, out, fs, l)
			ts := 1
			p.Run(ctx)

			for i, st := range tc.stages {
				println("stage ", i)
				nextTS := ts + st.increaseTimeStamp
				// If failurePercentile is greater 0 then write some failed records.
				if st.failurePercentile > 0 {
					successes := 100 - st.failurePercentile
					for sIndex := 0; sIndex < successes; sIndex++ {
						fs.SendSeriesNetworkStats(types.NetworkStats{
							Series: types.CategoryStats{SeriesSent: 1},
						})
					}
					failures := 100 - successes
					for sIndex := 0; sIndex < failures; sIndex++ {
						fs.SendSeriesNetworkStats(types.NetworkStats{
							Series: types.CategoryStats{FailedSamples: 1},
						})
					}

				}
				// Serializer should always be newer than network.
				fs.SendSerializerStats(types.SerializerStats{
					NewestTimestampSeconds: int64(nextTS),
				})
				fs.SendSeriesNetworkStats(types.NetworkStats{
					NewestTimestampSeconds: int64(ts),
				})

				// set our starting to next + 1
				ts = nextTS + 1

				// If we want to sleep then thats all the stage does.
				if st.waitFor > 0 {
					time.Sleep(st.waitFor)
					continue
				}

				// changes are only sent if they are different, in this we are checking that no requests come in.s
				if st.noChange {
					select {
					case <-out.ReceiveC():
						require.Fail(t, "should not receive any changes")
					case <-time.After(1 * time.Second):
						require.True(t, true)
					}
					continue
				}
				// Check for the desired state.
				select {
				case desired := <-out.ReceiveC():
					require.True(t, desired == st.desired)
					continue
				case <-time.After(1 * time.Second):
					require.Failf(t, "should have gotten desired ", "%d", st.desired)
					return
				}

			}
		})
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
