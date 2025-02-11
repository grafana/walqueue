package network

import (
	"context"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/walqueue/types"
)

// parallelism drives the behavior on determining what the desired number of connections should be.
type parallelism struct {
	// mut covers all items here
	mut      sync.RWMutex
	cfg      types.ParallelismConfig
	statshub types.StatsHub
	ctx      context.Context
	// networkErrors is any 4xx,5xx.
	// network* holds the time any success or error occurs.
	// This is used for the lookback so we can drop any that are older than our `cfg.Lookback`.
	networkErrors    []time.Time
	networkSuccesses []time.Time

	timestampDriftSeconds int64
	currentDesired        uint
	out                   *types.Mailbox[uint]
	stop                  chan struct{}
	// previous is the number of previous desired instances. This is to prevent flapping.
	previous                   []previousDesired
	networkRelease             types.NotificationRelease
	serializerRelease          types.NotificationRelease
	timestampNetworkSeconds    int64
	timestampSerializerSeconds int64
	l                          log.Logger
}

type previousDesired struct {
	desired  uint
	recorded time.Time
}

func newParallelism(cfg types.ParallelismConfig, out *types.Mailbox[uint], statshub types.StatsHub, l log.Logger) *parallelism {
	p := &parallelism{
		cfg:            cfg,
		statshub:       statshub,
		currentDesired: cfg.MinConnections,
		out:            out,
		stop:           make(chan struct{}),
		l:              l,
	}
	// Register the network callback so we can track network in and outs.
	p.networkRelease = p.statshub.RegisterSeriesNetwork(func(ns types.NetworkStats) {
		p.mut.Lock()
		defer p.mut.Unlock()

		// These refer to the number of series but this is generated on each send or retry.
		if ns.Total429() > 0 || ns.Total5XX() > 0 || ns.TotalFailed() > 0 || ns.TotalRetried() > 0 {
			p.networkErrors = append(p.networkErrors, time.Now())
		}
		if ns.TotalSent() > 0 {
			p.networkSuccesses = append(p.networkSuccesses, time.Now())
		}

		if ns.NewestTimestampSeconds > p.timestampNetworkSeconds {
			p.timestampNetworkSeconds = ns.NewestTimestampSeconds
		}

		// Only record drift if we have valid values for both. There is a small window
		// where we get a serializer value but have 0 for timestamp.:
		if p.timestampNetworkSeconds > 0 && p.timestampSerializerSeconds > 0 {
			p.timestampDriftSeconds = p.timestampSerializerSeconds - p.timestampNetworkSeconds
		}
	})

	// Register serializer for tracking incoming timestamp.
	p.serializerRelease = p.statshub.RegisterSerializer(func(ss types.SerializerStats) {
		p.mut.Lock()
		defer p.mut.Unlock()

		if ss.NewestTimestampSeconds > p.timestampSerializerSeconds {
			p.timestampSerializerSeconds = ss.NewestTimestampSeconds
		}
		if p.timestampNetworkSeconds > 0 && p.timestampSerializerSeconds > 0 {
			p.timestampDriftSeconds = p.timestampSerializerSeconds - p.timestampNetworkSeconds
		}
	})
	return p
}

func (p *parallelism) Stop() {
	p.serializerRelease()
	p.networkRelease()
	p.stop <- struct{}{}
}

func (p *parallelism) Run(ctx context.Context) {
	go func() {
		p.run(ctx)
	}()
}

func (p *parallelism) run(ctx context.Context) {
	p.mut.Lock()
	p.ctx = ctx
	p.statshub.SendParralelismStats(types.ParralelismStats{
		MinConnections:     p.cfg.MinConnections,
		MaxConnections:     p.cfg.MaxConnections,
		DesiredConnections: p.currentDesired,
	})
	p.mut.Unlock()
	for {
		var checkInterval time.Duration
		p.mut.RLock()
		checkInterval = p.cfg.CheckInterval
		p.mut.RUnlock()
		select {
		case <-ctx.Done():
			return
		case <-p.stop:
			return
		case <-time.After(checkInterval):
			p.desiredLoop()
		}
	}
}

func (p *parallelism) UpdateConfig(cfg types.ParallelismConfig) {
	p.mut.Lock()
	defer p.mut.Unlock()
	p.cfg = cfg
	p.statshub.SendParralelismStats(types.ParralelismStats{
		MinConnections:     p.cfg.MinConnections,
		MaxConnections:     p.cfg.MaxConnections,
		DesiredConnections: p.currentDesired,
	})
}

func (p *parallelism) desiredLoop() {
	p.mut.Lock()
	defer p.mut.Unlock()

	// Dont bother calculating if connections are the same value.
	if p.cfg.MinConnections == p.cfg.MaxConnections {
		p.calculateDesiredParallelism(p.currentDesired)
		return
	}

	// Prune the previous lookback to prevent flapping the desired amount.
	previousInLookback := make([]previousDesired, 0, len(p.previous))
	for _, previous := range p.previous {
		// Remove any outliers
		if time.Since(previous.recorded) <= p.cfg.Lookback {
			previousInLookback = append(previousInLookback, previous)
		}
	}
	p.previous = previousInLookback

	// Loop over network errors and remove them if the ttl expired.
	keepErrors := make([]time.Time, 0, len(p.networkErrors))
	for _, err := range p.networkErrors {
		if time.Since(err) <= p.cfg.ResetInterval {
			keepErrors = append(keepErrors, err)
		}
	}
	p.networkErrors = keepErrors

	keepSuccesses := make([]time.Time, 0, len(p.networkSuccesses))
	for _, success := range p.networkSuccesses {
		if time.Since(success) <= p.cfg.ResetInterval {
			keepSuccesses = append(keepSuccesses, success)
		}
	}
	p.networkSuccesses = keepSuccesses
	errorRate := p.networkErrorRate()
	// If we have network errors then ramp down the number of loops.
	if p.cfg.AllowedNetworkErrorFraction != 0.0 && errorRate >= p.cfg.AllowedNetworkErrorFraction {
		// Need to keep the value between min and max.
		if p.currentDesired-1 >= p.cfg.MinConnections {
			level.Debug(p.l).Log("msg", "triggering lower desired due to network errors", "desired", p.currentDesired-1)
			p.calculateDesiredParallelism(p.currentDesired - 1)
		}
		return
	}
	// If we are drifting too much then ramp up the number of loops.
	if float64(p.timestampDriftSeconds) > p.cfg.AllowedDrift.Seconds() {
		// Need to keep the value between min and max.
		if p.currentDesired+1 <= p.cfg.MaxConnections {
			level.Debug(p.l).Log("msg", "increasing desired due to timestamp drift", "desired", p.currentDesired+1, "drift", p.timestampDriftSeconds)
			p.calculateDesiredParallelism(p.currentDesired + 1)
		}
		return
	}

	// Can we ramp down, if we have dropped below our minimum scale down.
	if float64(p.timestampDriftSeconds) < p.cfg.MinimumScaleDownDrift.Seconds() {
		// Need to keep the value between min and max.
		if p.currentDesired-1 >= p.cfg.MinConnections {
			level.Debug(p.l).Log("msg", "decreasing desired due to drift lowering", "desired", p.currentDesired-1, "drift", p.timestampDriftSeconds)
			p.calculateDesiredParallelism(p.currentDesired - 1)

		}
		return
	}
	level.Debug(p.l).Log("msg", "no changes needed", "desired", p.currentDesired)
}

func (p *parallelism) networkErrorRate() float64 {
	// If nothing has happened assume success
	if len(p.networkSuccesses) == 0 && len(p.networkErrors) == 0 {
		return 0.0
	}

	if len(p.networkErrors) == 0 {
		return 0.0
	}

	if len(p.networkSuccesses) == 0 {
		return 1.0
	}

	errorRate := float64(len(p.networkErrors)) / float64(len(p.networkSuccesses)+len(p.networkErrors))
	return errorRate
}

func (p *parallelism) calculateDesiredParallelism(desired uint) {
	// Always add the desired to our previous entries.
	defer func() {
		p.previous = append(p.previous, previousDesired{
			desired:  desired,
			recorded: time.Now(),
		})
		p.statshub.SendParralelismStats(types.ParralelismStats{
			MaxConnections:     p.cfg.MaxConnections,
			MinConnections:     p.cfg.MinConnections,
			DesiredConnections: desired,
		})
	}()
	if desired == p.currentDesired {
		level.Debug(p.l).Log("msg", "desired is equal to current", "desired", desired)
		return
	}
	targetValue := desired
	// Are we ramping down?
	if desired < p.currentDesired {
		for _, previous := range p.previous {
			// If we previously said we needed a higher value then keep to that previous value.
			if targetValue < previous.desired {
				level.Debug(p.l).Log("msg", "lookback on previous values is higher, using higher value", "desired", targetValue, "previous", previous.desired)
				targetValue = previous.desired
			}
		}

		// Finally set the value of current and out if the values are different.
		// No need to notify if the same.
		if targetValue != p.currentDesired {
			p.currentDesired = targetValue
			level.Debug(p.l).Log("msg", "sending desired", "desired", p.currentDesired)
			p.out.Send(p.ctx, targetValue)
		}
	} else {
		// Going up is always allowed. Scaling up should be easy, scaling down should be slow.
		p.currentDesired = desired
		level.Debug(p.l).Log("msg", "sending desired", "desired", p.currentDesired)
		p.out.Send(p.ctx, desired)
	}
}
