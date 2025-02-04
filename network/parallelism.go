package network

import (
	"context"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/walqueue/types"
)

type parallelism struct {
	mut         sync.RWMutex
	cfg         parallelismConfig
	statshub    types.StatsHub
	driftNotify *types.Mailbox[uint]
	// networkErrors is any 4xx,5xx.
	networkErrors         []time.Time
	networkSuccesses      []time.Time
	timestampDriftSeconds int64
	currentLoops          uint
	out                   chan uint
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

type parallelismConfig struct {
	allowedDriftSeconds        int64
	maxLoops                   uint
	minLoops                   uint
	resetInterval              time.Duration
	lookback                   time.Duration
	checkInterval              time.Duration
	allowedNetworkErrorPercent float64
}

func newParallelism(cfg parallelismConfig, out chan uint, statshub types.StatsHub, l log.Logger) *parallelism {
	p := &parallelism{
		cfg:          cfg,
		statshub:     statshub,
		currentLoops: cfg.minLoops,
		out:          out,
		stop:         make(chan struct{}),
		l:            l,
	}
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

		if p.timestampNetworkSeconds > 0 && p.timestampSerializerSeconds > 0 {
			p.timestampDriftSeconds = p.timestampSerializerSeconds - p.timestampNetworkSeconds
		}
	})

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
	for {
		var checkInterval time.Duration
		p.mut.RLock()
		checkInterval = p.cfg.checkInterval
		p.mut.RUnlock()
		select {
		case <-ctx.Done():
			return
		case <-p.stop:
			return
		case <-time.After(checkInterval):
			p.desiredLoops()
		}
	}
}

func (p *parallelism) AddNetworkError() {
	p.mut.Lock()
	defer p.mut.Unlock()

	p.networkErrors = append(p.networkErrors, time.Now())
}

func (p *parallelism) desiredLoops() {
	p.mut.Lock()
	defer p.mut.Unlock()

	// Dont bother calculating if loops are the same value.
	if p.cfg.minLoops == p.cfg.maxLoops {
		level.Debug(p.l).Log("msg", "min and max loops are same no change", "desired", p.cfg.minLoops)
		p.changeParallelism(p.currentLoops)
	}

	// Loop over network errors and remove them if the ttl expired.
	for _, err := range p.networkErrors {
		if time.Since(err) > p.cfg.resetInterval {
			p.networkErrors = p.networkErrors[1:]
		}
	}

	for _, err := range p.networkSuccesses {
		if time.Since(err) > p.cfg.resetInterval {
			p.networkSuccesses = p.networkSuccesses[1:]
		}
	}

	// If we have network errors then ramp down the number of loops.
	if p.cfg.allowedNetworkErrorPercent != 0.0 && p.networkErrorRate() >= p.cfg.allowedNetworkErrorPercent {
		// Need to keep the value between min and max.
		if p.currentLoops-1 >= p.cfg.minLoops {
			level.Debug(p.l).Log("msg", "triggering lower desired due to network errors", "desired", p.currentLoops-1)
			p.changeParallelism(p.currentLoops - 1)
		}
		return
	}
	// If we are drifting too much then ramp up the number of loops.
	if p.timestampDriftSeconds > p.cfg.allowedDriftSeconds {
		// Need to keep the value between min and max.
		if p.currentLoops+1 <= p.cfg.maxLoops {
			level.Debug(p.l).Log("msg", "increasing desired due to timestamp drift", "desired", p.currentLoops+1, "drift", p.timestampDriftSeconds)
			p.changeParallelism(p.currentLoops + 1)
		}
		return
	}

	// Can we ramp down, only ramp down if we are 10% below the target.
	if p.timestampDriftSeconds+int64(float64(p.cfg.allowedDriftSeconds)*0.1) < p.cfg.allowedDriftSeconds {
		// Need to keep the value between min and max.
		if p.currentLoops-1 >= p.cfg.minLoops {
			level.Debug(p.l).Log("msg", "decreasing desired due to drift lowering", "desired", p.currentLoops-1, "drift", p.timestampDriftSeconds)
			p.changeParallelism(p.currentLoops - 1)
		}
	}
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

	errorRate := float64(len(p.networkErrors)) / float64(len(p.networkSuccesses))
	return errorRate
}

func (p *parallelism) changeParallelism(desired uint) {
	// Always add the desired to our previous entries.
	defer func() {
		p.previous = append(p.previous, previousDesired{
			desired:  desired,
			recorded: time.Now(),
		})
	}()
	if desired == p.currentLoops {
		level.Debug(p.l).Log("msg", "desired is equal to current", "desired", desired)
		return
	}
	actualValue := desired
	// Are we ramping down?
	if desired < p.currentLoops {
		// Let's see if we are going to catch the flapping issues.
		for _, previous := range p.previous {
			// Remove any outliers
			if time.Since(previous.recorded) > p.cfg.lookback {
				p.previous = p.previous[1:]
				continue
			}
			// If we previously said we needed a higher value then keep to that previous value.
			if actualValue < previous.desired {
				level.Debug(p.l).Log("msg", "lookback on previous values is higher, using higher value", "desired", actualValue, "previous", previous.desired)
				actualValue = previous.desired
			}
		}
		// Finally set the value of current and out if the values are different.
		// No need to notify if the same.
		if actualValue != p.currentLoops {
			p.currentLoops = actualValue
			level.Debug(p.l).Log("msg", "sending desired", p.currentLoops)
			p.out <- actualValue
		}
	} else {
		// Going up is always allowed. Scaling up should be easy, scaling down should be slow.
		p.currentLoops = desired
		level.Debug(p.l).Log("msg", "sending desired", p.currentLoops)
		p.out <- p.currentLoops
	}
}
