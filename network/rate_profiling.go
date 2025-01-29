package network

import (
	"context"
	"github.com/grafana/walqueue/types"
	"slices"
	"time"

	"go.uber.org/atomic"
)

type profiler struct {
	configInbox   *types.SyncMailbox[config, bool]
	resetInterval time.Duration
	// networkErrors is any 4xx,5xx.
	networkErrors         []time.Time
	timestampDriftSeconds int
	loopBatch             int
	out                   chan int
	currentLoops          int
	cfg                   config
	// Whenever a new point is calculated it is added to here. This is used
	// to reduce flapping behavior. So if the lookback is 5m then the
	// find the max number of records for the past 5 minutes when it needs to downgrade
	// and use those. This is only used when scaling down.
	past []pastPoints
}

type pastPoints struct {
	desiredLoops int
	ts           time.Time
}

type config struct {
	maxLoops            int
	minLoops            int
	allowedDriftSeconds int
	interval            time.Duration
}

func newProfiler(cfg config, out chan int) *profiler {
	return &profiler{
		cfg: cfg,
		out: out,
	}
}

func (p *profiler) updateConfig(ctx context.Context, cfg config) (bool, error) {
	return p.configInbox.Send(ctx, cfg)
}

func (p *profiler) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(60 * time.Second):
			p.desiredLoops()
		}
	}
}

func (p *profiler) desiredLoops() {

	// Dont bother calculating if loops are the same value.
	if p.minLoops == p.maxLoops {
		p.out <- p.minLoops
	}

	// Loop over network errors and remove them if the ttl expired.
	for _, err := range p.networkErrors {
		if time.Since(err) > p.resetInterval {
			p.networkErrors = p.networkErrors[1:]
		}
	}

	// If we have network errors then ramp down the number of loops.
	if len(p.networkErrors) > 0 {
		maxPast := p.highestDesiredLast()
		if maxPast > p.currentLoops-1 {
			// Need to append last point
			p.past = append(p.past, pastPoints{
				desiredLoops: p.currentLoops - 1,
				ts:           time.Now(),
			})
			p.out <- p.currentLoops - 1
			return

		}
		// Need to keep the value between min and max.
		if p.currentLoops-1 >= p.minLoops {
			p.currentLoops--
		}
		p.out <- p.currentLoops
		return
	}
	// If we are drifting too much then ramp up the number of loops.
	if p.timestampDriftSeconds > int(p.allowedDriftSeconds.Load()) {
		// Need to keep the value between min and max.
		if p.currentLoops+1 <= p.maxLoops {
			p.currentLoops++
		}
		p.out <- p.currentLoops
		return
	}
	// If we are drifting too much then ramp up the number of loops.
	if p.timestampDriftSeconds < int(p.allowedDriftSeconds.Load()) {
		// refactor this.
		maxPast := p.highestDesiredLast()
		if maxPast > p.currentLoops-1 {
			// Need to append last point
			p.past = append(p.past, pastPoints{
				desiredLoops: p.currentLoops - 1,
				ts:           time.Now(),
			})
			p.out <- p.currentLoops - 1
			return
		}
		// Need to keep the value between min and max.
		if p.currentLoops-1 >= p.minLoops {
			p.currentLoops--
		}
		p.out <- p.currentLoops
		return
	}
}

func (p *profiler) highestDesiredLast() int {
	if len(p.past) == 0 {
		return 0
	}
	for _, ts := range p.past {
		if time.Since(ts.ts) > 5*time.Minute {
			p.past = p.past[1:]
		}
	}
	maxValue := slices.MaxFunc(p.past, func(a, b pastPoints) int {
		if a.desiredLoops > b.desiredLoops {
			return a.desiredLoops
		}
		return b.desiredLoops
	})
	return maxValue.desiredLoops
}
