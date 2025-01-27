package network

import (
	"context"
	"sync"
	"time"

	"go.uber.org/atomic"
)

type profiler struct {
	mut           sync.Mutex
	interval      time.Duration
	resetInterval time.Duration
	// networkErrors is any 4xx,5xx.
	networkErrors         []time.Time
	timestampDriftSeconds int
	allowedDriftSeconds   atomic.Int64
	maxLoops              int
	minLoops              int
	currentLoops          int
	loopBatch             int
	out                   chan int
}

func newProfiler(interval time.Duration, resetInterval time.Duration, allowedDriftSeconds int, maxLoops int, minLoops int, loopBatch int, out chan int) *profiler {
	return &profiler{
		interval:            interval,
		resetInterval:       resetInterval,
		maxLoops:            maxLoops,
		minLoops:            minLoops,
		loopBatch:           loopBatch,
		currentLoops:        minLoops,
		out:                 out,
		allowedDriftSeconds: *atomic.NewInt64(int64(allowedDriftSeconds)),
	}
}

func (p *profiler) updateConfig(maxLoops int, minLoops int, loopBatch int) {
	p.mut.Lock()
	defer p.mut.Unlock()

	p.maxLoops = maxLoops
	p.minLoops = minLoops
	p.loopBatch = loopBatch
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
	p.mut.Lock()
	defer p.mut.Unlock()

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
}
