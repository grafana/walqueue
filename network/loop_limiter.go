package network

import (
	"context"
	"sync"
	"time"
)

type limitConfig struct {
	interval            time.Duration
	resetInterval       time.Duration
	allowedDriftSeconds int
	maxLoops            int
	minLoops            int
}

type limiter struct {
	mut sync.Mutex
	// networkErrors is any 4xx,5xx, when one comes it is timestamped so that after so long it falls off.
	networkErrors    []time.Time
	currentLoops     int
	timeDriftSeconds int
	cfg              limitConfig
	out              chan int
	cfgChan          chan limitConfig
}

func newLimiter(cfg limitConfig, out chan int) *limiter {
	return &limiter{
		cfg: cfg,
		out: out,
	}
}

func (p *limiter) updateConfig(cfg limitConfig) {
	p.cfgChan <- cfg
}

func (p *limiter) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(p.cfg.interval):
			p.desiredLoops()
		case cfg := <-p.cfgChan:
			p.cfg = cfg
		}
	}
}

func (p *limiter) desiredLoops() {
	p.mut.Lock()
	defer p.mut.Unlock()

	// Dont bother calculating if loops are the same value.
	if p.cfg.minLoops == p.cfg.maxLoops {
		p.out <- p.cfg.minLoops
	}

	// Loop over network errors and remove them if the ttl expired.
	for _, err := range p.networkErrors {
		if time.Since(err) > p.cfg.resetInterval {
			p.networkErrors = p.networkErrors[1:]
		}
	}

	// If we have network errors then ramp down the number of loops.
	if len(p.networkErrors) > 0 {
		// Need to keep the value between min and max.
		if p.currentLoops-1 >= p.cfg.minLoops {
			p.currentLoops--
		}
		p.out <- p.currentLoops
		return
	}
	// If we are drifting too much then ramp up the number of loops.
	if p.timeDriftSeconds > p.cfg.allowedDriftSeconds {
		// Need to keep the value between min and max.
		if p.currentLoops+1 <= p.cfg.maxLoops {
			p.currentLoops++
		}
		p.out <- p.currentLoops
		return
	}
}
