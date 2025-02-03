package network

import (
	"context"
	"sync"
	"time"
)

type parallelism struct {
	mut           sync.Mutex
	resetInterval time.Duration
	// networkErrors is any 4xx,5xx.
	networkErrors         []time.Time
	timestampDriftSeconds uint
	allowedDriftSeconds   uint
	maxLoops              uint
	minLoops              uint
	currentLoops          uint
	out                   chan uint
	stop                  chan struct{}
	networkError          chan time.Time
	// previous is the number of previous desired instances. This is to prevent flapping.
	previous []previousDesired
}

type previousDesired struct {
	desired  uint
	recorded time.Time
}

func newParallelism(resetInterval time.Duration, allowedDriftSeconds uint, maxLoops uint, minLoops uint, out chan uint) *parallelism {
	return &parallelism{
		resetInterval:       resetInterval,
		maxLoops:            maxLoops,
		minLoops:            minLoops,
		currentLoops:        minLoops,
		out:                 out,
		allowedDriftSeconds: allowedDriftSeconds,
		stop:                make(chan struct{}),
	}
}

func (p *parallelism) Stop() {
	p.stop <- struct{}{}
}

func (p *parallelism) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-p.stop:
			return
		case <-time.After(10 * time.Second):
			p.desiredLoops()
		}
	}
}

func (p *parallelism) AddNetworkError() {
	p.mut.Lock()
	defer p.mut.Unlock()

	p.networkErrors = append(p.networkErrors, time.Now())
}

func (p *parallelism) SetDriftSeconds(seconds uint) {
	p.mut.Lock()
	defer p.mut.Unlock()

	p.timestampDriftSeconds = seconds
}

func (p *parallelism) desiredLoops() {
	p.mut.Lock()
	defer p.mut.Unlock()

	// Dont bother calculating if loops are the same value.
	if p.minLoops == p.maxLoops {
		p.changeParallelism(p.currentLoops)
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
			p.changeParallelism(p.currentLoops - 1)
		}
		return
	}
	// If we are drifting too much then ramp up the number of loops.
	if p.timestampDriftSeconds > p.allowedDriftSeconds {
		// Need to keep the value between min and max.
		if p.currentLoops+1 <= p.maxLoops {
			p.changeParallelism(p.currentLoops + 1)
		}
		return
	}

	// Can we ramp down, only ramp down if we are 10% below the target.
	if p.timestampDriftSeconds+uint(float64(p.allowedDriftSeconds)*1.1) < p.allowedDriftSeconds {
		// Need to keep the value between min and max.
		if p.currentLoops+1 <= p.maxLoops {
			p.currentLoops++
		}
		p.out <- p.currentLoops
	}
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
		return
	}
	actualValue := desired
	// Are we ramping down?
	if desired < p.currentLoops {
		// Let's see if we are going to catch the flapping issues.
		for _, previous := range p.previous {
			// Remove any outliers
			if time.Since(previous.recorded) > 5*time.Minute {
				p.previous = p.previous[1:]
				continue
			}
			// If we previously said we needed a higher value then keep to that previous value.
			if actualValue < previous.desired {
				actualValue = previous.desired
			}
		}
		// Finally set the value of current and out if the values are different.
		// No need to notify if the same.
		if actualValue != p.currentLoops {
			p.currentLoops = actualValue
			p.out <- actualValue
		}
	} else {
		// Going up is always allowed. Scaling up should be easy, scaling down should be slow.
		p.currentLoops = desired
		p.out <- p.currentLoops
	}
}
