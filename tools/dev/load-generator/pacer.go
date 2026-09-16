package main

import (
	"context"
	"time"
)

// pacer throttles a loop to a fixed rate by sleeping a fixed interval between
// iterations. It is intentionally simple (no token bucket / bursting) since
// the load generator only needs an approximate, evenly spread target rate.
type pacer struct {
	interval time.Duration
	last     time.Time
}

// newPacer returns a pacer for the given target rate (iterations per
// second), or nil if ratePerSecond is 0 (unlimited).
func newPacer(ratePerSecond float64) *pacer {
	if ratePerSecond <= 0 {
		return nil
	}
	return &pacer{interval: time.Duration(float64(time.Second) / ratePerSecond)}
}

// wait blocks until it is time for the next iteration, or ctx is done.
func (p *pacer) wait(ctx context.Context) error {
	now := time.Now()
	if !p.last.IsZero() {
		if sleep := p.interval - now.Sub(p.last); sleep > 0 {
			timer := time.NewTimer(sleep)
			defer timer.Stop()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-timer.C:
			}
		}
	}
	p.last = time.Now()
	return nil
}
