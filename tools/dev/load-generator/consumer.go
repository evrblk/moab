package main

import (
	"context"
	"math/rand"
	"time"

	moab "github.com/evrblk/evrblk-go/moab/v0"
)

// RunConsumer starts one MoabConsumer bound to the given queue and blocks
// until ctx is cancelled. It is meant to be run on its own goroutine.
//
// MoabConsumer.Start invokes the handler from up to 32 internal worker
// goroutines concurrently, so the handler uses the math/rand package-level
// functions (safe for concurrent use) rather than a private *rand.Rand.
func RunConsumer(ctx context.Context, id int, client moab.MoabApi, queueName string, config *Config, stats *StatsCollector) {
	consumer := moab.NewMoabConsumer(client, queueName)
	consumer.Start(ctx, moab.HandlerFunc(func(task *moab.Task) error {
		startTime := time.Now()

		if config.HandlerLatencyMsMax > 0 {
			sleep := time.Duration(randIntBetween(config.HandlerLatencyMsMin, config.HandlerLatencyMsMax)) * time.Millisecond
			select {
			case <-ctx.Done():
			case <-time.After(sleep):
			}
		}

		handlerDuration.Observe(time.Since(startTime).Seconds())
		tasksHandledTotal.WithLabelValues("success").Inc()
		stats.RecordHandled(true)

		return nil
	}))
}

// randIntBetween returns a random integer from [a, b] using the math/rand
// package-level source, which is safe for concurrent use (unlike a private
// *rand.Rand, which is not).
func randIntBetween(a, b int) int {
	if a >= b {
		return a
	}
	return a + rand.Intn(b-a+1)
}
