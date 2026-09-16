package main

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	evrblk "github.com/evrblk/evrblk-go"
	moab "github.com/evrblk/evrblk-go/moab/v0"
)

// Producer generates Enqueue traffic against a random queue out of the shared
// pool, on its own goroutine.
type Producer struct {
	id     int
	client moab.MoabApi
	config *Config
	queues []string
	stats  *StatsCollector
	rng    *rand.Rand
	pacer  *pacer
}

// NewProducer creates a new producer.
func NewProducer(id int, client moab.MoabApi, config *Config, queues []string, stats *StatsCollector, p *pacer) *Producer {
	return &Producer{
		id:     id,
		client: client,
		config: config,
		queues: queues,
		stats:  stats,
		rng:    rand.New(rand.NewSource(time.Now().UnixNano() + int64(id))),
		pacer:  p,
	}
}

// Run executes the producer loop until ctx is cancelled.
func (p *Producer) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if p.pacer != nil {
			if err := p.pacer.wait(ctx); err != nil {
				return
			}
		}

		queueIdx := p.rng.Intn(len(p.queues))
		request := p.buildEnqueueRequest(queueIdx)

		reqCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		startTime := time.Now()
		resp, err := p.client.Enqueue(reqCtx, request)
		cancel()
		duration := time.Since(startTime).Seconds()

		if ctx.Err() != nil {
			return
		}

		enqueueDuration.Observe(duration)

		success := err == nil
		statusLabel := "success"
		tasksEnqueued := 0
		if success {
			tasksEnqueued = len(resp.Tasks)
		} else {
			statusLabel = "error"
			enqueueErrorsTotal.WithLabelValues(getErrorType(err)).Inc()
		}
		enqueueRequestsTotal.WithLabelValues(statusLabel).Inc()
		tasksEnqueuedTotal.Add(float64(tasksEnqueued))

		p.stats.RecordEnqueue(tasksEnqueued, success)
	}
}

// buildEnqueueRequest builds a batch of entries for the given queue index,
// applying the configured payload size range and dedupe_key / thread_id
// injection rates.
func (p *Producer) buildEnqueueRequest(queueIdx int) *moab.EnqueueRequest {
	queueName := p.queues[queueIdx]
	batchSize := randBetween(p.rng, p.config.BatchSizeMin, p.config.BatchSizeMax)

	entries := make([]*moab.EnqueueRequestEntry, 0, batchSize)
	for i := 0; i < batchSize; i++ {
		payloadSize := randBetween(p.rng, p.config.PayloadSizeMin, p.config.PayloadSizeMax)
		entry := &moab.EnqueueRequestEntry{
			Payload: randomPayload(p.rng, payloadSize),
		}

		if p.rng.Intn(100) < p.config.DedupeKeyPct {
			entry.DedupeKey = fmt.Sprintf("dedupe-%d-%d", queueIdx, p.rng.Intn(p.config.DedupeKeysPerQueue))
		}
		if p.rng.Intn(100) < p.config.ThreadIdPct {
			entry.ThreadId = fmt.Sprintf("thread-%d-%d", queueIdx, p.rng.Intn(p.config.ThreadsPerQueue))
		}

		entries = append(entries, entry)
	}

	return &moab.EnqueueRequest{
		QueueName: queueName,
		Entries:   entries,
	}
}

const payloadAlphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

// randomPayload returns a random byte slice of the given size.
func randomPayload(rng *rand.Rand, size int) []byte {
	b := make([]byte, size)
	for i := range b {
		b[i] = payloadAlphabet[rng.Intn(len(payloadAlphabet))]
	}
	return b
}

// randBetween returns a random integer from [a, b].
func randBetween(rng *rand.Rand, a, b int) int {
	if a >= b {
		return a
	}
	return a + rng.Intn(b-a+1)
}

// getErrorType extracts a coarse error type label for metrics.
func getErrorType(err error) string {
	switch evrblk.CodeOf(err) {
	case evrblk.InternalFailure:
		return "internal_failure"
	case evrblk.Timeout:
		return "timeout"
	case evrblk.InvalidRequest:
		return "invalid_request"
	case evrblk.Unauthenticated:
		return "unauthenticated"
	case evrblk.PermissionDenied:
		return "permission_denied"
	case evrblk.NotFound:
		return "not_found"
	case evrblk.ResourceExhausted:
		return "resource_exhausted"
	case evrblk.Unavailable:
		return "unavailable"
	case evrblk.AlreadyExists:
		return "already_exists"
	default:
		return "unknown"
	}
}
