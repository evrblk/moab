package main

import (
	"flag"
	"fmt"
	"time"
)

// Server-side limits enforced by Moab (see pkg/server/v0/validators.go). The
// generator validates against these up front so a bad flag fails fast instead
// of producing a wall of rejected requests.
const (
	maxPayloadSize           = 64 * 1024
	maxEnqueueBatchSize      = 50
	minQueueKeepaliveTimeout = 5
	maxQueueKeepaliveTimeout = 60
	minQueueExpiresInSeconds = 60
	maxQueueExpiresInSeconds = 14 * 86400
)

type Config struct {
	// Connection
	Endpoint string

	// Load shape
	Producers int
	Consumers int
	Queues    int
	Duration  time.Duration

	// EnqueueRate is the target number of Enqueue RPCs per second across all
	// producers combined (0 = unlimited).
	EnqueueRate int

	// Payload / batching
	PayloadSizeMin int
	PayloadSizeMax int
	BatchSizeMin   int
	BatchSizeMax   int

	// Task shape
	DedupeKeyPct       int
	DedupeKeysPerQueue int
	ThreadIdPct        int
	ThreadsPerQueue    int

	// Consumer behavior
	HandlerLatencyMsMin int
	HandlerLatencyMsMax int

	// Queue creation
	QueueNamePrefix       string
	QueueKeepaliveTimeout int
	QueueExpiresInSeconds int
	QueueSetupConcurrency int

	// General
	PrometheusListenAddr string
	LogInterval          time.Duration
	Cleanup              bool
}

func parseFlags() *Config {
	config := &Config{}

	// Connection
	flag.StringVar(&config.Endpoint, "endpoint", "localhost:8000", "Moab server (gateway or single-node) address")

	// Load shape
	flag.IntVar(&config.Producers, "producers", 100, "Number of concurrent producer goroutines")
	flag.IntVar(&config.Consumers, "consumers", 100, "Number of concurrent MoabConsumer instances")
	flag.IntVar(&config.Queues, "queues", 200, "Number of queues to create and spread load across")
	flag.DurationVar(&config.Duration, "duration", 60*time.Second, "Load test duration (0 = infinite)")
	flag.IntVar(&config.EnqueueRate, "enqueue-rate", 0, "Target Enqueue RPCs per second across all producers (0 = unlimited)")

	// Payload / batching
	flag.IntVar(&config.PayloadSizeMin, "payload-size-min", 64, "Minimum task payload size in bytes")
	flag.IntVar(&config.PayloadSizeMax, "payload-size-max", 1024, "Maximum task payload size in bytes")
	flag.IntVar(&config.BatchSizeMin, "batch-size-min", 1, "Minimum number of entries per Enqueue call")
	flag.IntVar(&config.BatchSizeMax, "batch-size-max", 10, "Maximum number of entries per Enqueue call")

	// Task shape
	flag.IntVar(&config.DedupeKeyPct, "dedupe-key-pct", 10, "Percentage of tasks enqueued with a dedupe_key")
	flag.IntVar(&config.DedupeKeysPerQueue, "dedupe-keys-per-queue", 1000, "Size of the reused dedupe_key pool per queue (bounds the pool so duplicates actually happen)")
	flag.IntVar(&config.ThreadIdPct, "thread-id-pct", 10, "Percentage of tasks enqueued with a thread_id")
	flag.IntVar(&config.ThreadsPerQueue, "threads-per-queue", 20, "Size of the reused thread_id pool per queue")

	// Consumer behavior
	flag.IntVar(&config.HandlerLatencyMsMin, "handler-latency-ms-min", 0, "Minimum simulated task handler processing time in milliseconds")
	flag.IntVar(&config.HandlerLatencyMsMax, "handler-latency-ms-max", 50, "Maximum simulated task handler processing time in milliseconds")

	// Queue creation
	flag.StringVar(&config.QueueNamePrefix, "queue-name-prefix", "load-test-queue", "Prefix for created queue names")
	flag.IntVar(&config.QueueKeepaliveTimeout, "queue-keepalive-timeout", 15, "Queue keepalive_timeout_in_seconds")
	flag.IntVar(&config.QueueExpiresInSeconds, "queue-expires-in-seconds", 86400, "Queue expires_in_seconds")
	flag.IntVar(&config.QueueSetupConcurrency, "queue-setup-concurrency", 32, "Number of concurrent GetOrCreateQueue calls during setup")

	// General
	flag.StringVar(&config.PrometheusListenAddr, "prometheus-listen-addr", ":2114", "Prometheus metrics bind address")
	flag.DurationVar(&config.LogInterval, "log-interval", 5*time.Second, "Stats logging interval")
	flag.BoolVar(&config.Cleanup, "cleanup", true, "Delete created queues on shutdown")

	flag.Parse()

	return config
}

func (c *Config) Validate() error {
	if c.Producers < 0 {
		return fmt.Errorf("producers cannot be negative, got %d", c.Producers)
	}
	if c.Consumers < 0 {
		return fmt.Errorf("consumers cannot be negative, got %d", c.Consumers)
	}
	if c.Producers == 0 && c.Consumers == 0 {
		return fmt.Errorf("at least one of producers or consumers must be positive")
	}
	if c.Queues <= 0 {
		return fmt.Errorf("queues must be positive, got %d", c.Queues)
	}
	if c.Duration < 0 {
		return fmt.Errorf("duration cannot be negative, got %v", c.Duration)
	}
	if c.EnqueueRate < 0 {
		return fmt.Errorf("enqueue-rate cannot be negative, got %d", c.EnqueueRate)
	}

	if c.PayloadSizeMin < 0 {
		return fmt.Errorf("payload-size-min cannot be negative, got %d", c.PayloadSizeMin)
	}
	if c.PayloadSizeMax < c.PayloadSizeMin {
		return fmt.Errorf("payload-size-max (%d) cannot be smaller than payload-size-min (%d)", c.PayloadSizeMax, c.PayloadSizeMin)
	}
	if c.PayloadSizeMax > maxPayloadSize {
		return fmt.Errorf("payload-size-max (%d) exceeds Moab's max payload size (%d)", c.PayloadSizeMax, maxPayloadSize)
	}

	if c.BatchSizeMin <= 0 {
		return fmt.Errorf("batch-size-min must be positive, got %d", c.BatchSizeMin)
	}
	if c.BatchSizeMax < c.BatchSizeMin {
		return fmt.Errorf("batch-size-max (%d) cannot be smaller than batch-size-min (%d)", c.BatchSizeMax, c.BatchSizeMin)
	}
	if c.BatchSizeMax > maxEnqueueBatchSize {
		return fmt.Errorf("batch-size-max (%d) exceeds Moab's max entries per Enqueue call (%d)", c.BatchSizeMax, maxEnqueueBatchSize)
	}

	for _, p := range []struct {
		name string
		val  int
	}{
		{"dedupe-key-pct", c.DedupeKeyPct},
		{"thread-id-pct", c.ThreadIdPct},
	} {
		if p.val < 0 || p.val > 100 {
			return fmt.Errorf("%s must be between 0 and 100, got %d", p.name, p.val)
		}
	}
	if c.DedupeKeysPerQueue <= 0 {
		return fmt.Errorf("dedupe-keys-per-queue must be positive, got %d", c.DedupeKeysPerQueue)
	}
	if c.ThreadsPerQueue <= 0 {
		return fmt.Errorf("threads-per-queue must be positive, got %d", c.ThreadsPerQueue)
	}

	if c.HandlerLatencyMsMin < 0 {
		return fmt.Errorf("handler-latency-ms-min cannot be negative, got %d", c.HandlerLatencyMsMin)
	}
	if c.HandlerLatencyMsMax < c.HandlerLatencyMsMin {
		return fmt.Errorf("handler-latency-ms-max (%d) cannot be smaller than handler-latency-ms-min (%d)", c.HandlerLatencyMsMax, c.HandlerLatencyMsMin)
	}

	if c.QueueNamePrefix == "" {
		return fmt.Errorf("queue-name-prefix cannot be empty")
	}
	if c.QueueKeepaliveTimeout < minQueueKeepaliveTimeout || c.QueueKeepaliveTimeout > maxQueueKeepaliveTimeout {
		return fmt.Errorf("queue-keepalive-timeout must be between %d and %d, got %d", minQueueKeepaliveTimeout, maxQueueKeepaliveTimeout, c.QueueKeepaliveTimeout)
	}
	if c.QueueExpiresInSeconds < minQueueExpiresInSeconds || c.QueueExpiresInSeconds > maxQueueExpiresInSeconds {
		return fmt.Errorf("queue-expires-in-seconds must be between %d and %d, got %d", minQueueExpiresInSeconds, maxQueueExpiresInSeconds, c.QueueExpiresInSeconds)
	}
	if c.QueueSetupConcurrency <= 0 {
		return fmt.Errorf("queue-setup-concurrency must be positive, got %d", c.QueueSetupConcurrency)
	}

	return nil
}
