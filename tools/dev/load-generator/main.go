package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	evrblk "github.com/evrblk/evrblk-go"
	moab "github.com/evrblk/evrblk-go/moab/v0"
	"github.com/evrblk/yellowstone-common/metrics"
)

func main() {
	config := parseFlags()

	if err := config.Validate(); err != nil {
		log.Fatalf("Configuration error: %v", err)
	}

	log.Println("Starting Moab Load Generator...")
	log.Printf("Configuration:")
	log.Printf("  Endpoint: %s", config.Endpoint)
	log.Printf("  Producers: %d", config.Producers)
	log.Printf("  Consumers: %d", config.Consumers)
	log.Printf("  Queues: %d", config.Queues)
	log.Printf("  Duration: %v", config.Duration)
	log.Printf("  Enqueue rate: %d req/sec (0 = unlimited)", config.EnqueueRate)
	log.Printf("  Payload size: [%d, %d] bytes", config.PayloadSizeMin, config.PayloadSizeMax)
	log.Printf("  Batch size: [%d, %d] entries", config.BatchSizeMin, config.BatchSizeMax)
	log.Printf("  Dedupe key: %d%% of tasks (pool of %d per queue)", config.DedupeKeyPct, config.DedupeKeysPerQueue)
	log.Printf("  Thread id: %d%% of tasks (pool of %d per queue)", config.ThreadIdPct, config.ThreadsPerQueue)
	log.Printf("  Handler latency: [%d, %d] ms", config.HandlerLatencyMsMin, config.HandlerLatencyMsMax)

	// Start Prometheus metrics server
	RegisterMetrics()
	metricsSrv := metrics.NewMetricsServer(config.PrometheusPort)
	metricsSrv.Start()
	defer metricsSrv.Stop()
	log.Printf("Prometheus metrics available at http://localhost:%d/metrics", config.PrometheusPort)

	// Connect to Moab
	log.Printf("Connecting to Moab at %s...", config.Endpoint)
	client, err := moab.NewMoabGrpcClient(config.Endpoint, evrblk.NewNoOpSigner())
	if err != nil {
		log.Fatalf("Failed to create Moab client: %v", err)
	}
	defer client.Close()

	setupCtx, setupCancel := context.WithTimeout(context.Background(), 2*time.Minute)
	queueNames, err := SetupQueues(setupCtx, client, config)
	setupCancel()
	if err != nil {
		log.Fatalf("Failed to set up queues: %v", err)
	}
	queuesGauge.Set(float64(len(queueNames)))

	stats := NewStatsCollector()

	mainCtx, mainCancel := context.WithCancel(context.Background())
	defer mainCancel()

	// Stats logger
	go func() {
		ticker := time.NewTicker(config.LogInterval)
		defer ticker.Stop()
		for {
			select {
			case <-mainCtx.Done():
				return
			case <-ticker.C:
				stats.PrintStats()
			}
		}
	}()

	// Worker context, bounded by Duration if configured
	workerCtx := mainCtx
	var workerCancel context.CancelFunc
	if config.Duration > 0 {
		workerCtx, workerCancel = context.WithTimeout(mainCtx, config.Duration)
		defer workerCancel()
		log.Printf("Load test will run for %v", config.Duration)
	} else {
		log.Println("Load test will run indefinitely (press Ctrl+C to stop)")
	}

	var wg sync.WaitGroup

	log.Printf("Starting %d consumers...", config.Consumers)
	for i := 0; i < config.Consumers; i++ {
		queueName := queueNames[i%len(queueNames)]
		wg.Add(1)
		go func(id int, queueName string) {
			defer wg.Done()
			RunConsumer(workerCtx, id, client, queueName, config, stats)
		}(i, queueName)
	}
	activeConsumers.Set(float64(config.Consumers))

	log.Printf("Starting %d producers...", config.Producers)
	enqueueRatePerProducer := 0.0
	if config.EnqueueRate > 0 {
		enqueueRatePerProducer = float64(config.EnqueueRate) / float64(config.Producers)
	}
	for i := 0; i < config.Producers; i++ {
		producer := NewProducer(i, client, config, queueNames, stats, newPacer(enqueueRatePerProducer))
		wg.Add(1)
		go func() {
			defer wg.Done()
			producer.Run(workerCtx)
		}()
	}
	activeProducers.Set(float64(config.Producers))

	log.Println("All producers and consumers started!")

	// Signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-sigChan:
		log.Println("Received shutdown signal...")
	case <-workerCtx.Done():
		log.Println("Duration elapsed...")
	}

	log.Println("Stopping producers and consumers...")
	mainCancel()
	wg.Wait()
	activeProducers.Set(0)
	activeConsumers.Set(0)
	log.Println("All producers and consumers stopped")

	log.Println("=== Final Statistics ===")
	stats.PrintStats()

	if config.Cleanup {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 2*time.Minute)
		CleanupQueues(cleanupCtx, client, queueNames, config)
		cleanupCancel()
	} else {
		log.Println("Skipping queue cleanup (--cleanup=false)")
	}

	log.Println("Done!")
}
