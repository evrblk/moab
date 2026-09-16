package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	evrblk "github.com/evrblk/evrblk-go"
	moab "github.com/evrblk/evrblk-go/moab/v0"
)

// SetupQueues creates config.Queues queues (or reuses ones that already exist
// from a previous run), fanning the work out over config.QueueSetupConcurrency
// concurrent workers since we may be creating hundreds of them.
func SetupQueues(ctx context.Context, client moab.MoabApi, config *Config) ([]string, error) {
	log.Printf("Setting up %d queues...", config.Queues)

	names := make([]string, config.Queues)
	for i := 0; i < config.Queues; i++ {
		names[i] = fmt.Sprintf("%s-%d", config.QueueNamePrefix, i)
	}

	sem := make(chan struct{}, config.QueueSetupConcurrency)
	errCh := make(chan error, config.Queues)
	var wg sync.WaitGroup

	for _, name := range names {
		wg.Add(1)
		sem <- struct{}{}
		go func(name string) {
			defer wg.Done()
			defer func() { <-sem }()

			if err := getOrCreateQueue(ctx, client, name, config); err != nil {
				errCh <- fmt.Errorf("queue %s: %w", name, err)
			}
		}(name)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		return nil, err
	}

	log.Printf("%d queues ready", config.Queues)
	return names, nil
}

func getOrCreateQueue(ctx context.Context, client moab.MoabApi, name string, config *Config) error {
	getCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	_, err := client.GetQueue(getCtx, &moab.GetQueueRequest{QueueName: name})
	if err == nil {
		return nil
	}
	if !evrblk.IsNotFound(err) {
		return err
	}

	createCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	_, err = client.CreateQueue(createCtx, &moab.CreateQueueRequest{
		Name:                      name,
		Description:               "Created by moab load generator",
		KeepaliveTimeoutInSeconds: int64(config.QueueKeepaliveTimeout),
		ExpiresInSeconds:          int64(config.QueueExpiresInSeconds),
	})
	if err != nil && evrblk.IsAlreadyExists(err) {
		// Lost a race with another setup goroutine or a concurrent run.
		return nil
	}
	return err
}

// CleanupQueues deletes all queues created for this run.
func CleanupQueues(ctx context.Context, client moab.MoabApi, names []string, config *Config) {
	log.Println("Cleaning up queues...")

	sem := make(chan struct{}, config.QueueSetupConcurrency)
	var wg sync.WaitGroup

	for _, name := range names {
		wg.Add(1)
		sem <- struct{}{}
		go func(name string) {
			defer wg.Done()
			defer func() { <-sem }()

			delCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()

			_, err := client.DeleteQueue(delCtx, &moab.DeleteQueueRequest{QueueName: name})
			if err != nil && !evrblk.IsNotFound(err) {
				log.Printf("Warning: failed to delete queue %s: %v", name, err)
			}
		}(name)
	}

	wg.Wait()
	log.Println("Queue cleanup complete")
}
