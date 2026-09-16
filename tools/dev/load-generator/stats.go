package main

import (
	"fmt"
	"sync"
	"time"
)

// StatsCollector collects and reports statistics.
type StatsCollector struct {
	mu        sync.Mutex
	startTime time.Time

	enqueueRequests uint64
	enqueueErrors   uint64
	tasksEnqueued   uint64

	tasksHandled  uint64
	handlerErrors uint64

	// For rate calculation over the last logging window.
	lastWindowTasksEnqueued uint64
	lastWindowTasksHandled  uint64
	lastWindowTime          time.Time
}

// NewStatsCollector creates a new statistics collector.
func NewStatsCollector() *StatsCollector {
	now := time.Now()
	return &StatsCollector{
		startTime:      now,
		lastWindowTime: now,
	}
}

// RecordEnqueue records the outcome of a single Enqueue RPC.
func (s *StatsCollector) RecordEnqueue(tasksEnqueued int, success bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.enqueueRequests++
	s.tasksEnqueued += uint64(tasksEnqueued)
	if !success {
		s.enqueueErrors++
	}
}

// RecordHandled records the outcome of a single task handler invocation.
func (s *StatsCollector) RecordHandled(success bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.tasksHandled++
	if !success {
		s.handlerErrors++
	}
}

// PrintStats prints current statistics to stdout.
func (s *StatsCollector) PrintStats() {
	s.mu.Lock()
	defer s.mu.Unlock()

	elapsed := time.Since(s.startTime)

	enqueueSuccessRate := 100.0
	if s.enqueueRequests > 0 {
		enqueueSuccessRate = float64(s.enqueueRequests-s.enqueueErrors) / float64(s.enqueueRequests) * 100
	}
	handlerSuccessRate := 100.0
	if s.tasksHandled > 0 {
		handlerSuccessRate = float64(s.tasksHandled-s.handlerErrors) / float64(s.tasksHandled) * 100
	}

	now := time.Now()
	windowSeconds := now.Sub(s.lastWindowTime).Seconds()
	enqueueRPS := 0.0
	consumeRPS := 0.0
	if windowSeconds > 0 {
		enqueueRPS = float64(s.tasksEnqueued-s.lastWindowTasksEnqueued) / windowSeconds
		consumeRPS = float64(s.tasksHandled-s.lastWindowTasksHandled) / windowSeconds
	}
	s.lastWindowTasksEnqueued = s.tasksEnqueued
	s.lastWindowTasksHandled = s.tasksHandled
	s.lastWindowTime = now

	fmt.Printf("[%s] Enqueued: %s tasks (%s reqs, %s errors, %s tasks/sec) | Handled: %s tasks (%s errors, %s tasks/sec) | Success: enqueue=%.1f%% handler=%.1f%%\n",
		formatDuration(elapsed),
		formatNumber(s.tasksEnqueued), formatNumber(s.enqueueRequests), formatNumber(s.enqueueErrors), formatNumber(uint64(enqueueRPS)),
		formatNumber(s.tasksHandled), formatNumber(s.handlerErrors), formatNumber(uint64(consumeRPS)),
		enqueueSuccessRate, handlerSuccessRate,
	)
}

// formatDuration formats a duration as HH:MM:SS.
func formatDuration(d time.Duration) string {
	totalSeconds := int(d.Seconds())
	hours := totalSeconds / 3600
	minutes := (totalSeconds % 3600) / 60
	seconds := totalSeconds % 60
	return fmt.Sprintf("%02d:%02d:%02d", hours, minutes, seconds)
}

// formatNumber formats a number with thousand separators.
func formatNumber(n uint64) string {
	if n < 1000 {
		return fmt.Sprintf("%d", n)
	}
	if n < 1000000 {
		return fmt.Sprintf("%d,%03d", n/1000, n%1000)
	}
	return fmt.Sprintf("%d,%03d,%03d", n/1000000, (n%1000000)/1000, n%1000)
}
