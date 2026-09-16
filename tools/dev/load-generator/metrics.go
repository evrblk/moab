package main

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	// enqueueRequestsTotal tracks Enqueue RPCs by status.
	enqueueRequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "load_generator_enqueue_requests_total",
			Help: "Total number of Enqueue RPCs by status",
		},
		[]string{"status"},
	)

	// enqueueDuration tracks Enqueue RPC latency.
	enqueueDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:                            "load_generator_enqueue_duration_seconds",
			Help:                            "Enqueue RPC duration",
			NativeHistogramBucketFactor:     1.1,
			NativeHistogramMaxBucketNumber:  100,
			NativeHistogramMinResetDuration: 0,
		},
	)

	// tasksEnqueuedTotal tracks individual tasks actually enqueued (returned by
	// the server, i.e. excluding entries skipped due to deduplication).
	tasksEnqueuedTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "load_generator_tasks_enqueued_total",
			Help: "Total number of tasks actually enqueued (excludes deduped entries skipped by the server)",
		},
	)

	// enqueueErrorsTotal tracks Enqueue RPC errors by type.
	enqueueErrorsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "load_generator_enqueue_errors_total",
			Help: "Total number of Enqueue RPC errors by error type",
		},
		[]string{"error_type"},
	)

	// tasksHandledTotal tracks task handler invocations by status, as reported
	// back to Moab via MoabConsumer's ReportStatus.
	tasksHandledTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "load_generator_tasks_handled_total",
			Help: "Total number of tasks handled by consumers, by outcome",
		},
		[]string{"status"},
	)

	// handlerDuration tracks simulated task handler processing time.
	handlerDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:                            "load_generator_handler_duration_seconds",
			Help:                            "Simulated task handler processing time",
			NativeHistogramBucketFactor:     1.1,
			NativeHistogramMaxBucketNumber:  100,
			NativeHistogramMinResetDuration: 0,
		},
	)

	// activeProducers tracks the number of running producer goroutines.
	activeProducers = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "load_generator_active_producers",
			Help: "Number of active producer goroutines",
		},
	)

	// activeConsumers tracks the number of running MoabConsumer instances.
	activeConsumers = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "load_generator_active_consumers",
			Help: "Number of active MoabConsumer instances",
		},
	)

	// queuesGauge tracks the number of queues in use.
	queuesGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "load_generator_queues",
			Help: "Number of queues created for this run",
		},
	)
)

// RegisterMetrics registers all metrics with Prometheus.
func RegisterMetrics() {
	prometheus.MustRegister(enqueueRequestsTotal)
	prometheus.MustRegister(enqueueDuration)
	prometheus.MustRegister(tasksEnqueuedTotal)
	prometheus.MustRegister(enqueueErrorsTotal)
	prometheus.MustRegister(tasksHandledTotal)
	prometheus.MustRegister(handlerDuration)
	prometheus.MustRegister(activeProducers)
	prometheus.MustRegister(activeConsumers)
	prometheus.MustRegister(queuesGauge)
}
