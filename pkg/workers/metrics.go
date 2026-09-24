package workers

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	moabQueuesCronWorkerDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:                            "moab_queues_cron_worker_duration_seconds",
		Help:                            "Moab Queues Cron Worker duration",
		NativeHistogramBucketFactor:     1.1,
		NativeHistogramMaxBucketNumber:  100,
		NativeHistogramMinResetDuration: time.Hour,
	}, []string{"shard_id"})
	moabQueuesCronWorkerSchedulesTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "moab_queues_cron_worker_schedules_total",
		Help: "Moab Queues Cron Worker total amount of schedules processed",
	}, []string{"shard_id"})
	moabQueuesCronWorkerErrorsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "moab_queues_cron_worker_errors_total",
		Help: "Moab Queues Cron Worker total amount of errors",
	}, []string{"shard_id"})

	moabTasksGCWorkerDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:                            "moab_tasks_gc_worker_duration_seconds",
		Help:                            "Moab Tasks Garbage Collection Worker duration",
		NativeHistogramBucketFactor:     1.1,
		NativeHistogramMaxBucketNumber:  100,
		NativeHistogramMinResetDuration: time.Hour,
	}, []string{"shard_id"})
	moabTasksGCWorkerErrorsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "moab_tasks_gc_worker_errors_total",
		Help: "Moab Tasks Garbage Collection Worker total amount of errors",
	}, []string{"shard_id"})

	moabQueuesGCWorkerDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:                            "moab_queues_gc_worker_duration_seconds",
		Help:                            "Moab Queues Garbage Collection Worker duration",
		NativeHistogramBucketFactor:     1.1,
		NativeHistogramMaxBucketNumber:  100,
		NativeHistogramMinResetDuration: time.Hour,
	}, []string{"shard_id"})
	moabQueuesGCWorkerErrorsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "moab_queues_gc_worker_errors_total",
		Help: "Moab Queues Garbage Collection Worker total amount of errors",
	}, []string{"shard_id"})
)

// RegisterMetrics registers the worker metrics with the given registerer.
// Call once at startup, e.g. RegisterMetrics(prometheus.DefaultRegisterer).
// It panics if a metric is already registered.
func RegisterMetrics(registerer prometheus.Registerer) {
	registerer.MustRegister(moabQueuesCronWorkerDuration)
	registerer.MustRegister(moabQueuesCronWorkerSchedulesTotal)
	registerer.MustRegister(moabQueuesCronWorkerErrorsTotal)

	registerer.MustRegister(moabTasksGCWorkerDuration)
	registerer.MustRegister(moabTasksGCWorkerErrorsTotal)

	registerer.MustRegister(moabQueuesGCWorkerDuration)
	registerer.MustRegister(moabQueuesGCWorkerErrorsTotal)
}
