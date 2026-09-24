package v0

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	tasksEnqueuedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "moab_tasks_enqueued_total",
		Help: "Moab tasks enqueued total",
	})
	tasksDequeuedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "moab_tasks_dequeued_total",
		Help: "Moab tasks dequeued total",
	})
	tasksEnqueuedBytesTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "moab_tasks_enqueued_bytes_total",
		Help: "Moab tasks enqueued total size",
	})
	tasksDequeuedBytesTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "moab_bytes_dequeued_bytes_total",
		Help: "Moab tasks dequeued total size",
	})
)

// RegisterMetrics registers the server metrics with the given registerer.
// Call once at startup, e.g. RegisterMetrics(prometheus.DefaultRegisterer).
// It panics if a metric is already registered.
func RegisterMetrics(registerer prometheus.Registerer) {
	registerer.MustRegister(tasksEnqueuedTotal)
	registerer.MustRegister(tasksDequeuedTotal)
	registerer.MustRegister(tasksEnqueuedBytesTotal)
	registerer.MustRegister(tasksDequeuedBytesTotal)
}
