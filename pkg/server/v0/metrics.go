package v0

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	totalRequestsCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "moab_server_requests_total",
		Help: "Total number of requests",
	}, []string{"method"})
	failedRequestsCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "moab_server_requests_failed",
		Help: "Number of failed requests",
	}, []string{"method", "error"})
	requestsDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:                            "moab_server_request_duration_seconds",
		Help:                            "Request duration",
		NativeHistogramBucketFactor:     1.1,
		NativeHistogramMaxBucketNumber:  100,
		NativeHistogramMinResetDuration: time.Hour,
	}, []string{"method"})
)

func RegisterMetrics() {
	prometheus.MustRegister(totalRequestsCounter)
	prometheus.MustRegister(failedRequestsCounter)
	prometheus.MustRegister(requestsDuration)
	prometheus.MustRegister(tasksEnqueuedTotal)
	prometheus.MustRegister(tasksDequeuedTotal)
	prometheus.MustRegister(tasksEnqueuedBytesTotal)
	prometheus.MustRegister(tasksDequeuedBytesTotal)
}
