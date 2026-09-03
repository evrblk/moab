package moab

type ServiceLimits struct {
	MaxNumberOfQueues             int64
	MaxNumberOfSchedulesPerQueue  int64
	MaxNumberOfSchedules          int64
	EnqueuePerQueueRequestRate    int64
	DequeuePerQueueRequestRate    int64
	MaxEnqueueBatchSize           int32
	MaxDequeueBatchSize           int32
	ControlPlaneReadRequestRate   int64
	ControlPlaneUpdateRequestRate int64
	DataPlaneRequestRate          int64
	PurgeQueueRequestRate         int64
	// MaxScheduledDelayInSeconds bounds how far into the future a caller may
	// set ScheduledAt (Enqueue/RestartTasks). Violations are rejected, not
	// clamped: an absurdly-far ScheduledAt is far more likely a client bug
	// (e.g. a millis/nanos unit mixup) than a deliberate request, and
	// silently clamping it would run the task at a time the caller never
	// asked for.
	MaxScheduledDelayInSeconds int64
}

var (
	DefaultServiceLimits = ServiceLimits{
		MaxNumberOfQueues:             10_000,
		MaxNumberOfSchedulesPerQueue:  1_000,
		MaxNumberOfSchedules:          1_000,
		EnqueuePerQueueRequestRate:    10_000,
		DequeuePerQueueRequestRate:    10_000,
		MaxEnqueueBatchSize:           25,
		MaxDequeueBatchSize:           25,
		ControlPlaneReadRequestRate:   10_000,
		ControlPlaneUpdateRequestRate: 1_000,
		DataPlaneRequestRate:          20_000,
		PurgeQueueRequestRate:         1,
		// Same ceiling as pkg/server/v0/validators.go's maxExpiresTimeoutInSeconds
		// (14 days) — kept as a literal here to avoid an import cycle
		// (pkg/server/v0 already imports pkg/moab).
		MaxScheduledDelayInSeconds: 14 * 86400,
	}
)
