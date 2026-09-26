//lint:file-ignore ST1005 field names are capitalized
package corepb

import (
	"fmt"
	"time"

	"github.com/adhocore/gronx"
)

// Bounds mirror the front-end validators in pkg/server/v0/validators.go.
// They are duplicated rather than imported (server/v0 depends on corepb,
// not the other way around) and are deliberately limited to fields the
// cores actually dereference or use in arithmetic — not, e.g., description
// length, which no core method reads.
// TODO: think how to share some validation logic/consts between cores and
// server gateways.
const (
	maxKeepaliveTimeoutInSeconds = 60
	minKeepaliveTimeoutInSeconds = 5

	maxExpiresTimeoutInSeconds = 14 * 86400
	minExpiresTimeoutInSeconds = 60

	maxDLQRetentionPeriod = 14 * 86400

	maxPayloadSize = 64 * 1024

	maxNumberOfRetryIntervalsInSeconds = 21
	maxRetryIntervalInSeconds          = 60 * 15

	maxNumberOfTaskIds               = 50
	maxNumberOfEnqueueRequestEntries = 50
	maxNumberOfReportStatusEntries   = 50
)

// CreateQueueRequest

func (r *CreateQueueRequest) Validate() error {
	if err := validateQueueId(r.QueueId); err != nil {
		return err
	}

	if err := validateExpiresInSeconds(r.ExpiresInSeconds); err != nil {
		return err
	}

	if err := validateKeepaliveTimeoutInSeconds(r.KeepaliveTimeoutInSeconds); err != nil {
		return err
	}

	if err := validateRetryStrategy(r.RetryStrategy); err != nil {
		return err
	}

	if err := validateDequeuingSettings(r.DequeuingSettings); err != nil {
		return err
	}

	if err := validateDeadLetterQueueConfig(r.DeadLetterQueueConfig); err != nil {
		return err
	}

	return nil
}

// CreateScheduleRequest

func (r *CreateScheduleRequest) Validate() error {
	if !gronx.New().IsValid(r.Cron) {
		return fmt.Errorf("invalid cron expression: %q", r.Cron)
	}

	if _, err := time.LoadLocation(r.Timezone); err != nil {
		return fmt.Errorf("invalid timezone: %q", r.Timezone)
	}

	if r.ExpiresInSeconds != 0 {
		if err := validateExpiresInSeconds(r.ExpiresInSeconds); err != nil {
			return err
		}
	}

	if err := validateRetryStrategy(r.RetryStrategy); err != nil {
		return err
	}

	if len(r.Payload) > maxPayloadSize {
		return fmt.Errorf("Payload exceeds max size (%d bytes)", maxPayloadSize)
	}

	return nil
}

// DeleteQueueRequest

// AccountId/QueueName are resolved by a lookup in Core; there is no nested
// id or timestamp here for a malformed value to corrupt.
func (r *DeleteQueueRequest) Validate() error {
	return nil
}

// DeleteScheduleRequest

func (r *DeleteScheduleRequest) Validate() error {
	return nil
}

// DeleteTasksRequest

func (r *DeleteTasksRequest) Validate() error {
	if err := validateQueueId(r.QueueId); err != nil {
		return err
	}

	if len(r.TaskIds) > maxNumberOfTaskIds {
		return fmt.Errorf("TaskIds exceeds max length (%d)", maxNumberOfTaskIds)
	}

	return nil
}

// DequeSchedulesRequest

// An internal cron-tick trigger, not user-facing: DueBefore is only ever
// used as a scan bound, so any value is safe to evaluate.
func (r *DequeSchedulesRequest) Validate() error {
	return nil
}

// DequeueRequest

func (r *DequeueRequest) Validate() error {
	if err := validateQueueId(r.QueueId); err != nil {
		return err
	}

	if err := validateDequeuingSettings(r.DequeuingSettings); err != nil {
		return err
	}

	if err := validateDeadLetterQueueConfig(r.DeadLetterQueueConfig); err != nil {
		return err
	}

	if r.KeepaliveTimeoutInSeconds != 0 {
		if err := validateKeepaliveTimeoutInSeconds(r.KeepaliveTimeoutInSeconds); err != nil {
			return err
		}
	}

	return nil
}

// EnqueueRequest

func (r *EnqueueRequest) Validate() error {
	if err := validateQueueId(r.QueueId); err != nil {
		return err
	}

	if len(r.Entries) > maxNumberOfEnqueueRequestEntries {
		return fmt.Errorf("Entries exceeds max number of entries (%d)", maxNumberOfEnqueueRequestEntries)
	}

	for i, e := range r.Entries {
		if e.ScheduledAt < 0 {
			return fmt.Errorf("Entries[%d].ScheduledAt must be non-negative", i)
		}

		if e.ExpiresAt < 0 {
			return fmt.Errorf("Entries[%d].ExpiresAt must be non-negative", i)
		}

		if err := validateRetryStrategy(e.RetryStrategy); err != nil {
			return fmt.Errorf("Entries[%d].%w", i, err)
		}

		if len(e.Payload) > maxPayloadSize {
			return fmt.Errorf("Entries[%d].Payload exceeds max payload size (%d bytes)", i, maxPayloadSize)
		}
	}

	return nil
}

// GetQueueRequest

func (r *GetQueueRequest) Validate() error {
	return validateQueueId(r.QueueId)
}

// GetQueueByNameRequest

func (r *GetQueueByNameRequest) Validate() error {
	return nil
}

// GetScheduleRequest

func (r *GetScheduleRequest) Validate() error {
	return nil
}

// GetStatisticsRequest

func (r *GetStatisticsRequest) Validate() error {
	return validateQueueId(r.QueueId)
}

// GetTaskRequest

func (r *GetTaskRequest) Validate() error {
	return validateTaskId(r.TaskId)
}

// ListQueuesRequest

// Limit and PaginationToken are self-correcting (pagination.GetLimitWithDefaults
// falls back to a default for any value <= 0, and an unresolvable token just
// fails the lookup with an ordinary error) — nothing here a core method
// dereferences unconditionally.
func (r *ListQueuesRequest) Validate() error {
	return nil
}

// ListSchedulesRequest

func (r *ListSchedulesRequest) Validate() error {
	return validateQueueId(r.QueueId)
}

// ListTasksRequest

func (r *ListTasksRequest) Validate() error {
	return validateQueueId(r.QueueId)
}

// PurgeQueueRequest

// QueueId is stored verbatim into a PurgeQueueGarbageCollectionRecord and
// only dereferenced later, by RunPurgeQueueGarbageCollection — a nil value
// would not panic here, but would panic on whichever GC tick eventually
// drains this record.
func (r *PurgeQueueRequest) Validate() error {
	return validateQueueId(r.QueueId)
}

// ReportSchedulesStatusRequest

func (r *ReportSchedulesStatusRequest) Validate() error {
	if err := validateScheduleId(r.ScheduleId); err != nil {
		return err
	}

	if r.NextScheduledAt < 0 {
		return fmt.Errorf("NextScheduledAt must be non-negative")
	}

	if r.LastEnqueuedFor < 0 {
		return fmt.Errorf("LastEnqueuedFor must be non-negative")
	}

	return nil
}

// ReportStatusRequest

func (r *ReportStatusRequest) Validate() error {
	if len(r.Entries) > maxNumberOfReportStatusEntries {
		return fmt.Errorf("Entries exceeds max number of entries (%d)", maxNumberOfReportStatusEntries)
	}

	for i, e := range r.Entries {
		if err := validateTaskId(e.TaskId); err != nil {
			return fmt.Errorf("Entries[%d].%w", i, err)
		}

		if e.KeepaliveTimeoutInSeconds != 0 {
			if err := validateKeepaliveTimeoutInSeconds(e.KeepaliveTimeoutInSeconds); err != nil {
				return fmt.Errorf("Entries[%d].%w", i, err)
			}
		}
	}

	if err := validateDeadLetterQueueConfig(r.DeadLetterQueueConfig); err != nil {
		return err
	}

	return nil
}

// RestartTasksRequest

func (r *RestartTasksRequest) Validate() error {
	if err := validateQueueId(r.QueueId); err != nil {
		return err
	}

	if len(r.Entries) > maxNumberOfTaskIds {
		return fmt.Errorf("Entries exceeds max length (%d)", maxNumberOfTaskIds)
	}

	for i, e := range r.Entries {
		if e.ScheduledAt < 0 {
			return fmt.Errorf("Entries[%d].ScheduledAt must be non-negative", i)
		}

		if e.ExpiresAt < 0 {
			return fmt.Errorf("Entries[%d].ExpiresAt must be non-negative", i)
		}
	}

	return nil
}

// RunPurgeQueueGarbageCollectionRequest

// Every field is a page/budget knob that Core itself defaults whenever it
// is <= 0; there is nothing a bad value can corrupt.
func (r *RunPurgeQueueGarbageCollectionRequest) Validate() error {
	return nil
}

// RunQueuesGarbageCollectionRequest

func (r *RunQueuesGarbageCollectionRequest) Validate() error {
	return nil
}

// RunTasksGarbageCollectionRequest

func (r *RunTasksGarbageCollectionRequest) Validate() error {
	return nil
}

// SwapQueueIdRequest

// NewQueueId is a caller-generated candidate id, checked for collision by
// Core itself; there is no value that is structurally wrong.
func (r *SwapQueueIdRequest) Validate() error {
	return nil
}

// UpdateQueueRequest

func (r *UpdateQueueRequest) Validate() error {
	if err := validateExpiresInSeconds(r.ExpiresInSeconds); err != nil {
		return err
	}

	if err := validateKeepaliveTimeoutInSeconds(r.KeepaliveTimeoutInSeconds); err != nil {
		return err
	}

	if err := validateRetryStrategy(r.RetryStrategy); err != nil {
		return err
	}

	if err := validateDequeuingSettings(r.DequeuingSettings); err != nil {
		return err
	}

	if err := validateDeadLetterQueueConfig(r.DeadLetterQueueConfig); err != nil {
		return err
	}

	return nil
}

// UpdateScheduleRequest

func (r *UpdateScheduleRequest) Validate() error {
	if !gronx.New().IsValid(r.Cron) {
		return fmt.Errorf("invalid cron expression: %q", r.Cron)
	}

	if _, err := time.LoadLocation(r.Timezone); err != nil {
		return fmt.Errorf("invalid timezone: %q", r.Timezone)
	}

	if r.ExpiresInSeconds != 0 {
		if err := validateExpiresInSeconds(r.ExpiresInSeconds); err != nil {
			return err
		}
	}

	if err := validateRetryStrategy(r.RetryStrategy); err != nil {
		return err
	}

	if len(r.Payload) > maxPayloadSize {
		return fmt.Errorf("Payload exceeds max size (%d bytes)", maxPayloadSize)
	}

	return nil
}

// validateQueueId, validateTaskId, and validateScheduleId require the id to
// be set and every field but AccountId to be positive. AccountId is legitimately
// zero in the single-tenant OSS deployment (there are no accounts to assign
// it from), but QueueId/TaskId/ScheduleId are always assigned by
// CreateQueue/Enqueue/CreateSchedule — zero here can only mean a field that
// was never set, not a real id.

func validateQueueId(id *QueueId) error {
	if id == nil {
		return fmt.Errorf("QueueId must be set")
	}

	if id.QueueId == 0 {
		return fmt.Errorf("QueueId.QueueId must be positive")
	}

	return nil
}

func validateTaskId(id *TaskId) error {
	if id == nil {
		return fmt.Errorf("TaskId must be set")
	}

	if id.QueueId == 0 {
		return fmt.Errorf("TaskId.QueueId must be positive")
	}

	if id.TaskId == 0 {
		return fmt.Errorf("TaskId.TaskId must be positive")
	}

	return nil
}

func validateScheduleId(id *ScheduleId) error {
	if id == nil {
		return fmt.Errorf("ScheduleId must be set")
	}

	if id.QueueId == 0 {
		return fmt.Errorf("ScheduleId.QueueId must be positive")
	}

	if id.ScheduleId == 0 {
		return fmt.Errorf("ScheduleId.ScheduleId must be positive")
	}

	return nil
}

func validateExpiresInSeconds(value int64) error {
	if value < minExpiresTimeoutInSeconds || value > maxExpiresTimeoutInSeconds {
		return fmt.Errorf("ExpiresInSeconds must be between %d and %d seconds", minExpiresTimeoutInSeconds, maxExpiresTimeoutInSeconds)
	}

	return nil
}

func validateKeepaliveTimeoutInSeconds(value int64) error {
	if value < minKeepaliveTimeoutInSeconds || value > maxKeepaliveTimeoutInSeconds {
		return fmt.Errorf("KeepaliveTimeoutInSeconds must be between %d and %d seconds", minKeepaliveTimeoutInSeconds, maxKeepaliveTimeoutInSeconds)
	}

	return nil
}

// validateRetryStrategy bounds the interval count and each interval's
// value: both are multiplied by time.Second (a task's ScheduledAt/VisibleAt
// arithmetic) with no further clamping downstream, so an extreme value here
// would otherwise carry through as a silently wrong, deterministically
// replicated schedule rather than a caught error.
func validateRetryStrategy(value *RetryStrategy) error {
	if value == nil {
		return nil
	}

	if len(value.RetryIntervalsInSeconds) > maxNumberOfRetryIntervalsInSeconds {
		return fmt.Errorf("RetryStrategy.RetryIntervalsInSeconds exceeds max length (%d)", maxNumberOfRetryIntervalsInSeconds)
	}

	for i, interval := range value.RetryIntervalsInSeconds {
		if interval < 0 || interval > maxRetryIntervalInSeconds {
			return fmt.Errorf("RetryStrategy.RetryIntervalsInSeconds[%d] must be between 0 and %d seconds", i, maxRetryIntervalInSeconds)
		}
	}

	return nil
}

// validateDequeuingSettings guards the one field Core actually branches on
// unconditionally: getRefillInterval switches on IntervalUnit with no
// default case and panics on an unrecognized value, once RateLimiting is
// set and its Interval/MaxTokens both go positive. Validating whenever
// RateLimiting is set at all, rather than only once it is "enabled", catches
// the bug before a later update to Interval/MaxTokens alone could trigger it.
func validateDequeuingSettings(value *DequeuingSettings) error {
	if value == nil {
		return nil
	}

	if value.MaxInProgressTasks < 0 {
		return fmt.Errorf("DequeuingSettings.MaxInProgressTasks must be non-negative")
	}

	if value.RateLimiting != nil {
		if value.RateLimiting.Interval < 0 {
			return fmt.Errorf("DequeuingSettings.RateLimiting.Interval must be non-negative")
		}

		if value.RateLimiting.MaxTokens < 0 {
			return fmt.Errorf("DequeuingSettings.RateLimiting.MaxTokens must be non-negative")
		}

		switch value.RateLimiting.IntervalUnit {
		case IntervalUnit_INTERVAL_UNIT_SECONDS,
			IntervalUnit_INTERVAL_UNIT_MINUTES,
			IntervalUnit_INTERVAL_UNIT_HOURS:
		default:
			return fmt.Errorf("DequeuingSettings.RateLimiting.IntervalUnit: unrecognized value")
		}
	}

	return nil
}

// validateDeadLetterQueueConfig enforces the invariant documented on the
// message itself: RetentionPeriodInSeconds is required whenever this
// message is set, regardless of Enable, so failTaskToDead's
// LastFailedAt + RetentionPeriodInSeconds*time.Second always computes a
// real deadline, never one derived from an unset or out-of-range value.
func validateDeadLetterQueueConfig(value *DeadLetterQueueConfig) error {
	if value == nil {
		return nil
	}

	if value.MaxSize < 0 {
		return fmt.Errorf("DeadLetterQueueConfig.MaxSize must be non-negative")
	}

	if value.RetentionPeriodInSeconds < minExpiresTimeoutInSeconds || value.RetentionPeriodInSeconds > maxDLQRetentionPeriod {
		return fmt.Errorf("DeadLetterQueueConfig.RetentionPeriodInSeconds must be between %d and %d seconds", minExpiresTimeoutInSeconds, maxDLQRetentionPeriod)
	}

	return nil
}
