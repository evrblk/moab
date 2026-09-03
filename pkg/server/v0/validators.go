package v0

import (
	"fmt"
	"regexp"
	"time"

	"github.com/adhocore/gronx"

	moabpb "github.com/evrblk/evrblk-go/moab/v0"
	"github.com/evrblk/moab/pkg/ids"
)

const (
	maxNameLength        = 128
	maxDescriptionLength = 1024

	maxDedupeKeyLength = 256

	maxKeepaliveTimeoutInSeconds = 60
	minKeepaliveTimeoutInSeconds = 5

	maxExpiresTimeoutInSeconds = 14 * 86400
	minExpiresTimeoutInSeconds = 60

	maxPayloadSize = 64 * 1024

	maxNumberOfRetryIntervalsInSeconds = 21
	maxRetryIntervalInSeconds          = 60 * 15

	maxNumberOfTaskIds = 50

	maxNumberOfEnqueueRequestEntries = 50

	maxNumberOfReportStatusEntries = 50

	maxDLQRetentionPeriod = 14 * 86400
)

var (
	nameRegex = regexp.MustCompile("^[-_0-9a-zA-Z]*$")
)

func ValidateEnqueueRequest(req *moabpb.EnqueueRequest) error {
	if err := validateQueueName(req.QueueName, "EnqueueRequest.QueueName"); err != nil {
		return err
	}

	if len(req.Entries) == 0 {
		return invalid("EnqueueRequest.Entries", "must not be empty")
	}

	if len(req.Entries) > maxNumberOfEnqueueRequestEntries {
		return invalid("EnqueueRequest.Entries", fmt.Sprintf("exceeds max number of entries (%d)", maxNumberOfEnqueueRequestEntries))
	}

	for i, e := range req.Entries {
		if e.KeepaliveTimeoutInSeconds != 0 {
			if err := validateKeepaliveTimeoutInSeconds(e.KeepaliveTimeoutInSeconds, fmt.Sprintf("EnqueueRequest.Entries[%d].KeepaliveTimeoutInSeconds", i)); err != nil {
				return err
			}
		}

		if len(e.DedupeKey) > maxDedupeKeyLength {
			return invalid(fmt.Sprintf("EnqueueRequest.Entries[%d].DedupeKey", i), fmt.Sprintf("exceeds max length (%d)", maxDedupeKeyLength))
		}

		if err := validateRetryStrategy(e.RetryStrategy, fmt.Sprintf("EnqueueRequest.Entries[%d].RetryStrategy", i)); err != nil {
			return err
		}

		if e.ExpiresAt < 0 {
			return invalid(fmt.Sprintf("EnqueueRequest.Entries[%d].ExpiresAt", i), "must be non-negative")
		}

		if e.ScheduledAt < 0 {
			return invalid(fmt.Sprintf("EnqueueRequest.Entries[%d].ScheduledAt", i), "must be non-negative")
		}

		if len(e.Payload) > maxPayloadSize {
			return invalid(fmt.Sprintf("EnqueueRequest.Entries[%d].Payload", i), fmt.Sprintf("exceeds max payload size (%d bytes)", maxPayloadSize))
		}
	}

	return nil
}

func ValidateDequeueRequest(req *moabpb.DequeueRequest) error {
	if err := validateQueueName(req.QueueName, "DequeueRequest.QueueName"); err != nil {
		return err
	}

	if req.BatchSize <= 0 {
		return invalid("DequeueRequest.BatchSize", "must be greater than 0")
	}

	return nil
}

func ValidatePurgeQueueRequest(req *moabpb.PurgeQueueRequest) error {
	if err := validateQueueName(req.QueueName, "PurgeQueueRequest.QueueName"); err != nil {
		return err
	}

	return nil
}

func ValidateGetTaskRequest(req *moabpb.GetTaskRequest) error {
	if err := validateQueueName(req.QueueName, "GetTaskRequest.QueueName"); err != nil {
		return err
	}

	if _, err := ids.DecodeTaskId(req.TaskId); err != nil {
		return invalid("GetTaskRequest.TaskId", "")
	}

	return nil
}

func ValidateReportStatusRequest(req *moabpb.ReportStatusRequest) error {
	if err := validateQueueName(req.QueueName, "ReportStatusRequest.QueueName"); err != nil {
		return err
	}

	if len(req.Entries) == 0 {
		return invalid("ReportStatusRequest.Entries", "must not be empty")
	}

	if len(req.Entries) > maxNumberOfReportStatusEntries {
		return invalid("ReportStatusRequest.Entries", fmt.Sprintf("exceeds max number of entries (%d)", maxNumberOfReportStatusEntries))
	}

	for i, e := range req.Entries {
		if err := validateTaskId(e.TaskId, fmt.Sprintf("ReportStatusRequest.Entries[%d].TaskId", i)); err != nil {
			return err
		}

		if e.Attempt <= 0 {
			return invalid(fmt.Sprintf("ReportStatusRequest.Entries[%d].Attempt", i), "must be greater than 0")
		}
	}

	return nil
}

func ValidateDeleteTasksRequest(req *moabpb.DeleteTasksRequest) error {
	if err := validateQueueName(req.QueueName, "DeleteTasksRequest.QueueName"); err != nil {
		return err
	}

	if err := validateTaskIds(req.TaskIds, "DeleteTasksRequest.TaskIds"); err != nil {
		return err
	}

	return nil
}

func ValidateRestartTasksRequest(req *moabpb.RestartTasksRequest) error {
	if err := validateQueueName(req.QueueName, "RestartTasksRequest.QueueName"); err != nil {
		return err
	}

	if len(req.Entries) < 1 || len(req.Entries) > maxNumberOfTaskIds {
		return invalid("RestartTasksRequest.Entries", fmt.Sprintf("must be between 1 and %d", maxNumberOfTaskIds))
	}

	seen := make(map[string]struct{})
	for i, e := range req.Entries {
		fieldName := fmt.Sprintf("RestartTasksRequest.Entries[%d]", i)

		if _, ok := seen[e.TaskId]; ok {
			return invalid(fieldName+".TaskId", fmt.Sprintf("task ids must be unique, %s is duplicated", e.TaskId))
		}
		seen[e.TaskId] = struct{}{}

		if err := validateTaskId(e.TaskId, fieldName+".TaskId"); err != nil {
			return err
		}

		if e.ScheduledAt < 0 {
			return invalid(fieldName+".ScheduledAt", "must be non-negative")
		}

		if e.ExpiresAt < 0 {
			return invalid(fieldName+".ExpiresAt", "must be non-negative")
		}
	}

	return nil
}

func ValidateListQueuesRequest(_ *moabpb.ListQueuesRequest) error {
	return nil
}

func ValidateGetQueueRequest(req *moabpb.GetQueueRequest) error {
	if err := validateQueueName(req.QueueName, "GetQueueRequest.QueueName"); err != nil {
		return err
	}

	return nil
}

func ValidateCreateQueueRequest(req *moabpb.CreateQueueRequest) error {
	if err := validateQueueName(req.Name, "CreateQueueRequest.Name"); err != nil {
		return err
	}

	if err := validateDescription(req.Description, "CreateQueueRequest.Description"); err != nil {
		return err
	}

	if err := validateDequeuingSettings(req.DequeuingSettings, "CreateQueueRequest.DequeuingSettings"); err != nil {
		return err
	}

	if err := validateDeadLetterQueueConfig(req.DeadLetterQueueConfig, "CreateQueueRequest.DeadLetterQueueConfig"); err != nil {
		return err
	}

	if err := validateRetryStrategy(req.RetryStrategy, "CreateQueueRequest.RetryStrategy"); err != nil {
		return err
	}

	if err := validateKeepaliveTimeoutInSeconds(req.KeepaliveTimeoutInSeconds, "CreateQueueRequest.KeepaliveTimeoutInSeconds"); err != nil {
		return err
	}

	if err := validateExpiresInSeconds(req.ExpiresInSeconds, "CreateQueueRequest.ExpiresInSeconds"); err != nil {
		return err
	}

	return nil
}

func ValidateUpdateQueueRequest(req *moabpb.UpdateQueueRequest) error {
	if err := validateQueueName(req.QueueName, "UpdateQueueRequest.QueueName"); err != nil {
		return err
	}

	if err := validateDescription(req.Description, "UpdateQueueRequest.Description"); err != nil {
		return err
	}

	if err := validateRetryStrategy(req.RetryStrategy, "UpdateQueueRequest.RetryStrategy"); err != nil {
		return err
	}

	if err := validateDequeuingSettings(req.DequeuingSettings, "UpdateQueueRequest.DequeuingSettings"); err != nil {
		return err
	}

	if err := validateDeadLetterQueueConfig(req.DeadLetterQueueConfig, "UpdateQueueRequest.DeadLetterQueueConfig"); err != nil {
		return err
	}

	if err := validateExpiresInSeconds(req.ExpiresInSeconds, "UpdateQueueRequest.ExpiresInSeconds"); err != nil {
		return err
	}

	if err := validateKeepaliveTimeoutInSeconds(req.KeepaliveTimeoutInSeconds, "UpdateQueueRequest.KeepaliveTimeoutInSeconds"); err != nil {
		return err
	}

	if req.ExpectedVersion <= 0 {
		return invalid("UpdateQueueRequest.ExpectedVersion", "must be greater than 0")
	}

	return nil
}

func ValidateDeleteQueueRequest(req *moabpb.DeleteQueueRequest) error {
	if err := validateQueueName(req.QueueName, "DeleteQueueRequest.QueueName"); err != nil {
		return err
	}

	return nil
}

func ValidateGetScheduleRequest(req *moabpb.GetScheduleRequest) error {
	if err := validateQueueName(req.QueueName, "GetScheduleRequest.QueueName"); err != nil {
		return err
	}

	if err := validateScheduleName(req.ScheduleName, "GetScheduleRequest.ScheduleName"); err != nil {
		return err
	}

	return nil
}

func ValidateListSchedulesRequest(req *moabpb.ListSchedulesRequest) error {
	if err := validateQueueName(req.QueueName, "ListSchedulesRequest.QueueName"); err != nil {
		return err
	}

	return nil
}

func ValidateCreateScheduleRequest(req *moabpb.CreateScheduleRequest) error {
	if err := validateQueueName(req.QueueName, "CreateScheduleRequest.QueueName"); err != nil {
		return err
	}

	if err := validateScheduleName(req.Name, "CreateScheduleRequest.Name"); err != nil {
		return err
	}

	if err := validateDescription(req.Description, "CreateScheduleRequest.Description"); err != nil {
		return err
	}

	if err := validateCron(req.Cron, "CreateScheduleRequest.Cron"); err != nil {
		return err
	}

	if len(req.DedupeKey) > maxDedupeKeyLength {
		return invalid("CreateScheduleRequest.DedupeKey", fmt.Sprintf("exceeds max length (%d)", maxDedupeKeyLength))
	}

	if len(req.Payload) > maxPayloadSize {
		return invalid("CreateScheduleRequest.Payload", fmt.Sprintf("exceeds max size (%d bytes)", maxPayloadSize))
	}

	if req.KeepaliveTimeoutInSeconds != 0 {
		if err := validateKeepaliveTimeoutInSeconds(req.KeepaliveTimeoutInSeconds, "CreateScheduleRequest.KeepaliveTimeoutInSeconds"); err != nil {
			return err
		}
	}

	if req.ExpiresInSeconds != 0 {
		if err := validateExpiresInSeconds(req.ExpiresInSeconds, "CreateScheduleRequest.ExpiresInSeconds"); err != nil {
			return err
		}
	}

	if err := validateRetryStrategy(req.RetryStrategy, "CreateScheduleRequest.RetryStrategy"); err != nil {
		return err
	}

	if err := validateTimezone(req.Timezone, "CreateScheduleRequest.Timezone"); err != nil {
		return err
	}

	return nil
}

func ValidateUpdateScheduleRequest(req *moabpb.UpdateScheduleRequest) error {
	if err := validateQueueName(req.QueueName, "UpdateScheduleRequest.QueueName"); err != nil {
		return err
	}

	if err := validateScheduleName(req.ScheduleName, "UpdateScheduleRequest.ScheduleName"); err != nil {
		return err
	}

	if err := validateDescription(req.Description, "UpdateScheduleRequest.Description"); err != nil {
		return err
	}

	if req.KeepaliveTimeoutInSeconds != 0 {
		if err := validateKeepaliveTimeoutInSeconds(req.KeepaliveTimeoutInSeconds, "UpdateScheduleRequest.KeepaliveTimeoutInSeconds"); err != nil {
			return err
		}
	}

	if req.ExpiresInSeconds != 0 {
		if err := validateExpiresInSeconds(req.ExpiresInSeconds, "UpdateScheduleRequest.ExpiresInSeconds"); err != nil {
			return err
		}
	}

	if err := validateRetryStrategy(req.RetryStrategy, "UpdateScheduleRequest.RetryStrategy"); err != nil {
		return err
	}

	if len(req.Payload) > maxPayloadSize {
		return invalid("UpdateScheduleRequest.Payload", fmt.Sprintf("exceeds max size (%d bytes)", maxPayloadSize))
	}

	if err := validateCron(req.Cron, "UpdateScheduleRequest.Cron"); err != nil {
		return err
	}

	if len(req.DedupeKey) > maxDedupeKeyLength {
		return invalid("UpdateScheduleRequest.DedupeKey", fmt.Sprintf("exceeds max length (%d)", maxDedupeKeyLength))
	}

	if err := validateTimezone(req.Timezone, "UpdateScheduleRequest.Timezone"); err != nil {
		return err
	}

	if req.ExpectedVersion <= 0 {
		return invalid("UpdateScheduleRequest.ExpectedVersion", "must be greater than 0")
	}

	return nil
}

func ValidateDeleteScheduleRequest(req *moabpb.DeleteScheduleRequest) error {
	if err := validateQueueName(req.QueueName, "DeleteScheduleRequest.QueueName"); err != nil {
		return err
	}

	if err := validateScheduleName(req.ScheduleName, "DeleteScheduleRequest.ScheduleName"); err != nil {
		return err
	}

	return nil
}

func validateTimezone(value string, fieldName string) error {
	if value == "" {
		return invalid(fieldName, "must not be empty")
	}

	_, err := time.LoadLocation(value)
	if err != nil {
		return invalid(fieldName, "invalid timezone")
	}

	return nil
}

func validateCron(value string, fieldName string) error {
	gron := gronx.New()
	if !gron.IsValid(value) {
		return invalid(fieldName, "invalid cron expression")
	}

	return nil
}

func validateKeepaliveTimeoutInSeconds(value int64, fieldName string) error {
	if value < minKeepaliveTimeoutInSeconds || value > maxKeepaliveTimeoutInSeconds {
		return invalid(fieldName, fmt.Sprintf("must be between %d and %d seconds", minKeepaliveTimeoutInSeconds, maxKeepaliveTimeoutInSeconds))
	}

	return nil
}

func validateQueueName(value string, fieldName string) error {
	return validateString(value, 1, maxNameLength, nameRegex, fieldName)
}

func validateScheduleName(value string, fieldName string) error {
	return validateString(value, 1, maxNameLength, nameRegex, fieldName)
}

func validateDescription(value string, fieldName string) error {
	if len(value) > maxDescriptionLength {
		return invalid(fieldName, fmt.Sprintf("exceeds max length (%d)", maxDescriptionLength))
	}

	return nil
}

func validateString(value string, minLength int, maxLength int, regex *regexp.Regexp, fieldName string) error {
	if len(value) > maxLength || len(value) < minLength {
		return invalid(fieldName, fmt.Sprintf("length must be between %d and %d characters", minLength, maxLength))
	}

	if !regex.Match([]byte(value)) {
		return invalid(fieldName, "must match regex pattern "+regex.String())
	}

	return nil
}

func validateExpiresInSeconds(value int64, fieldName string) error {
	if value < minExpiresTimeoutInSeconds || value > maxExpiresTimeoutInSeconds {
		return invalid(fieldName, fmt.Sprintf("must be between %d and %d seconds", minExpiresTimeoutInSeconds, maxExpiresTimeoutInSeconds))
	}

	return nil
}

func validateRetryStrategy(value *moabpb.RetryStrategy, fieldName string) error {
	if value == nil {
		return nil
	}

	if value.RetryIntervalsInSeconds != nil {
		if len(value.RetryIntervalsInSeconds) > maxNumberOfRetryIntervalsInSeconds {
			return invalid(fmt.Sprintf("%s.RetryIntervalsInSeconds", fieldName), fmt.Sprintf("exceeds max length (%d)", maxNumberOfRetryIntervalsInSeconds))
		}

		for i := range value.RetryIntervalsInSeconds {
			if value.RetryIntervalsInSeconds[i] < 0 || value.RetryIntervalsInSeconds[i] > maxRetryIntervalInSeconds {
				return invalid(fmt.Sprintf("%s.RetryIntervalsInSeconds[%d]", fieldName, i), fmt.Sprintf("must be between 0 and %d seconds", maxRetryIntervalInSeconds))
			}
		}
	}

	return nil
}

func validateTaskIds(value []string, fieldName string) error {
	if len(value) < 1 || len(value) > maxNumberOfTaskIds {
		return invalid(fieldName, fmt.Sprintf("task ids must be between 1 and %d", maxNumberOfTaskIds))
	}

	taskIds := make(map[string]struct{})
	for i, id := range value {
		if _, ok := taskIds[id]; ok {
			return invalid(fieldName, fmt.Sprintf("task ids must be unique, %s is duplicated", id))
		}
		taskIds[id] = struct{}{}

		if err := validateTaskId(id, fmt.Sprintf("%s[%d]", fieldName, i)); err != nil {
			return err
		}
	}

	return nil
}

func validateTaskId(value string, fieldName string) error {
	_, err := ids.DecodeTaskId(value)
	if err != nil {
		return invalid(fieldName, "invalid task id")
	}

	return nil
}

func validateDequeuingSettings(value *moabpb.DequeuingSettings, fieldName string) error {
	if value == nil {
		return nil
	}

	if value.MaxInProgressTasks < 0 {
		return invalid(fmt.Sprintf("%s.MaxInProgressTasks", fieldName), "max in progress tasks must be non-negative")
	}

	if value.RateLimiting != nil {
		if value.RateLimiting.Interval < 0 {
			return invalid(fmt.Sprintf("%s.RateLimiting.Interval", fieldName), "rate limiting interval must be non-negative")
		}

		if value.RateLimiting.MaxTokens < 0 {
			return invalid(fmt.Sprintf("%s.RateLimiting.MaxTokens", fieldName), "rate limiting max tokens must be non-negative")
		}

		if value.RateLimiting.IntervalUnit == moabpb.IntervalUnit_INTERVAL_UNIT_INVALID {
			return invalid(fmt.Sprintf("%s.RateLimiting.IntervalUnit", fieldName), "rate limiting interval unit must be set")
		}
	}

	return nil
}

func validateDeadLetterQueueConfig(value *moabpb.DeadLetterQueueConfig, fieldName string) error {
	if value == nil {
		return nil
	}

	if value.MaxSize < 0 {
		return invalid(fmt.Sprintf("%s.MaxSize", fieldName), "max size must be non-negative")
	}

	if value.RetentionPeriodInSeconds < minExpiresTimeoutInSeconds || value.RetentionPeriodInSeconds > maxDLQRetentionPeriod {
		return invalid(fmt.Sprintf("%s.RetentionPeriodInSeconds", fieldName), fmt.Sprintf("retention period must be between %d and %d seconds", minExpiresTimeoutInSeconds, maxDLQRetentionPeriod))
	}

	return nil
}

func invalid(fieldName string, details string) error {
	if details == "" {
		return fmt.Errorf("invalid %s", fieldName)
	} else {
		return fmt.Errorf("invalid %s: %s", fieldName, details)
	}
}
