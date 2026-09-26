package corepb

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func validQueueId() *QueueId {
	return &QueueId{AccountId: 1, QueueId: 2}
}

func TestValidateQueueId(t *testing.T) {
	require.Error(t, validateQueueId(nil))

	// AccountId == 0 is valid: the single-tenant OSS deployment has no
	// accounts to assign it from.
	require.NoError(t, validateQueueId(&QueueId{AccountId: 0, QueueId: 1}))

	require.Error(t, validateQueueId(&QueueId{AccountId: 1, QueueId: 0}))
}

func TestValidateTaskId(t *testing.T) {
	require.Error(t, validateTaskId(nil))
	require.NoError(t, validateTaskId(&TaskId{AccountId: 0, QueueId: 1, TaskId: 1}))
	require.Error(t, validateTaskId(&TaskId{AccountId: 1, QueueId: 0, TaskId: 1}))
	require.Error(t, validateTaskId(&TaskId{AccountId: 1, QueueId: 1, TaskId: 0}))
}

func TestValidateScheduleId(t *testing.T) {
	require.Error(t, validateScheduleId(nil))
	require.NoError(t, validateScheduleId(&ScheduleId{AccountId: 0, QueueId: 1, ScheduleId: 1}))
	require.Error(t, validateScheduleId(&ScheduleId{AccountId: 1, QueueId: 0, ScheduleId: 1}))
	require.Error(t, validateScheduleId(&ScheduleId{AccountId: 1, QueueId: 1, ScheduleId: 0}))
}

func TestCreateQueueRequest_Validate(t *testing.T) {
	valid := func() *CreateQueueRequest {
		return &CreateQueueRequest{
			QueueId:                   validQueueId(),
			Name:                      "test_queue",
			KeepaliveTimeoutInSeconds: 15,
			ExpiresInSeconds:          600,
		}
	}
	require.NoError(t, valid().Validate())

	req := valid()
	req.QueueId = nil
	require.Error(t, req.Validate())

	for _, v := range []int64{0, -1, 15 * 86400} {
		req = valid()
		req.ExpiresInSeconds = v
		require.Error(t, req.Validate())
	}

	for _, v := range []int64{0, 4, 61} {
		req = valid()
		req.KeepaliveTimeoutInSeconds = v
		require.Error(t, req.Validate())
	}

	req = valid()
	req.RetryStrategy = &RetryStrategy{RetryIntervalsInSeconds: []int64{-1}}
	require.Error(t, req.Validate())

	req = valid()
	req.DequeuingSettings = &DequeuingSettings{MaxInProgressTasks: -1}
	require.Error(t, req.Validate())

	req = valid()
	req.DequeuingSettings = &DequeuingSettings{RateLimiting: &TokenBucketRateLimiting{Interval: 1, MaxTokens: 1}}
	require.Error(t, req.Validate())

	req = valid()
	req.DeadLetterQueueConfig = &DeadLetterQueueConfig{Enable: true, RetentionPeriodInSeconds: 0}
	require.Error(t, req.Validate())
}

func TestCreateScheduleRequest_Validate(t *testing.T) {
	valid := func() *CreateScheduleRequest {
		return &CreateScheduleRequest{Cron: "*/5 * * * *", Timezone: "UTC"}
	}
	require.NoError(t, valid().Validate())

	req := valid()
	req.Cron = "this is not a cron expression at all!!"
	require.Error(t, req.Validate())

	req = valid()
	req.Timezone = "this is not a timezone at all!!"
	require.Error(t, req.Validate())

	// ExpiresInSeconds is an optional override here (0 means "use the
	// queue's setting"), unlike CreateQueueRequest.
	req = valid()
	req.ExpiresInSeconds = 0
	require.NoError(t, req.Validate())

	req = valid()
	req.ExpiresInSeconds = -1
	require.Error(t, req.Validate())

	req = valid()
	req.RetryStrategy = &RetryStrategy{RetryIntervalsInSeconds: []int64{-1}}
	require.Error(t, req.Validate())

	req = valid()
	req.Payload = make([]byte, maxPayloadSize+1)
	require.Error(t, req.Validate())
}

func TestUpdateScheduleRequest_Validate(t *testing.T) {
	valid := func() *UpdateScheduleRequest {
		return &UpdateScheduleRequest{Cron: "*/5 * * * *", Timezone: "UTC"}
	}
	require.NoError(t, valid().Validate())

	req := valid()
	req.Cron = "this is not a cron expression at all!!"
	require.Error(t, req.Validate())

	req = valid()
	req.Timezone = "this is not a timezone at all!!"
	require.Error(t, req.Validate())

	req = valid()
	req.ExpiresInSeconds = -1
	require.Error(t, req.Validate())

	req = valid()
	req.Payload = make([]byte, maxPayloadSize+1)
	require.Error(t, req.Validate())
}

func TestUpdateQueueRequest_Validate(t *testing.T) {
	valid := func() *UpdateQueueRequest {
		return &UpdateQueueRequest{
			AccountId:                 1,
			QueueName:                 "test_queue",
			KeepaliveTimeoutInSeconds: 15,
			ExpiresInSeconds:          600,
		}
	}
	require.NoError(t, valid().Validate())

	req := valid()
	req.ExpiresInSeconds = 0
	require.Error(t, req.Validate())

	req = valid()
	req.KeepaliveTimeoutInSeconds = 0
	require.Error(t, req.Validate())

	req = valid()
	req.RetryStrategy = &RetryStrategy{RetryIntervalsInSeconds: []int64{maxRetryIntervalInSeconds + 1}}
	require.Error(t, req.Validate())

	req = valid()
	req.DequeuingSettings = &DequeuingSettings{RateLimiting: &TokenBucketRateLimiting{Interval: 1, MaxTokens: 1, IntervalUnit: 99}}
	require.Error(t, req.Validate())

	req = valid()
	req.DeadLetterQueueConfig = &DeadLetterQueueConfig{MaxSize: -1, RetentionPeriodInSeconds: 600}
	require.Error(t, req.Validate())
}

func TestDequeueRequest_Validate(t *testing.T) {
	req := &DequeueRequest{QueueId: validQueueId()}
	require.NoError(t, req.Validate())

	req = &DequeueRequest{}
	require.Error(t, req.Validate())

	// The crash this guards against: getRefillInterval switches on
	// IntervalUnit with no default case once RateLimiting is set.
	req = &DequeueRequest{
		QueueId: validQueueId(),
		DequeuingSettings: &DequeuingSettings{
			RateLimiting: &TokenBucketRateLimiting{Interval: 1, MaxTokens: 1, IntervalUnit: IntervalUnit_INTERVAL_UNIT_INVALID},
		},
	}
	require.Error(t, req.Validate())

	req = &DequeueRequest{
		QueueId:               validQueueId(),
		DeadLetterQueueConfig: &DeadLetterQueueConfig{Enable: true, RetentionPeriodInSeconds: 0},
	}
	require.Error(t, req.Validate())

	// KeepaliveTimeoutInSeconds is an optional override (0 means "use the
	// queue's setting") — this is the consumer's own call, unlike Enqueue.
	req = &DequeueRequest{QueueId: validQueueId(), KeepaliveTimeoutInSeconds: 0}
	require.NoError(t, req.Validate())

	req = &DequeueRequest{QueueId: validQueueId(), KeepaliveTimeoutInSeconds: 4}
	require.Error(t, req.Validate())

	req = &DequeueRequest{QueueId: validQueueId(), KeepaliveTimeoutInSeconds: 15}
	require.NoError(t, req.Validate())
}

func TestEnqueueRequest_Validate(t *testing.T) {
	valid := func() *EnqueueRequest {
		return &EnqueueRequest{
			QueueId: validQueueId(),
			Entries: []*EnqueueRequestEntry{{Payload: []byte("hello")}},
		}
	}
	require.NoError(t, valid().Validate())

	req := valid()
	req.QueueId = nil
	require.Error(t, req.Validate())

	req = valid()
	entries := make([]*EnqueueRequestEntry, maxNumberOfEnqueueRequestEntries+1)
	for i := range entries {
		entries[i] = &EnqueueRequestEntry{}
	}
	req.Entries = entries
	require.Error(t, req.Validate())

	req = valid()
	req.Entries[0].ScheduledAt = -1
	require.Error(t, req.Validate())

	req = valid()
	req.Entries[0].ExpiresAt = -1
	require.Error(t, req.Validate())

	req = valid()
	req.Entries[0].RetryStrategy = &RetryStrategy{RetryIntervalsInSeconds: []int64{-1}}
	require.Error(t, req.Validate())

	req = valid()
	req.Entries[0].Payload = make([]byte, maxPayloadSize+1)
	require.Error(t, req.Validate())
}

func TestGetQueueRequest_Validate(t *testing.T) {
	require.NoError(t, (&GetQueueRequest{QueueId: validQueueId()}).Validate())
	require.Error(t, (&GetQueueRequest{}).Validate())
}

func TestGetStatisticsRequest_Validate(t *testing.T) {
	require.NoError(t, (&GetStatisticsRequest{QueueId: validQueueId()}).Validate())
	require.Error(t, (&GetStatisticsRequest{}).Validate())
}

func TestGetTaskRequest_Validate(t *testing.T) {
	require.NoError(t, (&GetTaskRequest{TaskId: &TaskId{AccountId: 1, QueueId: 2, TaskId: 3}}).Validate())
	require.Error(t, (&GetTaskRequest{}).Validate())
}

func TestListSchedulesRequest_Validate(t *testing.T) {
	require.NoError(t, (&ListSchedulesRequest{QueueId: validQueueId()}).Validate())
	require.Error(t, (&ListSchedulesRequest{}).Validate())
}

func TestListTasksRequest_Validate(t *testing.T) {
	require.NoError(t, (&ListTasksRequest{QueueId: validQueueId()}).Validate())
	require.Error(t, (&ListTasksRequest{}).Validate())
}

func TestPurgeQueueRequest_Validate(t *testing.T) {
	require.NoError(t, (&PurgeQueueRequest{QueueId: validQueueId()}).Validate())
	require.Error(t, (&PurgeQueueRequest{}).Validate())
}

func TestDeleteTasksRequest_Validate(t *testing.T) {
	require.NoError(t, (&DeleteTasksRequest{QueueId: validQueueId()}).Validate())
	require.Error(t, (&DeleteTasksRequest{}).Validate())

	taskIds := make([]uint64, maxNumberOfTaskIds+1)
	require.Error(t, (&DeleteTasksRequest{QueueId: validQueueId(), TaskIds: taskIds}).Validate())
}

func TestRestartTasksRequest_Validate(t *testing.T) {
	require.NoError(t, (&RestartTasksRequest{QueueId: validQueueId()}).Validate())
	require.Error(t, (&RestartTasksRequest{}).Validate())

	req := &RestartTasksRequest{QueueId: validQueueId(), Entries: []*RestartTasksRequestEntry{{ScheduledAt: -1}}}
	require.Error(t, req.Validate())

	req = &RestartTasksRequest{QueueId: validQueueId(), Entries: []*RestartTasksRequestEntry{{ExpiresAt: -1}}}
	require.Error(t, req.Validate())

	entries := make([]*RestartTasksRequestEntry, maxNumberOfTaskIds+1)
	for i := range entries {
		entries[i] = &RestartTasksRequestEntry{}
	}
	require.Error(t, (&RestartTasksRequest{QueueId: validQueueId(), Entries: entries}).Validate())
}

func TestReportStatusRequest_Validate(t *testing.T) {
	require.NoError(t, (&ReportStatusRequest{Entries: []*ReportStatusRequestEntry{{TaskId: &TaskId{AccountId: 1, QueueId: 2, TaskId: 3}}}}).Validate())

	req := &ReportStatusRequest{Entries: []*ReportStatusRequestEntry{{}}}
	require.Error(t, req.Validate())

	entries := make([]*ReportStatusRequestEntry, maxNumberOfReportStatusEntries+1)
	for i := range entries {
		entries[i] = &ReportStatusRequestEntry{TaskId: &TaskId{AccountId: 1, QueueId: 2, TaskId: uint64(i + 1)}}
	}
	require.Error(t, (&ReportStatusRequest{Entries: entries}).Validate())

	req = &ReportStatusRequest{DeadLetterQueueConfig: &DeadLetterQueueConfig{Enable: true, RetentionPeriodInSeconds: 0}}
	require.Error(t, req.Validate())
}

func TestReportSchedulesStatusRequest_Validate(t *testing.T) {
	req := &ReportSchedulesStatusRequest{ScheduleId: &ScheduleId{AccountId: 1, QueueId: 2, ScheduleId: 3}}
	require.NoError(t, req.Validate())

	require.Error(t, (&ReportSchedulesStatusRequest{}).Validate())

	req = &ReportSchedulesStatusRequest{ScheduleId: &ScheduleId{AccountId: 1, QueueId: 2, ScheduleId: 3}, NextScheduledAt: -1}
	require.Error(t, req.Validate())

	req = &ReportSchedulesStatusRequest{ScheduleId: &ScheduleId{AccountId: 1, QueueId: 2, ScheduleId: 3}, LastEnqueuedFor: -1}
	require.Error(t, req.Validate())
}
