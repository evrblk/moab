package v0

import (
	"testing"

	"github.com/stretchr/testify/require"

	moabpb "github.com/evrblk/evrblk-go/moab/v0"
)

func TestValidateEnqueueRequest(t *testing.T) {
	tests := []struct {
		name        string
		request     *moabpb.EnqueueRequest
		shouldError bool
	}{
		{
			name:        "empty request",
			request:     &moabpb.EnqueueRequest{},
			shouldError: true,
		},
		{
			name: "no entries",
			request: &moabpb.EnqueueRequest{
				QueueName: "myqueue1",
			},
			shouldError: true,
		},
		{
			name: "invalid queue name",
			request: &moabpb.EnqueueRequest{
				QueueName: "Myqueue 1",
			},
			shouldError: true,
		},
		{
			name: "too many entries",
			request: &moabpb.EnqueueRequest{
				QueueName: "myqueue1",
				Entries:   make([]*moabpb.EnqueueRequestEntry, 51),
			},
			shouldError: true,
		},
		{
			name: "keepalive too small",
			request: &moabpb.EnqueueRequest{
				QueueName: "myqueue1",
				Entries: []*moabpb.EnqueueRequestEntry{
					{KeepaliveTimeoutInSeconds: 4},
				},
			},
			shouldError: true,
		},
		{
			name: "keepalive too big",
			request: &moabpb.EnqueueRequest{
				QueueName: "myqueue1",
				Entries: []*moabpb.EnqueueRequestEntry{
					{KeepaliveTimeoutInSeconds: 61},
				},
			},
			shouldError: true,
		},
		{
			name: "dedupe key too long",
			request: &moabpb.EnqueueRequest{
				QueueName: "myqueue1",
				Entries: []*moabpb.EnqueueRequestEntry{
					{DedupeKey: string(make([]byte, 257))},
				},
			},
			shouldError: true,
		},
		{
			name: "too many retry intervals",
			request: &moabpb.EnqueueRequest{
				QueueName: "myqueue1",
				Entries: []*moabpb.EnqueueRequestEntry{
					{RetryStrategy: &moabpb.RetryStrategy{RetryIntervalsInSeconds: make([]int64, 22)}},
				},
			},
			shouldError: true,
		},
		{
			name: "negative retry interval",
			request: &moabpb.EnqueueRequest{
				QueueName: "myqueue1",
				Entries: []*moabpb.EnqueueRequestEntry{
					{RetryStrategy: &moabpb.RetryStrategy{RetryIntervalsInSeconds: []int64{-1}}},
				},
			},
			shouldError: true,
		},
		{
			name: "retry interval too big",
			request: &moabpb.EnqueueRequest{
				QueueName: "myqueue1",
				Entries: []*moabpb.EnqueueRequestEntry{
					{RetryStrategy: &moabpb.RetryStrategy{RetryIntervalsInSeconds: []int64{60*15 + 1}}},
				},
			},
			shouldError: true,
		},
		{
			name: "payload too big",
			request: &moabpb.EnqueueRequest{
				QueueName: "myqueue1",
				Entries: []*moabpb.EnqueueRequestEntry{
					{Payload: make([]byte, 64*1024+1)},
				},
			},
			shouldError: true,
		},
		{
			name: "negative expires at",
			request: &moabpb.EnqueueRequest{
				QueueName: "myqueue1",
				Entries: []*moabpb.EnqueueRequestEntry{
					{ExpiresAt: -1},
				},
			},
			shouldError: true,
		},
		{
			name: "negative scheduled at",
			request: &moabpb.EnqueueRequest{
				QueueName: "myqueue1",
				Entries: []*moabpb.EnqueueRequestEntry{
					{ScheduledAt: -1},
				},
			},
			shouldError: true,
		},
		{
			name: "valid request",
			request: &moabpb.EnqueueRequest{
				QueueName: "myqueue1",
				Entries: []*moabpb.EnqueueRequestEntry{
					{
						Payload:                   []byte(`{"key": "value"}`),
						ScheduledAt:               0,
						ExpiresAt:                 0,
						DedupeKey:                 "key1",
						KeepaliveTimeoutInSeconds: 0,
						RetryStrategy: &moabpb.RetryStrategy{
							RetryIntervalsInSeconds: []int64{1},
						},
						OverwriteOnDuplicate: []moabpb.EnqueueRequestEntry_OverwriteOnDuplicate{},
						ThreadId:             "",
					},
					{
						Payload:                   []byte(`{"key": "value"}`),
						ScheduledAt:               0,
						ExpiresAt:                 0,
						DedupeKey:                 "key2",
						KeepaliveTimeoutInSeconds: 15,
						RetryStrategy: &moabpb.RetryStrategy{
							RetryIntervalsInSeconds: []int64{1},
						},
						OverwriteOnDuplicate: []moabpb.EnqueueRequestEntry_OverwriteOnDuplicate{},
						ThreadId:             "",
					},
				},
			},
			shouldError: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.shouldError {
				require.Error(t, ValidateEnqueueRequest(test.request))
			} else {
				require.NoError(t, ValidateEnqueueRequest(test.request))
			}
		})
	}
}

func TestValidateDequeueRequest(t *testing.T) {
	tests := []struct {
		name        string
		request     *moabpb.DequeueRequest
		shouldError bool
	}{
		{
			name:        "empty request",
			request:     &moabpb.DequeueRequest{},
			shouldError: true,
		},
		{
			name: "invalid queue name",
			request: &moabpb.DequeueRequest{
				QueueName: "Myqueue 1",
			},
			shouldError: true,
		},
		{
			name: "invalid batch size",
			request: &moabpb.DequeueRequest{
				QueueName: "myqueue1",
				BatchSize: 0,
			},
			shouldError: true,
		},
		{
			name: "valid request",
			request: &moabpb.DequeueRequest{
				QueueName: "myqueue1",
				BatchSize: 10,
			},
			shouldError: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.shouldError {
				require.Error(t, ValidateDequeueRequest(test.request))
			} else {
				require.NoError(t, ValidateDequeueRequest(test.request))
			}
		})
	}
}

func TestValidatePurgeQueueRequest(t *testing.T) {
	tests := []struct {
		name        string
		request     *moabpb.PurgeQueueRequest
		shouldError bool
	}{
		{
			name:        "empty request",
			request:     &moabpb.PurgeQueueRequest{},
			shouldError: true,
		},
		{
			name: "invalid queue name",
			request: &moabpb.PurgeQueueRequest{
				QueueName: "Myqueue+1",
			},
			shouldError: true,
		},
		{
			name: "valid request",
			request: &moabpb.PurgeQueueRequest{
				QueueName: "myqueue1",
			},
			shouldError: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.shouldError {
				require.Error(t, ValidatePurgeQueueRequest(test.request))
			} else {
				require.NoError(t, ValidatePurgeQueueRequest(test.request))
			}
		})
	}
}

func TestValidateGetTaskRequest(t *testing.T) {
	tests := []struct {
		name        string
		request     *moabpb.GetTaskRequest
		shouldError bool
	}{
		{
			name:        "empty request",
			request:     &moabpb.GetTaskRequest{},
			shouldError: true,
		},
		{
			name: "invalid queue name",
			request: &moabpb.GetTaskRequest{
				QueueName: "Myqueue 1",
				TaskId:    "tsk_ISfFsVup2QS",
			},
			shouldError: true,
		},
		{
			name: "invalid task id prefix",
			request: &moabpb.GetTaskRequest{
				QueueName: "myqueue1",
				TaskId:    "err_ISfFsVup2QS",
			},
			shouldError: true,
		},
		{
			name: "valid request",
			request: &moabpb.GetTaskRequest{
				QueueName: "myqueue1",
				TaskId:    "tsk_ISfFsVup2QS",
			},
			shouldError: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.shouldError {
				require.Error(t, ValidateGetTaskRequest(test.request))
			} else {
				require.NoError(t, ValidateGetTaskRequest(test.request))
			}
		})
	}
}

func TestValidateReportStatusRequest(t *testing.T) {
	tests := []struct {
		name        string
		request     *moabpb.ReportStatusRequest
		shouldError bool
	}{
		{
			name:        "empty request",
			request:     &moabpb.ReportStatusRequest{},
			shouldError: true,
		},
		{
			name: "no entries",
			request: &moabpb.ReportStatusRequest{
				QueueName: "myqueue1",
			},
			shouldError: true,
		},
		{
			name: "too many entries",
			request: &moabpb.ReportStatusRequest{
				QueueName: "myqueue1",
				Entries:   make([]*moabpb.ReportStatusRequestEntry, 51),
			},
			shouldError: true,
		},
		{
			name: "invalid task id",
			request: &moabpb.ReportStatusRequest{
				QueueName: "myqueue1",
				Entries: []*moabpb.ReportStatusRequestEntry{
					{
						TaskId:  "err_ISfFsVup2QS",
						Attempt: 1,
					},
				},
			},
			shouldError: true,
		},
		{
			name: "invalid attempt",
			request: &moabpb.ReportStatusRequest{
				QueueName: "myqueue1",
				Entries: []*moabpb.ReportStatusRequestEntry{
					{
						TaskId:  "tsk_ISfFsVup2QS",
						Attempt: 0,
					},
				},
			},
			shouldError: true,
		},
		{
			name: "valid request",
			request: &moabpb.ReportStatusRequest{
				QueueName: "myqueue1",
				Entries: []*moabpb.ReportStatusRequestEntry{
					{
						TaskId:  "tsk_ISfFsVup2QS",
						Attempt: 1,
						Status:  moabpb.ReportStatusRequestEntry_STATUS_SUCCEEDED,
					},
				},
			},
			shouldError: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.shouldError {
				require.Error(t, ValidateReportStatusRequest(test.request))
			} else {
				require.NoError(t, ValidateReportStatusRequest(test.request))
			}
		})
	}
}

func TestValidateDeleteTasksRequest(t *testing.T) {
	tooManyTaskIds := make([]string, 51)
	for i := range tooManyTaskIds {
		tooManyTaskIds[i] = "tsk_ISfFsVup2QS"
	}

	tests := []struct {
		name        string
		request     *moabpb.DeleteTasksRequest
		shouldError bool
	}{
		{
			name:        "empty request",
			request:     &moabpb.DeleteTasksRequest{},
			shouldError: true,
		},
		{
			name: "no task ids",
			request: &moabpb.DeleteTasksRequest{
				QueueName: "myqueue1",
			},
			shouldError: true,
		},
		{
			name: "invalid task id",
			request: &moabpb.DeleteTasksRequest{
				QueueName: "myqueue1",
				TaskIds:   []string{"err_ISfFsVup2QS"},
			},
			shouldError: true,
		},
		{
			name: "too many task ids",
			request: &moabpb.DeleteTasksRequest{
				QueueName: "myqueue1",
				TaskIds:   tooManyTaskIds,
			},
			shouldError: true,
		},
		{
			name: "duplicate task ids",
			request: &moabpb.DeleteTasksRequest{
				QueueName: "myqueue1",
				TaskIds:   []string{"tsk_ISfFsVup2QS", "tsk_ISfFsVup2QS"},
			},
			shouldError: true,
		},
		{
			name: "valid request",
			request: &moabpb.DeleteTasksRequest{
				QueueName: "myqueue1",
				TaskIds:   []string{"tsk_ISfFsVup2QS", "tsk_eAALyLnlNDR"},
			},
			shouldError: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.shouldError {
				require.Error(t, ValidateDeleteTasksRequest(test.request))
			} else {
				require.NoError(t, ValidateDeleteTasksRequest(test.request))
			}
		})
	}
}

func TestValidateRestartTasksRequest(t *testing.T) {
	tooManyEntries := make([]*moabpb.RestartTasksRequestEntry, 51)
	for i := range tooManyEntries {
		tooManyEntries[i] = &moabpb.RestartTasksRequestEntry{TaskId: "tsk_ISfFsVup2QS"}
	}

	tests := []struct {
		name        string
		request     *moabpb.RestartTasksRequest
		shouldError bool
	}{
		{
			name:        "empty request",
			request:     &moabpb.RestartTasksRequest{},
			shouldError: true,
		},
		{
			name: "no entries",
			request: &moabpb.RestartTasksRequest{
				QueueName: "myqueue1",
			},
			shouldError: true,
		},
		{
			name: "invalid task id",
			request: &moabpb.RestartTasksRequest{
				QueueName: "myqueue1",
				Entries:   []*moabpb.RestartTasksRequestEntry{{TaskId: "err_ISfFsVup2QS"}},
			},
			shouldError: true,
		},
		{
			name: "too many entries",
			request: &moabpb.RestartTasksRequest{
				QueueName: "myqueue1",
				Entries:   tooManyEntries,
			},
			shouldError: true,
		},
		{
			name: "duplicate task ids",
			request: &moabpb.RestartTasksRequest{
				QueueName: "myqueue1",
				Entries: []*moabpb.RestartTasksRequestEntry{
					{TaskId: "tsk_ISfFsVup2QS"},
					{TaskId: "tsk_ISfFsVup2QS"},
				},
			},
			shouldError: true,
		},
		{
			name: "negative scheduled_at",
			request: &moabpb.RestartTasksRequest{
				QueueName: "myqueue1",
				Entries:   []*moabpb.RestartTasksRequestEntry{{TaskId: "tsk_ISfFsVup2QS", ScheduledAt: -1}},
			},
			shouldError: true,
		},
		{
			name: "negative expires_at",
			request: &moabpb.RestartTasksRequest{
				QueueName: "myqueue1",
				Entries:   []*moabpb.RestartTasksRequestEntry{{TaskId: "tsk_ISfFsVup2QS", ExpiresAt: -1}},
			},
			shouldError: true,
		},
		{
			name: "valid request",
			request: &moabpb.RestartTasksRequest{
				QueueName: "myqueue1",
				Entries: []*moabpb.RestartTasksRequestEntry{
					{TaskId: "tsk_ISfFsVup2QS"},
					{TaskId: "tsk_eAALyLnlNDR"},
				},
			},
			shouldError: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.shouldError {
				require.Error(t, ValidateRestartTasksRequest(test.request))
			} else {
				require.NoError(t, ValidateRestartTasksRequest(test.request))
			}
		})
	}
}

func TestValidateListQueuesRequest(t *testing.T) {
	require.NoError(t, ValidateListQueuesRequest(&moabpb.ListQueuesRequest{}))
}

func TestValidateGetQueueRequest(t *testing.T) {
	tests := []struct {
		name        string
		request     *moabpb.GetQueueRequest
		shouldError bool
	}{
		{
			name:        "empty request",
			request:     &moabpb.GetQueueRequest{},
			shouldError: true,
		},
		{
			name: "invalid queue name",
			request: &moabpb.GetQueueRequest{
				QueueName: "myqueue 1",
			},
			shouldError: true,
		},
		{
			name: "valid request",
			request: &moabpb.GetQueueRequest{
				QueueName: "myqueue1",
			},
			shouldError: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.shouldError {
				require.Error(t, ValidateGetQueueRequest(test.request))
			} else {
				require.NoError(t, ValidateGetQueueRequest(test.request))
			}
		})
	}
}

func TestValidateCreateQueueRequest(t *testing.T) {
	tests := []struct {
		name        string
		request     *moabpb.CreateQueueRequest
		shouldError bool
	}{
		{
			name:        "empty request",
			request:     &moabpb.CreateQueueRequest{},
			shouldError: true,
		},
		{
			name: "invalid name characters",
			request: &moabpb.CreateQueueRequest{
				Name:                      "invalid name",
				KeepaliveTimeoutInSeconds: 15,
			},
			shouldError: true,
		},
		{
			name: "name too long",
			request: &moabpb.CreateQueueRequest{
				Name:                      string(make([]byte, 129)),
				KeepaliveTimeoutInSeconds: 15,
			},
			shouldError: true,
		},
		{
			name: "description too long",
			request: &moabpb.CreateQueueRequest{
				Name:                      "myqueue1",
				KeepaliveTimeoutInSeconds: 15,
				Description:               string(make([]byte, 1025)),
			},
			shouldError: true,
		},
		{
			name: "too many retry intervals",
			request: &moabpb.CreateQueueRequest{
				Name:                      "myqueue1",
				KeepaliveTimeoutInSeconds: 15,
				RetryStrategy:             &moabpb.RetryStrategy{RetryIntervalsInSeconds: make([]int64, 22)},
			},
			shouldError: true,
		},
		{
			name: "negative max in progress tasks",
			request: &moabpb.CreateQueueRequest{
				Name:                      "myqueue1",
				KeepaliveTimeoutInSeconds: 15,
				DequeuingSettings: &moabpb.DequeuingSettings{
					MaxInProgressTasks: -1,
				},
			},
			shouldError: true,
		},
		{
			name: "negative rate limiting max tokens",
			request: &moabpb.CreateQueueRequest{
				Name:                      "myqueue1",
				KeepaliveTimeoutInSeconds: 15,
				DequeuingSettings: &moabpb.DequeuingSettings{
					RateLimiting: &moabpb.TokenBucketRateLimiting{
						MaxTokens:    -1,
						Interval:     10,
						IntervalUnit: moabpb.IntervalUnit_INTERVAL_UNIT_SECONDS,
					},
				},
			},
			shouldError: true,
		},
		{
			name: "negative rate limiting interval",
			request: &moabpb.CreateQueueRequest{
				Name:                      "myqueue1",
				KeepaliveTimeoutInSeconds: 15,
				DequeuingSettings: &moabpb.DequeuingSettings{
					RateLimiting: &moabpb.TokenBucketRateLimiting{
						MaxTokens:    100,
						Interval:     -1,
						IntervalUnit: moabpb.IntervalUnit_INTERVAL_UNIT_SECONDS,
					},
				},
			},
			shouldError: true,
		},
		{
			name: "keepalive too small",
			request: &moabpb.CreateQueueRequest{
				Name:                      "myqueue1",
				KeepaliveTimeoutInSeconds: 4,
			},
			shouldError: true,
		},
		{
			name: "keepalive too big",
			request: &moabpb.CreateQueueRequest{
				Name:                      "myqueue1",
				KeepaliveTimeoutInSeconds: 61,
			},
			shouldError: true,
		},
		{
			name: "expires too small",
			request: &moabpb.CreateQueueRequest{
				Name:                      "myqueue1",
				KeepaliveTimeoutInSeconds: 15,
				ExpiresInSeconds:          59,
			},
			shouldError: true,
		},
		{
			name: "expires too big",
			request: &moabpb.CreateQueueRequest{
				Name:                      "myqueue1",
				KeepaliveTimeoutInSeconds: 15,
				ExpiresInSeconds:          14*86400 + 1,
			},
			shouldError: true,
		},
		{
			name: "negative DLQ retention period",
			request: &moabpb.CreateQueueRequest{
				Name:                      "myqueue1",
				KeepaliveTimeoutInSeconds: 15,
				DeadLetterQueueConfig: &moabpb.DeadLetterQueueConfig{
					RetentionPeriodInSeconds: -1,
				},
			},
			shouldError: true,
		},
		{
			name: "zero DLQ retention period is no longer unlimited, must be rejected",
			request: &moabpb.CreateQueueRequest{
				Name:                      "myqueue1",
				KeepaliveTimeoutInSeconds: 15,
				DeadLetterQueueConfig: &moabpb.DeadLetterQueueConfig{
					RetentionPeriodInSeconds: 0,
				},
			},
			shouldError: true,
		},
		{
			name: "negative DLQ max size",
			request: &moabpb.CreateQueueRequest{
				Name:                      "myqueue1",
				KeepaliveTimeoutInSeconds: 15,
				DeadLetterQueueConfig: &moabpb.DeadLetterQueueConfig{
					RetentionPeriodInSeconds: 86400,
					MaxSize:                  -1,
				},
			},
			shouldError: true,
		},
		{
			name: "valid request",
			request: &moabpb.CreateQueueRequest{
				Name:                      "myqueue1",
				KeepaliveTimeoutInSeconds: 5,
				ExpiresInSeconds:          86400,
				RetryStrategy: &moabpb.RetryStrategy{
					RetryIntervalsInSeconds: []int64{10, 15, 20},
				},
				DeadLetterQueueConfig: &moabpb.DeadLetterQueueConfig{
					RetentionPeriodInSeconds: 86400,
					MaxSize:                  0,
				},
				DequeuingSettings: &moabpb.DequeuingSettings{
					RateLimiting: &moabpb.TokenBucketRateLimiting{
						MaxTokens:    100,
						Interval:     10,
						IntervalUnit: moabpb.IntervalUnit_INTERVAL_UNIT_SECONDS,
					},
				},
			},
			shouldError: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.shouldError {
				require.Error(t, ValidateCreateQueueRequest(test.request))
			} else {
				require.NoError(t, ValidateCreateQueueRequest(test.request))
			}
		})
	}
}

func TestValidateUpdateQueueRequest(t *testing.T) {
	tests := []struct {
		name        string
		request     *moabpb.UpdateQueueRequest
		shouldError bool
	}{
		{
			name:        "empty request",
			request:     &moabpb.UpdateQueueRequest{},
			shouldError: true,
		},
		{
			name: "invalid queue name",
			request: &moabpb.UpdateQueueRequest{
				QueueName:                 "invalid name",
				KeepaliveTimeoutInSeconds: 15,
				ExpiresInSeconds:          86400,
				ExpectedVersion:           1,
			},
			shouldError: true,
		},
		{
			name: "description too long",
			request: &moabpb.UpdateQueueRequest{
				QueueName:                 "myqueue1",
				KeepaliveTimeoutInSeconds: 15,
				ExpiresInSeconds:          86400,
				Description:               string(make([]byte, 1025)),
				ExpectedVersion:           1,
			},
			shouldError: true,
		},
		{
			name: "too many retry intervals",
			request: &moabpb.UpdateQueueRequest{
				QueueName:                 "myqueue1",
				KeepaliveTimeoutInSeconds: 15,
				ExpiresInSeconds:          86400,
				RetryStrategy:             &moabpb.RetryStrategy{RetryIntervalsInSeconds: make([]int64, 22)},
				ExpectedVersion:           1,
			},
			shouldError: true,
		},
		{
			name: "keepalive too small",
			request: &moabpb.UpdateQueueRequest{
				QueueName:                 "myqueue1",
				KeepaliveTimeoutInSeconds: 4,
				ExpiresInSeconds:          14 * 86400,
				ExpectedVersion:           1,
			},
			shouldError: true,
		},
		{
			name: "keepalive too big",
			request: &moabpb.UpdateQueueRequest{
				QueueName:                 "myqueue1",
				KeepaliveTimeoutInSeconds: 61,
				ExpiresInSeconds:          14 * 86400,
				ExpectedVersion:           1,
			},
			shouldError: true,
		},
		{
			name: "expires too small",
			request: &moabpb.UpdateQueueRequest{
				QueueName:                 "myqueue1",
				KeepaliveTimeoutInSeconds: 15,
				ExpiresInSeconds:          59,
				ExpectedVersion:           1,
			},
			shouldError: true,
		},
		{
			name: "expires too big",
			request: &moabpb.UpdateQueueRequest{
				QueueName:                 "myqueue1",
				KeepaliveTimeoutInSeconds: 15,
				ExpiresInSeconds:          14*86400 + 1,
				ExpectedVersion:           1,
			},
			shouldError: true,
		},
		{
			name: "negative DLQ retention period",
			request: &moabpb.UpdateQueueRequest{
				QueueName:                 "myqueue1",
				KeepaliveTimeoutInSeconds: 15,
				ExpiresInSeconds:          86400,
				DeadLetterQueueConfig: &moabpb.DeadLetterQueueConfig{
					RetentionPeriodInSeconds: -1,
				},
				ExpectedVersion: 1,
			},
			shouldError: true,
		},
		{
			name: "zero DLQ retention period is no longer unlimited, must be rejected",
			request: &moabpb.UpdateQueueRequest{
				QueueName:                 "myqueue1",
				KeepaliveTimeoutInSeconds: 15,
				ExpiresInSeconds:          86400,
				DeadLetterQueueConfig: &moabpb.DeadLetterQueueConfig{
					RetentionPeriodInSeconds: 0,
				},
				ExpectedVersion: 1,
			},
			shouldError: true,
		},
		{
			name: "negative DLQ max size",
			request: &moabpb.UpdateQueueRequest{
				QueueName:                 "myqueue1",
				KeepaliveTimeoutInSeconds: 15,
				ExpiresInSeconds:          86400,
				DeadLetterQueueConfig: &moabpb.DeadLetterQueueConfig{
					RetentionPeriodInSeconds: 86400,
					MaxSize:                  -1,
				},
				ExpectedVersion: 1,
			},
			shouldError: true,
		},
		{
			name: "negative max in progress tasks",
			request: &moabpb.UpdateQueueRequest{
				QueueName:                 "myqueue1",
				KeepaliveTimeoutInSeconds: 15,
				ExpiresInSeconds:          86400,
				DequeuingSettings: &moabpb.DequeuingSettings{
					MaxInProgressTasks: -1,
				},
				ExpectedVersion: 1,
			},
			shouldError: true,
		},
		{
			name: "negative rate limiting max tokens",
			request: &moabpb.UpdateQueueRequest{
				QueueName:                 "myqueue1",
				KeepaliveTimeoutInSeconds: 15,
				ExpiresInSeconds:          86400,
				DequeuingSettings: &moabpb.DequeuingSettings{
					RateLimiting: &moabpb.TokenBucketRateLimiting{
						MaxTokens:    -1,
						Interval:     10,
						IntervalUnit: moabpb.IntervalUnit_INTERVAL_UNIT_SECONDS,
					},
				},
				ExpectedVersion: 1,
			},
			shouldError: true,
		},
		{
			name: "negative rate limiting interval",
			request: &moabpb.UpdateQueueRequest{
				QueueName:                 "myqueue1",
				KeepaliveTimeoutInSeconds: 15,
				ExpiresInSeconds:          86400,
				DequeuingSettings: &moabpb.DequeuingSettings{
					RateLimiting: &moabpb.TokenBucketRateLimiting{
						MaxTokens:    100,
						Interval:     -1,
						IntervalUnit: moabpb.IntervalUnit_INTERVAL_UNIT_SECONDS,
					},
				},
				ExpectedVersion: 1,
			},
			shouldError: true,
		},
		{
			name: "empty expected version",
			request: &moabpb.UpdateQueueRequest{
				QueueName:                 "myqueue1",
				KeepaliveTimeoutInSeconds: 15,
				ExpiresInSeconds:          86400,
			},
			shouldError: true,
		},
		{
			name: "negative expected version",
			request: &moabpb.UpdateQueueRequest{
				QueueName:                 "myqueue1",
				KeepaliveTimeoutInSeconds: 15,
				ExpiresInSeconds:          86400,
				ExpectedVersion:           -1,
			},
			shouldError: true,
		},
		{
			name: "valid request",
			request: &moabpb.UpdateQueueRequest{
				QueueName:                 "myqueue1",
				KeepaliveTimeoutInSeconds: 15,
				ExpiresInSeconds:          86400,
				RetryStrategy: &moabpb.RetryStrategy{
					RetryIntervalsInSeconds: []int64{10, 15, 20},
				},
				DeadLetterQueueConfig: &moabpb.DeadLetterQueueConfig{
					RetentionPeriodInSeconds: 86400,
					MaxSize:                  0,
				},
				DequeuingSettings: &moabpb.DequeuingSettings{
					RateLimiting: &moabpb.TokenBucketRateLimiting{
						MaxTokens:    100,
						Interval:     10,
						IntervalUnit: moabpb.IntervalUnit_INTERVAL_UNIT_SECONDS,
					},
				},
				ExpectedVersion: 1,
			},
			shouldError: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.shouldError {
				require.Error(t, ValidateUpdateQueueRequest(test.request))
			} else {
				require.NoError(t, ValidateUpdateQueueRequest(test.request))
			}
		})
	}
}

func TestValidateDeleteQueueRequest(t *testing.T) {
	tests := []struct {
		name        string
		request     *moabpb.DeleteQueueRequest
		shouldError bool
	}{
		{
			name:        "empty request",
			request:     &moabpb.DeleteQueueRequest{},
			shouldError: true,
		},
		{
			name: "valid request",
			request: &moabpb.DeleteQueueRequest{
				QueueName: "myqueue1",
			},
			shouldError: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.shouldError {
				require.Error(t, ValidateDeleteQueueRequest(test.request))
			} else {
				require.NoError(t, ValidateDeleteQueueRequest(test.request))
			}
		})
	}
}

func TestValidateGetScheduleRequest(t *testing.T) {
	tests := []struct {
		name        string
		request     *moabpb.GetScheduleRequest
		shouldError bool
	}{
		{
			name:        "empty request",
			request:     &moabpb.GetScheduleRequest{},
			shouldError: true,
		},
		{
			name: "invalid schedule name",
			request: &moabpb.GetScheduleRequest{
				QueueName:    "myqueue1",
				ScheduleName: "Schedule Name",
			},
			shouldError: true,
		},
		{
			name: "valid request",
			request: &moabpb.GetScheduleRequest{
				QueueName:    "myqueue1",
				ScheduleName: "schedule1",
			},
			shouldError: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.shouldError {
				require.Error(t, ValidateGetScheduleRequest(test.request))
			} else {
				require.NoError(t, ValidateGetScheduleRequest(test.request))
			}
		})
	}
}

func TestValidateListSchedulesRequest(t *testing.T) {
	tests := []struct {
		name        string
		request     *moabpb.ListSchedulesRequest
		shouldError bool
	}{
		{
			name:        "empty request",
			request:     &moabpb.ListSchedulesRequest{},
			shouldError: true,
		},
		{
			name: "invalid queue name",
			request: &moabpb.ListSchedulesRequest{
				QueueName: "invalid name",
			},
			shouldError: true,
		},
		{
			name: "valid request with no pagination",
			request: &moabpb.ListSchedulesRequest{
				QueueName: "myqueue1",
			},
			shouldError: false,
		},
		{
			name: "valid request with pagination token and limit",
			request: &moabpb.ListSchedulesRequest{
				QueueName:       "myqueue1",
				PaginationToken: "dGVzdA==",
				Limit:           50,
			},
			shouldError: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.shouldError {
				require.Error(t, ValidateListSchedulesRequest(test.request))
			} else {
				require.NoError(t, ValidateListSchedulesRequest(test.request))
			}
		})
	}
}

func TestValidateCreateScheduleRequest(t *testing.T) {
	tests := []struct {
		name        string
		request     *moabpb.CreateScheduleRequest
		shouldError bool
	}{
		{
			name:        "empty request",
			request:     &moabpb.CreateScheduleRequest{},
			shouldError: true,
		},
		{
			name: "invalid queue name",
			request: &moabpb.CreateScheduleRequest{
				QueueName: "invalid name",
			},
			shouldError: true,
		},
		{
			name: "invalid schedule name",
			request: &moabpb.CreateScheduleRequest{
				QueueName: "myqueue1",
				Name:      "invalid name",
			},
			shouldError: true,
		},
		{
			name: "description too long",
			request: &moabpb.CreateScheduleRequest{
				QueueName:   "myqueue1",
				Name:        "myschedule1",
				Cron:        "* * * * *",
				Description: string(make([]byte, 1025)),
			},
			shouldError: true,
		},
		{
			name: "invalid cron",
			request: &moabpb.CreateScheduleRequest{
				QueueName: "myqueue1",
				Name:      "myschedule1",
				Cron:      "invalid cron",
			},
			shouldError: true,
		},
		{
			name: "dedupe key too long",
			request: &moabpb.CreateScheduleRequest{
				QueueName: "myqueue1",
				Name:      "myschedule1",
				Cron:      "* * * * *",
				DedupeKey: string(make([]byte, 257)),
			},
			shouldError: true,
		},
		{
			name: "payload too big",
			request: &moabpb.CreateScheduleRequest{
				QueueName: "myqueue1",
				Name:      "myschedule1",
				Cron:      "0 * * * *",
				Payload:   make([]byte, 64*1024+1),
			},
			shouldError: true,
		},
		{
			name: "keepalive too small",
			request: &moabpb.CreateScheduleRequest{
				QueueName:                 "myqueue1",
				Name:                      "myschedule1",
				Cron:                      "0 * * * *",
				KeepaliveTimeoutInSeconds: 4,
			},
			shouldError: true,
		},
		{
			name: "expires too small",
			request: &moabpb.CreateScheduleRequest{
				QueueName:        "myqueue1",
				Name:             "myschedule1",
				Cron:             "5 * * * *",
				ExpiresInSeconds: 59,
			},
			shouldError: true,
		},
		{
			name: "too many retry intervals",
			request: &moabpb.CreateScheduleRequest{
				QueueName:     "myqueue1",
				Name:          "myschedule1",
				Cron:          "* 0 * * *",
				RetryStrategy: &moabpb.RetryStrategy{RetryIntervalsInSeconds: make([]int64, 22)},
			},
			shouldError: true,
		},
		{
			name: "empty timezone",
			request: &moabpb.CreateScheduleRequest{
				QueueName: "myqueue1",
				Name:      "myschedule1",
				Cron:      "* 0 * * *",
			},
			shouldError: true,
		},
		{
			name: "invalid timezone",
			request: &moabpb.CreateScheduleRequest{
				QueueName: "myqueue1",
				Name:      "myschedule1",
				Cron:      "* 0 * * *",
				Timezone:  "invalid timezone",
			},
			shouldError: true,
		},
		{
			name: "valid request",
			request: &moabpb.CreateScheduleRequest{
				QueueName: "myqueue1",
				Name:      "myschedule1",
				Cron:      "* 0 * * *",
				Timezone:  "UTC",
			},
			shouldError: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.shouldError {
				require.Error(t, ValidateCreateScheduleRequest(test.request))
			} else {
				require.NoError(t, ValidateCreateScheduleRequest(test.request))
			}
		})
	}
}

func TestValidateUpdateScheduleRequest(t *testing.T) {
	tests := []struct {
		name        string
		request     *moabpb.UpdateScheduleRequest
		shouldError bool
	}{
		{
			name:        "empty request",
			request:     &moabpb.UpdateScheduleRequest{},
			shouldError: true,
		},
		{
			name: "invalid queue name",
			request: &moabpb.UpdateScheduleRequest{
				QueueName:       "invalid name",
				ScheduleName:    "schedule1",
				Cron:            "* 0 * * *",
				Timezone:        "UTC",
				ExpectedVersion: 1,
			},
			shouldError: true,
		},
		{
			name: "invalid schedule name",
			request: &moabpb.UpdateScheduleRequest{
				QueueName:       "myqueue1",
				ScheduleName:    "invalid name",
				Cron:            "* 0 * * *",
				Timezone:        "UTC",
				ExpectedVersion: 1,
			},
			shouldError: true,
		},
		{
			name: "description too long",
			request: &moabpb.UpdateScheduleRequest{
				QueueName:       "myqueue1",
				ScheduleName:    "myschedule1",
				Cron:            "* 0 * * *",
				Timezone:        "UTC",
				Description:     string(make([]byte, 1025)),
				ExpectedVersion: 1,
			},
			shouldError: true,
		},
		{
			name: "keepalive too small",
			request: &moabpb.UpdateScheduleRequest{
				QueueName:                 "myqueue1",
				ScheduleName:              "myschedule1",
				Cron:                      "* 0 * * *",
				Timezone:                  "UTC",
				KeepaliveTimeoutInSeconds: 4,
				ExpectedVersion:           1,
			},
			shouldError: true,
		},
		{
			name: "expires too small",
			request: &moabpb.UpdateScheduleRequest{
				QueueName:        "myqueue1",
				ScheduleName:     "myschedule1",
				Cron:             "* 0 * * *",
				Timezone:         "UTC",
				ExpiresInSeconds: 59,
				ExpectedVersion:  1,
			},
			shouldError: true,
		},
		{
			name: "too many retry intervals",
			request: &moabpb.UpdateScheduleRequest{
				QueueName:       "myqueue1",
				ScheduleName:    "myschedule1",
				Cron:            "* 0 * * *",
				Timezone:        "UTC",
				RetryStrategy:   &moabpb.RetryStrategy{RetryIntervalsInSeconds: make([]int64, 22)},
				ExpectedVersion: 1,
			},
			shouldError: true,
		},
		{
			name: "payload too big",
			request: &moabpb.UpdateScheduleRequest{
				QueueName:       "myqueue1",
				ScheduleName:    "myschedule1",
				Cron:            "* 0 * * *",
				Timezone:        "UTC",
				Payload:         make([]byte, 64*1024+1),
				ExpectedVersion: 1,
			},
			shouldError: true,
		},
		{
			name: "invalid cron",
			request: &moabpb.UpdateScheduleRequest{
				QueueName:       "myqueue1",
				ScheduleName:    "myschedule1",
				Cron:            "invalid cron",
				Timezone:        "UTC",
				ExpectedVersion: 1,
			},
			shouldError: true,
		},
		{
			name: "dedupe key too long",
			request: &moabpb.UpdateScheduleRequest{
				QueueName:       "myqueue1",
				ScheduleName:    "myschedule1",
				Cron:            "* 0 * * *",
				Timezone:        "UTC",
				DedupeKey:       string(make([]byte, 257)),
				ExpectedVersion: 1,
			},
			shouldError: true,
		},
		{
			name: "invalid timezone",
			request: &moabpb.UpdateScheduleRequest{
				QueueName:       "myqueue1",
				ScheduleName:    "myschedule1",
				Cron:            "* 0 * * *",
				Timezone:        "invalid timezone",
				ExpectedVersion: 1,
			},
			shouldError: true,
		},
		{
			name: "empty timezone",
			request: &moabpb.UpdateScheduleRequest{
				QueueName:       "myqueue1",
				ScheduleName:    "myschedule1",
				Cron:            "* 0 * * *",
				ExpectedVersion: 1,
			},
			shouldError: true,
		},
		{
			name: "empty expected version",
			request: &moabpb.UpdateScheduleRequest{
				QueueName:    "myqueue1",
				ScheduleName: "myschedule1",
				Cron:         "* 0 * * *",
				Timezone:     "America/Los_Angeles",
			},
			shouldError: true,
		},
		{
			name: "negative expected version",
			request: &moabpb.UpdateScheduleRequest{
				QueueName:       "myqueue1",
				ScheduleName:    "myschedule1",
				Cron:            "* 0 * * *",
				Timezone:        "America/Los_Angeles",
				ExpectedVersion: -1,
			},
			shouldError: true,
		},
		{
			name: "valid request",
			request: &moabpb.UpdateScheduleRequest{
				QueueName:       "myqueue1",
				ScheduleName:    "myschedule1",
				Cron:            "* 0 * * *",
				Timezone:        "America/Los_Angeles",
				ExpectedVersion: 1,
			},
			shouldError: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.shouldError {
				require.Error(t, ValidateUpdateScheduleRequest(test.request))
			} else {
				require.NoError(t, ValidateUpdateScheduleRequest(test.request))
			}
		})
	}
}

func TestValidateDeleteScheduleRequest(t *testing.T) {
	tests := []struct {
		name        string
		request     *moabpb.DeleteScheduleRequest
		shouldError bool
	}{
		{
			name:        "empty request",
			request:     &moabpb.DeleteScheduleRequest{},
			shouldError: true,
		},
		{
			name: "invalid queue name",
			request: &moabpb.DeleteScheduleRequest{
				QueueName:    "Myqueue 1",
				ScheduleName: "myschedule1",
			},
			shouldError: true,
		},
		{
			name: "invalid schedule name",
			request: &moabpb.DeleteScheduleRequest{
				QueueName:    "myqueue1",
				ScheduleName: "Myschedule 1",
			},
			shouldError: true,
		},
		{
			name: "empty schedule name",
			request: &moabpb.DeleteScheduleRequest{
				QueueName: "myqueue1",
			},
			shouldError: true,
		},
		{
			name: "valid request",
			request: &moabpb.DeleteScheduleRequest{
				QueueName:    "myqueue1",
				ScheduleName: "myschedule1",
			},
			shouldError: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.shouldError {
				require.Error(t, ValidateDeleteScheduleRequest(test.request))
			} else {
				require.NoError(t, ValidateDeleteScheduleRequest(test.request))
			}
		})
	}
}
