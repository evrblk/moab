package v0

import (
	"context"
	"log"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/evrblk/monstera/cluster"
	"github.com/evrblk/monstera/store"
	"github.com/evrblk/yellowstone-common/honey"

	moabpb "github.com/evrblk/evrblk-go/moab/v0"
	"github.com/evrblk/moab/pkg/coreapis"
	"github.com/evrblk/moab/pkg/queues"
	"github.com/evrblk/moab/pkg/tasks"
)

func TestCreateQueueValidation(t *testing.T) {
	server := setupMoabApiServer()
	ctx := context.Background()

	// valid request
	resp1, err := server.CreateQueue(ctx, &moabpb.CreateQueueRequest{
		Name:                      "testqueue",
		KeepaliveTimeoutInSeconds: 5,
		ExpiresInSeconds:          86400,
	})
	require.NoError(t, err)
	require.NotNil(t, resp1.Queue)

	// invalid request - missing expires in seconds and keepalive timeout
	_, err = server.CreateQueue(ctx, &moabpb.CreateQueueRequest{
		Name: "testqueue",
	})
	require.Error(t, err)
}

func TestGetQueueValidation(t *testing.T) {
	server := setupMoabApiServer()
	ctx := context.Background()

	// Create a queue first
	_, err := server.CreateQueue(ctx, &moabpb.CreateQueueRequest{
		Name:                      "testqueue",
		KeepaliveTimeoutInSeconds: 5,
		ExpiresInSeconds:          86400,
	})
	require.NoError(t, err)

	// valid request
	resp, err := server.GetQueue(ctx, &moabpb.GetQueueRequest{
		QueueName: "testqueue",
	})
	require.NoError(t, err)
	require.NotNil(t, resp.Queue)

	// invalid request - invalid queue name
	_, err = server.GetQueue(ctx, &moabpb.GetQueueRequest{
		QueueName: "invalid@queue",
	})
	require.Error(t, err)
}

func TestUpdateQueueValidation(t *testing.T) {
	server := setupMoabApiServer()
	ctx := context.Background()

	// Create a queue first
	_, err := server.CreateQueue(ctx, &moabpb.CreateQueueRequest{
		Name:                      "testqueue",
		KeepaliveTimeoutInSeconds: 5,
		ExpiresInSeconds:          86400,
	})
	require.NoError(t, err)

	// valid request
	resp, err := server.UpdateQueue(ctx, &moabpb.UpdateQueueRequest{
		QueueName:                 "testqueue",
		Description:               "Updated description",
		KeepaliveTimeoutInSeconds: 10,
		ExpiresInSeconds:          172800,
		ExpectedVersion:           1,
	})
	require.NoError(t, err)
	require.NotNil(t, resp.Queue)

	// invalid request - missing keepalive timeout
	_, err = server.UpdateQueue(ctx, &moabpb.UpdateQueueRequest{
		QueueName:        "testqueue",
		Description:      "Updated description",
		ExpiresInSeconds: 172800,
		ExpectedVersion:  1,
	})
	require.Error(t, err)
}

func TestDeleteQueueValidation(t *testing.T) {
	server := setupMoabApiServer()
	ctx := context.Background()

	// Create a queue first
	_, err := server.CreateQueue(ctx, &moabpb.CreateQueueRequest{
		Name:                      "testqueue",
		KeepaliveTimeoutInSeconds: 5,
		ExpiresInSeconds:          86400,
	})
	require.NoError(t, err)

	// valid request
	_, err = server.DeleteQueue(ctx, &moabpb.DeleteQueueRequest{
		QueueName: "testqueue",
	})
	require.NoError(t, err)

	// invalid request - invalid queue name
	_, err = server.DeleteQueue(ctx, &moabpb.DeleteQueueRequest{
		QueueName: "invalid@queue",
	})
	require.Error(t, err)
}

func TestListQueuesValidation(t *testing.T) {
	server := setupMoabApiServer()
	ctx := context.Background()

	// Create some queues first
	_, err := server.CreateQueue(ctx, &moabpb.CreateQueueRequest{
		Name:                      "queue1",
		KeepaliveTimeoutInSeconds: 5,
		ExpiresInSeconds:          86400,
	})
	require.NoError(t, err)

	_, err = server.CreateQueue(ctx, &moabpb.CreateQueueRequest{
		Name:                      "queue2",
		KeepaliveTimeoutInSeconds: 5,
		ExpiresInSeconds:          86400,
	})
	require.NoError(t, err)

	// valid request
	resp, err := server.ListQueues(ctx, &moabpb.ListQueuesRequest{})
	require.NoError(t, err)
	require.NotNil(t, resp.Queues)
	require.GreaterOrEqual(t, len(resp.Queues), 2)
}

func TestEnqueueValidation(t *testing.T) {
	server := setupMoabApiServer()
	ctx := context.Background()

	// Create a queue first
	_, err := server.CreateQueue(ctx, &moabpb.CreateQueueRequest{
		Name:                      "testqueue",
		KeepaliveTimeoutInSeconds: 5,
		ExpiresInSeconds:          86400,
	})
	require.NoError(t, err)

	// valid request
	resp, err := server.Enqueue(ctx, &moabpb.EnqueueRequest{
		QueueName: "testqueue",
		Entries: []*moabpb.EnqueueRequestEntry{
			{
				Payload: []byte("test payload"),
			},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, resp.Tasks)

	// invalid request - no entries
	_, err = server.Enqueue(ctx, &moabpb.EnqueueRequest{
		QueueName: "testqueue",
		Entries:   []*moabpb.EnqueueRequestEntry{},
	})
	require.Error(t, err)
}

func TestEnqueueDefaultExpiresAtIsAnchoredAtScheduledAt(t *testing.T) {
	server := setupMoabApiServer()
	ctx := context.Background()

	_, err := server.CreateQueue(ctx, &moabpb.CreateQueueRequest{
		Name:                      "testqueue",
		KeepaliveTimeoutInSeconds: 5,
		ExpiresInSeconds:          86400, // 1 day
	})
	require.NoError(t, err)

	now := time.Now()
	scheduledAt := now.Add(5 * 24 * time.Hour).UnixNano() // 5 days out

	resp, err := server.Enqueue(ctx, &moabpb.EnqueueRequest{
		QueueName: "testqueue",
		Entries: []*moabpb.EnqueueRequestEntry{
			{
				Payload:     []byte("test payload"),
				ScheduledAt: scheduledAt,
				// ExpiresAt left at 0: must default to ScheduledAt + queue's
				// ExpiresInSeconds, not enqueue-call-time + ExpiresInSeconds
				// (which would already be behind ScheduledAt here).
			},
		},
	})
	require.NoError(t, err)
	require.Len(t, resp.Tasks, 1)

	task := resp.Tasks[0]
	wantExpiresAt := scheduledAt + int64(86400)*int64(time.Second)
	require.InDelta(t, wantExpiresAt, task.ExpiresAt, float64(time.Second))
	require.Greater(t, task.ExpiresAt, scheduledAt, "ExpiresAt must be after ScheduledAt, not already behind it")
}

func TestEnqueueRejectsScheduledAtBeyondMaxDelay(t *testing.T) {
	server := setupMoabApiServer()
	ctx := context.Background()

	_, err := server.CreateQueue(ctx, &moabpb.CreateQueueRequest{
		Name:                      "testqueue",
		KeepaliveTimeoutInSeconds: 5,
		ExpiresInSeconds:          86400,
	})
	require.NoError(t, err)

	now := time.Now()

	// Beyond DefaultServiceLimits.MaxScheduledDelayInSeconds (14 days): rejected, not clamped.
	_, err = server.Enqueue(ctx, &moabpb.EnqueueRequest{
		QueueName: "testqueue",
		Entries: []*moabpb.EnqueueRequestEntry{
			{
				Payload:     []byte("test payload"),
				ScheduledAt: now.Add(15 * 24 * time.Hour).UnixNano(),
			},
		},
	})
	require.Error(t, err)

	// At the boundary: accepted.
	resp, err := server.Enqueue(ctx, &moabpb.EnqueueRequest{
		QueueName: "testqueue",
		Entries: []*moabpb.EnqueueRequestEntry{
			{
				Payload:     []byte("test payload"),
				ScheduledAt: now.Add(14 * 24 * time.Hour).UnixNano(),
			},
		},
	})
	require.NoError(t, err)
	require.Len(t, resp.Tasks, 1)
}

func TestDequeueValidation(t *testing.T) {
	server := setupMoabApiServer()
	ctx := context.Background()

	// Create a queue first
	_, err := server.CreateQueue(ctx, &moabpb.CreateQueueRequest{
		Name:                      "testqueue",
		KeepaliveTimeoutInSeconds: 5,
		ExpiresInSeconds:          86400,
	})
	require.NoError(t, err)

	// valid request
	resp, err := server.Dequeue(ctx, &moabpb.DequeueRequest{
		QueueName: "testqueue",
		BatchSize: 10,
	})
	require.NoError(t, err)
	require.NotNil(t, resp.Tasks)

	// invalid request - invalid batch size
	_, err = server.Dequeue(ctx, &moabpb.DequeueRequest{
		QueueName: "testqueue",
		BatchSize: -1,
	})
	require.Error(t, err)
}

func TestReportStatusValidation(t *testing.T) {
	server := setupMoabApiServer()
	ctx := context.Background()

	// Create a queue first
	_, err := server.CreateQueue(ctx, &moabpb.CreateQueueRequest{
		Name:                      "testqueue",
		KeepaliveTimeoutInSeconds: 5,
		ExpiresInSeconds:          86400,
	})
	require.NoError(t, err)

	// Enqueue a task
	enqueueResp, err := server.Enqueue(ctx, &moabpb.EnqueueRequest{
		QueueName: "testqueue",
		Entries: []*moabpb.EnqueueRequestEntry{
			{
				Payload: []byte("test payload"),
			},
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, enqueueResp.Tasks)

	taskId := enqueueResp.Tasks[0].Id

	// valid request
	_, err = server.ReportStatus(ctx, &moabpb.ReportStatusRequest{
		QueueName: "testqueue",
		Entries: []*moabpb.ReportStatusRequestEntry{
			{
				TaskId:  taskId,
				Attempt: 1,
				Status:  moabpb.ReportStatusRequestEntry_STATUS_SUCCEEDED,
			},
		},
	})
	require.NoError(t, err)

	// invalid request - invalid attempt
	_, err = server.ReportStatus(ctx, &moabpb.ReportStatusRequest{
		QueueName: "testqueue",
		Entries: []*moabpb.ReportStatusRequestEntry{
			{
				TaskId:  taskId,
				Attempt: 0,
				Status:  moabpb.ReportStatusRequestEntry_STATUS_SUCCEEDED,
			},
		},
	})
	require.Error(t, err)
}

func TestDeleteTasksValidation(t *testing.T) {
	server := setupMoabApiServer()
	ctx := context.Background()

	// Create a queue first
	_, err := server.CreateQueue(ctx, &moabpb.CreateQueueRequest{
		Name:                      "testqueue",
		KeepaliveTimeoutInSeconds: 5,
		ExpiresInSeconds:          86400,
	})
	require.NoError(t, err)

	// Enqueue a task
	enqueueResp, err := server.Enqueue(ctx, &moabpb.EnqueueRequest{
		QueueName: "testqueue",
		Entries: []*moabpb.EnqueueRequestEntry{
			{
				Payload: []byte("test payload"),
			},
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, enqueueResp.Tasks)

	taskId := enqueueResp.Tasks[0].Id

	// valid request
	_, err = server.DeleteTasks(ctx, &moabpb.DeleteTasksRequest{
		QueueName: "testqueue",
		TaskIds:   []string{taskId},
	})
	require.NoError(t, err)

	// invalid request - empty task ids
	_, err = server.DeleteTasks(ctx, &moabpb.DeleteTasksRequest{
		QueueName: "testqueue",
		TaskIds:   []string{},
	})
	require.Error(t, err)
}

func TestRestartTasksValidation(t *testing.T) {
	server := setupMoabApiServer()
	ctx := context.Background()

	// Create a queue with a DLQ, so a retry-exhausted task actually lands in
	// it (rather than being deleted outright) and is restartable.
	_, err := server.CreateQueue(ctx, &moabpb.CreateQueueRequest{
		Name:                      "testqueue",
		KeepaliveTimeoutInSeconds: 5,
		ExpiresInSeconds:          86400,
		DeadLetterQueueConfig: &moabpb.DeadLetterQueueConfig{
			Enable:                   true,
			RetentionPeriodInSeconds: 86400,
		},
	})
	require.NoError(t, err)

	// Enqueue a task with no retries, so it dies on its first failure.
	enqueueResp, err := server.Enqueue(ctx, &moabpb.EnqueueRequest{
		QueueName: "testqueue",
		Entries: []*moabpb.EnqueueRequestEntry{
			{
				Payload:       []byte("test payload"),
				RetryStrategy: &moabpb.RetryStrategy{RetryIntervalsInSeconds: []int64{}},
			},
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, enqueueResp.Tasks)
	taskId := enqueueResp.Tasks[0].Id

	dequeueResp, err := server.Dequeue(ctx, &moabpb.DequeueRequest{
		QueueName: "testqueue",
		BatchSize: 10,
	})
	require.NoError(t, err)
	require.Len(t, dequeueResp.Tasks, 1)

	_, err = server.ReportStatus(ctx, &moabpb.ReportStatusRequest{
		QueueName: "testqueue",
		Entries: []*moabpb.ReportStatusRequestEntry{
			{TaskId: taskId, Attempt: dequeueResp.Tasks[0].Attempts, Status: moabpb.ReportStatusRequestEntry_STATUS_FAILED},
		},
	})
	require.NoError(t, err)

	// valid request: the task is now DEAD, so restarting it succeeds.
	restartResp, err := server.RestartTasks(ctx, &moabpb.RestartTasksRequest{
		QueueName: "testqueue",
		Entries: []*moabpb.RestartTasksRequestEntry{
			{TaskId: taskId},
		},
	})
	require.NoError(t, err)
	require.Len(t, restartResp.Entries, 1)
	require.Equal(t, moabpb.RestartTasksResponseEntry_RESULT_RESTARTED, restartResp.Entries[0].Result)
	require.NotNil(t, restartResp.Entries[0].Task)

	// Restarting it again now fails per-entry: it's ENQUEUED, not DEAD.
	restartAgainResp, err := server.RestartTasks(ctx, &moabpb.RestartTasksRequest{
		QueueName: "testqueue",
		Entries: []*moabpb.RestartTasksRequestEntry{
			{TaskId: taskId},
		},
	})
	require.NoError(t, err)
	require.Len(t, restartAgainResp.Entries, 1)
	require.Equal(t, moabpb.RestartTasksResponseEntry_RESULT_NOT_DEAD, restartAgainResp.Entries[0].Result)

	// invalid request - empty entries
	_, err = server.RestartTasks(ctx, &moabpb.RestartTasksRequest{
		QueueName: "testqueue",
		Entries:   []*moabpb.RestartTasksRequestEntry{},
	})
	require.Error(t, err)
}

func TestPurgeQueueValidation(t *testing.T) {
	server := setupMoabApiServer()
	ctx := context.Background()

	// Create a queue first
	_, err := server.CreateQueue(ctx, &moabpb.CreateQueueRequest{
		Name:                      "testqueue",
		KeepaliveTimeoutInSeconds: 5,
		ExpiresInSeconds:          86400,
	})
	require.NoError(t, err)

	// valid request
	_, err = server.PurgeQueue(ctx, &moabpb.PurgeQueueRequest{
		QueueName: "testqueue",
	})
	require.NoError(t, err)

	// invalid request - invalid queue name
	_, err = server.PurgeQueue(ctx, &moabpb.PurgeQueueRequest{
		QueueName: "invalid@queue",
	})
	require.Error(t, err)
}

func TestGetTaskValidation(t *testing.T) {
	server := setupMoabApiServer()
	ctx := context.Background()

	// Create a queue first
	_, err := server.CreateQueue(ctx, &moabpb.CreateQueueRequest{
		Name:                      "testqueue",
		KeepaliveTimeoutInSeconds: 5,
		ExpiresInSeconds:          86400,
	})
	require.NoError(t, err)

	// Enqueue a task
	enqueueResp, err := server.Enqueue(ctx, &moabpb.EnqueueRequest{
		QueueName: "testqueue",
		Entries: []*moabpb.EnqueueRequestEntry{
			{
				Payload: []byte("test payload"),
			},
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, enqueueResp.Tasks)

	taskId := enqueueResp.Tasks[0].Id

	// valid request
	resp, err := server.GetTask(ctx, &moabpb.GetTaskRequest{
		QueueName: "testqueue",
		TaskId:    taskId,
	})
	require.NoError(t, err)
	require.NotNil(t, resp.Task)

	// invalid request - empty task id
	_, err = server.GetTask(ctx, &moabpb.GetTaskRequest{
		QueueName: "testqueue",
		TaskId:    "",
	})
	require.Error(t, err)
}

func TestCreateScheduleValidation(t *testing.T) {
	server := setupMoabApiServer()
	ctx := context.Background()

	// Create a queue first
	_, err := server.CreateQueue(ctx, &moabpb.CreateQueueRequest{
		Name:                      "testqueue",
		KeepaliveTimeoutInSeconds: 5,
		ExpiresInSeconds:          86400,
	})
	require.NoError(t, err)

	// valid request
	resp, err := server.CreateSchedule(ctx, &moabpb.CreateScheduleRequest{
		QueueName:                 "testqueue",
		Name:                      "testschedule",
		Description:               "Test schedule",
		Cron:                      "0 0 * * *",
		Payload:                   []byte("test payload"),
		ExpiresInSeconds:          86400,
		KeepaliveTimeoutInSeconds: 5,
		Timezone:                  "UTC",
	})
	require.NoError(t, err)
	require.NotNil(t, resp.Schedule)

	// invalid request - invalid cron expression
	_, err = server.CreateSchedule(ctx, &moabpb.CreateScheduleRequest{
		QueueName: "testqueue",
		Name:      "testschedule",
		Cron:      "invalid cron",
		Payload:   []byte("test payload"),
		Timezone:  "America/New_York",
	})
	require.Error(t, err)
}

func TestGetScheduleValidation(t *testing.T) {
	server := setupMoabApiServer()
	ctx := context.Background()

	// Create a queue first
	_, err := server.CreateQueue(ctx, &moabpb.CreateQueueRequest{
		Name:                      "testqueue",
		KeepaliveTimeoutInSeconds: 5,
		ExpiresInSeconds:          86400,
	})
	require.NoError(t, err)

	// Create a schedule
	_, err = server.CreateSchedule(ctx, &moabpb.CreateScheduleRequest{
		QueueName: "testqueue",
		Name:      "testschedule",
		Cron:      "0 0 * * *",
		Payload:   []byte("test payload"),
		Timezone:  "America/New_York",
	})
	require.NoError(t, err)

	// valid request
	resp, err := server.GetSchedule(ctx, &moabpb.GetScheduleRequest{
		QueueName:    "testqueue",
		ScheduleName: "testschedule",
	})
	require.NoError(t, err)
	require.NotNil(t, resp.Schedule)

	// invalid request - empty schedule name
	_, err = server.GetSchedule(ctx, &moabpb.GetScheduleRequest{
		QueueName:    "testqueue",
		ScheduleName: "",
	})
	require.Error(t, err)
}

func TestUpdateScheduleValidation(t *testing.T) {
	server := setupMoabApiServer()
	ctx := context.Background()

	// Create a queue first
	_, err := server.CreateQueue(ctx, &moabpb.CreateQueueRequest{
		Name:                      "testqueue",
		KeepaliveTimeoutInSeconds: 5,
		ExpiresInSeconds:          86400,
	})
	require.NoError(t, err)

	// Create a schedule
	_, err = server.CreateSchedule(ctx, &moabpb.CreateScheduleRequest{
		QueueName: "testqueue",
		Name:      "testschedule",
		Cron:      "0 0 * * *",
		Payload:   []byte("test payload"),
		Timezone:  "America/New_York",
	})
	require.NoError(t, err)

	// valid request
	resp, err := server.UpdateSchedule(ctx, &moabpb.UpdateScheduleRequest{
		QueueName:                 "testqueue",
		ScheduleName:              "testschedule",
		Description:               "Updated description",
		Cron:                      "0 12 * * *",
		Payload:                   []byte("updated payload"),
		ExpiresInSeconds:          172800,
		KeepaliveTimeoutInSeconds: 10,
		Timezone:                  "UTC",
		ExpectedVersion:           1,
	})
	require.NoError(t, err)
	require.NotNil(t, resp.Schedule)

	// invalid request - empty schedule name
	_, err = server.UpdateSchedule(ctx, &moabpb.UpdateScheduleRequest{
		QueueName:       "testqueue",
		ScheduleName:    "",
		Cron:            "0 0 * * *",
		Payload:         []byte("test payload"),
		Timezone:        "UTC",
		ExpectedVersion: 1,
	})
	require.Error(t, err)
}

func TestDeleteScheduleValidation(t *testing.T) {
	server := setupMoabApiServer()
	ctx := context.Background()

	// Create a queue first
	_, err := server.CreateQueue(ctx, &moabpb.CreateQueueRequest{
		Name:                      "testqueue",
		KeepaliveTimeoutInSeconds: 5,
		ExpiresInSeconds:          86400,
	})
	require.NoError(t, err)

	// Create a schedule
	_, err = server.CreateSchedule(ctx, &moabpb.CreateScheduleRequest{
		QueueName: "testqueue",
		Name:      "testschedule",
		Cron:      "0 0 * * *",
		Payload:   []byte("test payload"),
		Timezone:  "America/New_York",
	})
	require.NoError(t, err)

	// valid request
	_, err = server.DeleteSchedule(ctx, &moabpb.DeleteScheduleRequest{
		QueueName:    "testqueue",
		ScheduleName: "testschedule",
	})
	require.NoError(t, err)

	// invalid request - empty schedule name
	_, err = server.DeleteSchedule(ctx, &moabpb.DeleteScheduleRequest{
		QueueName:    "testqueue",
		ScheduleName: "",
	})
	require.Error(t, err)
}

func TestListSchedulesValidation(t *testing.T) {
	server := setupMoabApiServer()
	ctx := context.Background()

	// Create a queue first
	_, err := server.CreateQueue(ctx, &moabpb.CreateQueueRequest{
		Name:                      "testqueue",
		KeepaliveTimeoutInSeconds: 5,
		ExpiresInSeconds:          86400,
	})
	require.NoError(t, err)

	// Create a couple of schedules
	_, err = server.CreateSchedule(ctx, &moabpb.CreateScheduleRequest{
		QueueName: "testqueue",
		Name:      "testschedule1",
		Cron:      "0 0 * * *",
		Timezone:  "UTC",
	})
	require.NoError(t, err)

	_, err = server.CreateSchedule(ctx, &moabpb.CreateScheduleRequest{
		QueueName: "testqueue",
		Name:      "testschedule2",
		Cron:      "0 0 * * *",
		Timezone:  "UTC",
	})
	require.NoError(t, err)

	// valid request
	resp, err := server.ListSchedules(ctx, &moabpb.ListSchedulesRequest{
		QueueName: "testqueue",
	})
	require.NoError(t, err)
	require.Len(t, resp.Schedules, 2)

	// valid request - paginated
	page1, err := server.ListSchedules(ctx, &moabpb.ListSchedulesRequest{
		QueueName: "testqueue",
		Limit:     1,
	})
	require.NoError(t, err)
	require.Len(t, page1.Schedules, 1)
	require.NotEmpty(t, page1.NextPaginationToken)

	page2, err := server.ListSchedules(ctx, &moabpb.ListSchedulesRequest{
		QueueName:       "testqueue",
		Limit:           1,
		PaginationToken: page1.NextPaginationToken,
	})
	require.NoError(t, err)
	require.Len(t, page2.Schedules, 1)
	require.NotEqual(t, page1.Schedules[0].Name, page2.Schedules[0].Name)

	// invalid request - invalid queue name
	_, err = server.ListSchedules(ctx, &moabpb.ListSchedulesRequest{
		QueueName: "invalid@queue",
	})
	require.Error(t, err)

	// invalid request - nonexistent queue
	_, err = server.ListSchedules(ctx, &moabpb.ListSchedulesRequest{
		QueueName: "nonexistentqueue",
	})
	require.Error(t, err)
}

func setupMoabApiServer() *MoabApiServer {
	dataStore, err := store.NewBadgerInMemoryStore()
	if err != nil {
		log.Fatalf("failed to create data store: %v", err)
	}

	replicaRegistry := honey.NewReplicaPrefixRegistry(dataStore)
	replicaPrefix := func(shardId string) []byte {
		prefix, err := replicaRegistry.GetOrAssignPrefix(shardId)
		if err != nil {
			log.Fatalf("failed to assign replica prefix for shard %s: %v", shardId, err)
		}
		return prefix
	}

	coresFactory := &coreapis.MoabNonclusteredApplicationCoresFactory{
		MoabQueuesCoreFactoryFunc: func(shardId string, lowerBound cluster.ShardKey, upperBound cluster.ShardKey) coreapis.MoabQueuesCoreApi {
			return queues.NewCore(dataStore, replicaPrefix(shardId), lowerBound, upperBound)
		},
		MoabTasksCoreFactoryFunc: func(shardId string, lowerBound cluster.ShardKey, upperBound cluster.ShardKey) coreapis.MoabTasksCoreApi {
			return tasks.NewCore(dataStore, replicaPrefix(shardId), lowerBound, upperBound)
		},
	}
	moabCoreApiClient := coreapis.NewMoabNonclusteredStub(16, coresFactory)

	return NewMoabApiServer(moabCoreApiClient)
}
