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

// TestDeleteQueueMarksTasksForAsynchronousCleanup pins down that DeleteQueue
// no longer just orphans a queue's tasks forever: it marks them for the same
// asynchronous purge PurgeQueue uses, so an Enqueue that still lands on the
// deleted id (from this same gateway, right after DeleteQueue — the cache
// entry DeleteQueue itself invalidates) is rejected immediately rather than
// silently writing into a queue nothing will ever look at again.
func TestDeleteQueueMarksTasksForAsynchronousCleanup(t *testing.T) {
	server := setupMoabApiServer()
	ctx := context.Background()

	_, err := server.CreateQueue(ctx, &moabpb.CreateQueueRequest{
		Name:                      "testqueue",
		KeepaliveTimeoutInSeconds: 5,
		ExpiresInSeconds:          86400,
	})
	require.NoError(t, err)

	_, err = server.Enqueue(ctx, &moabpb.EnqueueRequest{
		QueueName: "testqueue",
		Entries:   []*moabpb.EnqueueRequestEntry{{Payload: []byte("before delete")}},
	})
	require.NoError(t, err)

	_, err = server.DeleteQueue(ctx, &moabpb.DeleteQueueRequest{QueueName: "testqueue"})
	require.NoError(t, err)

	_, err = server.Enqueue(ctx, &moabpb.EnqueueRequest{
		QueueName: "testqueue",
		Entries:   []*moabpb.EnqueueRequestEntry{{Payload: []byte("after delete")}},
	})
	require.Error(t, err)

	// A brand new queue under the same name is unaffected: it gets its own
	// fresh id and starts uncounted, not haunted by the deleted queue's
	// leftover tasks.
	_, err = server.CreateQueue(ctx, &moabpb.CreateQueueRequest{
		Name:                      "testqueue",
		KeepaliveTimeoutInSeconds: 5,
		ExpiresInSeconds:          86400,
	})
	require.NoError(t, err)

	getResp, err := server.GetQueue(ctx, &moabpb.GetQueueRequest{QueueName: "testqueue"})
	require.NoError(t, err)
	require.EqualValues(t, 0, getResp.Stats.EnqueuedTasksCount)
}

// TestDeleteQueueOnAnotherGatewayRetriesStaleCache mirrors
// TestPurgeQueueOnAnotherGatewayRetriesStaleCache for DeleteQueue: two
// independent MoabApiServer instances (independent queuesCache each) share
// one cluster backend, modeling two gateway processes. Unlike a purge, a
// delete has no fresh id to retry onto — the queue is gone for good — so
// the correct outcome for gatewayB's stale-cache Enqueue is an error, not a
// silent write into an id nothing will ever read from again.
func TestDeleteQueueOnAnotherGatewayRetriesStaleCache(t *testing.T) {
	client := setupMoabCoreApiClient()
	gatewayA := NewMoabApiServer(client)
	gatewayB := NewMoabApiServer(client)
	ctx := context.Background()

	_, err := gatewayA.CreateQueue(ctx, &moabpb.CreateQueueRequest{
		Name:                      "testqueue",
		KeepaliveTimeoutInSeconds: 5,
		ExpiresInSeconds:          86400,
	})
	require.NoError(t, err)

	// Warms gatewayB's queuesCache with the queue and its (soon to be
	// deleted) id.
	_, err = gatewayB.Enqueue(ctx, &moabpb.EnqueueRequest{
		QueueName: "testqueue",
		Entries:   []*moabpb.EnqueueRequestEntry{{Payload: []byte("before delete, via gatewayB")}},
	})
	require.NoError(t, err)

	// gatewayA deletes the queue. It only invalidates its own cache.
	_, err = gatewayA.DeleteQueue(ctx, &moabpb.DeleteQueueRequest{QueueName: "testqueue"})
	require.NoError(t, err)

	// gatewayB still has the pre-delete queue cached. Its next Enqueue must
	// fail — not silently succeed into an id that's being asynchronously
	// drained and that no name will ever resolve to again.
	_, err = gatewayB.Enqueue(ctx, &moabpb.EnqueueRequest{
		QueueName: "testqueue",
		Entries:   []*moabpb.EnqueueRequestEntry{{Payload: []byte("after delete, via gatewayB's stale cache")}},
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

// TestPurgeQueueRotatesQueueIdInsteadOfDeletingTasksSynchronously exercises
// PurgeQueue end to end through the server: rotating the queue's internal id
// makes its pre-purge tasks unreachable through the API immediately (GetTask
// resolves through the queue's *current* id), without needing the async GC
// worker to have run yet, while a task enqueued right after the purge is
// completely unaffected and starts counting from a clean slate.
func TestPurgeQueueRotatesQueueIdInsteadOfDeletingTasksSynchronously(t *testing.T) {
	server := setupMoabApiServer()
	ctx := context.Background()

	_, err := server.CreateQueue(ctx, &moabpb.CreateQueueRequest{
		Name:                      "testqueue",
		KeepaliveTimeoutInSeconds: 5,
		ExpiresInSeconds:          86400,
	})
	require.NoError(t, err)

	enqueueResp, err := server.Enqueue(ctx, &moabpb.EnqueueRequest{
		QueueName: "testqueue",
		Entries: []*moabpb.EnqueueRequestEntry{
			{Payload: []byte("before purge")},
		},
	})
	require.NoError(t, err)
	require.Len(t, enqueueResp.Tasks, 1)
	oldTaskId := enqueueResp.Tasks[0].Id

	_, err = server.PurgeQueue(ctx, &moabpb.PurgeQueueRequest{QueueName: "testqueue"})
	require.NoError(t, err)

	// The pre-purge task is unreachable right away, before any async GC has
	// had a chance to run: GetTask resolves "testqueue" to its new id, under
	// which this task id was never enqueued.
	_, err = server.GetTask(ctx, &moabpb.GetTaskRequest{
		QueueName: "testqueue",
		TaskId:    oldTaskId,
	})
	require.Error(t, err)

	// A brand new task enqueued right after the purge is unaffected, and
	// statistics start clean rather than inheriting anything from the old id.
	afterResp, err := server.Enqueue(ctx, &moabpb.EnqueueRequest{
		QueueName: "testqueue",
		Entries: []*moabpb.EnqueueRequestEntry{
			{Payload: []byte("after purge")},
		},
	})
	require.NoError(t, err)
	require.Len(t, afterResp.Tasks, 1)

	getResp, err := server.GetQueue(ctx, &moabpb.GetQueueRequest{QueueName: "testqueue"})
	require.NoError(t, err)
	require.EqualValues(t, 1, getResp.Stats.EnqueuedTasksCount)

	_, err = server.GetTask(ctx, &moabpb.GetTaskRequest{
		QueueName: "testqueue",
		TaskId:    afterResp.Tasks[0].Id,
	})
	require.NoError(t, err)
}

// TestPurgeQueueOnAnotherGatewayRetriesStaleCache is the scenario a single
// process can't exercise: PurgeQueue's own queuesCache invalidation only
// covers the gateway that handled the call. This spins up two independent
// MoabApiServer instances (independent queuesCache each) sharing one
// cluster backend to model two gateway processes, warms gatewayB's cache
// with the pre-purge queue, purges through gatewayA, and checks that
// gatewayB's very next Enqueue still succeeds — transparently, via
// TasksCore's NotFound-on-purged-id plus the handler's retry — landing on
// the new id instead of the one being asynchronously drained.
func TestPurgeQueueOnAnotherGatewayRetriesStaleCache(t *testing.T) {
	client := setupMoabCoreApiClient()
	gatewayA := NewMoabApiServer(client)
	gatewayB := NewMoabApiServer(client)
	ctx := context.Background()

	_, err := gatewayA.CreateQueue(ctx, &moabpb.CreateQueueRequest{
		Name:                      "testqueue",
		KeepaliveTimeoutInSeconds: 5,
		ExpiresInSeconds:          86400,
	})
	require.NoError(t, err)

	// Warms gatewayB's queuesCache with the pre-purge queue and its id.
	_, err = gatewayB.Enqueue(ctx, &moabpb.EnqueueRequest{
		QueueName: "testqueue",
		Entries:   []*moabpb.EnqueueRequestEntry{{Payload: []byte("before purge, via gatewayB")}},
	})
	require.NoError(t, err)

	// gatewayA purges the queue. It only invalidates its own cache — it has
	// no way to reach into gatewayB's.
	_, err = gatewayA.PurgeQueue(ctx, &moabpb.PurgeQueueRequest{QueueName: "testqueue"})
	require.NoError(t, err)

	// gatewayB still has the pre-purge queue cached; its next Enqueue must
	// still land correctly, by retrying against a freshly resolved queue
	// once TasksCore rejects the stale id.
	resp, err := gatewayB.Enqueue(ctx, &moabpb.EnqueueRequest{
		QueueName: "testqueue",
		Entries:   []*moabpb.EnqueueRequestEntry{{Payload: []byte("after purge, via gatewayB's stale cache")}},
	})
	require.NoError(t, err)
	require.Len(t, resp.Tasks, 1)

	// Confirms it landed under the new id, not the one being drained: stats
	// (fetched via gatewayA, unrelated to which gateway wrote it) show
	// exactly the one post-purge task.
	getResp, err := gatewayA.GetQueue(ctx, &moabpb.GetQueueRequest{QueueName: "testqueue"})
	require.NoError(t, err)
	require.EqualValues(t, 1, getResp.Stats.EnqueuedTasksCount)
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

func TestListTasksValidation(t *testing.T) {
	server := setupMoabApiServer()
	ctx := context.Background()

	_, err := server.CreateQueue(ctx, &moabpb.CreateQueueRequest{
		Name:                      "testqueue",
		KeepaliveTimeoutInSeconds: 5,
		ExpiresInSeconds:          86400,
	})
	require.NoError(t, err)

	enqueueResp, err := server.Enqueue(ctx, &moabpb.EnqueueRequest{
		QueueName: "testqueue",
		Entries: []*moabpb.EnqueueRequestEntry{
			{Payload: []byte("payload-1")},
			{Payload: []byte("payload-2")},
		},
	})
	require.NoError(t, err)
	require.Len(t, enqueueResp.Tasks, 2)

	// valid request - unfiltered lists both
	resp, err := server.ListTasks(ctx, &moabpb.ListTasksRequest{
		QueueName: "testqueue",
	})
	require.NoError(t, err)
	require.Len(t, resp.Tasks, 2)
	require.Equal(t, moabpb.TaskState_TASK_STATE_ENQUEUED, resp.Tasks[0].State)

	// valid request - filtered by state
	dequeueResp, err := server.Dequeue(ctx, &moabpb.DequeueRequest{
		QueueName: "testqueue",
		BatchSize: 2,
	})
	require.NoError(t, err)
	require.Len(t, dequeueResp.Tasks, 2)

	inProgressResp, err := server.ListTasks(ctx, &moabpb.ListTasksRequest{
		QueueName: "testqueue",
		State:     moabpb.TaskState_TASK_STATE_IN_PROGRESS,
	})
	require.NoError(t, err)
	require.Len(t, inProgressResp.Tasks, 2)

	deadResp, err := server.ListTasks(ctx, &moabpb.ListTasksRequest{
		QueueName: "testqueue",
		State:     moabpb.TaskState_TASK_STATE_DEAD,
	})
	require.NoError(t, err)
	require.Empty(t, deadResp.Tasks)

	// invalid request - invalid queue name
	_, err = server.ListTasks(ctx, &moabpb.ListTasksRequest{
		QueueName: "invalid@queue",
	})
	require.Error(t, err)

	// invalid request - unrecognized state
	_, err = server.ListTasks(ctx, &moabpb.ListTasksRequest{
		QueueName: "testqueue",
		State:     moabpb.TaskState(99),
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

// setupMoabCoreApiClient builds the shared cluster-side backend (an
// in-memory Badger store plus MoabQueues/MoabTasks cores). Every
// MoabApiServer built from the same client behaves like an independent
// gateway process talking to the same cluster: each gets its own handler
// and its own queuesCache, but they all see the same underlying queue/task
// state.
func setupMoabCoreApiClient() coreapis.MoabClientApi {
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
	return coreapis.NewMoabNonclusteredStub(16, coresFactory)
}

func setupMoabApiServer() *MoabApiServer {
	return NewMoabApiServer(setupMoabCoreApiClient())
}
