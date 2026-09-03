package tasks

import (
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/evrblk/monstera"
	"github.com/evrblk/monstera/cluster"
	mrpc "github.com/evrblk/monstera/rpc"
	"github.com/evrblk/monstera/store"
	"github.com/evrblk/yellowstone-common/honey"

	"github.com/evrblk/moab/pkg/coreapis"
	"github.com/evrblk/moab/pkg/corepb"
	"github.com/evrblk/moab/pkg/ids"
	"github.com/evrblk/moab/pkg/sharding"
)

const (
	// maxVisitedTasksForDequeuing bounds, per Dequeue call, how many index
	// entries dequeueInProgressTasksBeforeTime/dequeueTasksBeforeTime will
	// visit even if none of them are dequeuable (e.g. because
	// MaxInProgressTasks is already reached), so a busy queue cannot make a
	// single Dequeue call scan unboundedly.
	maxVisitedTasksForDequeuing = 100

	defaultGCMaxVisitedTasks = 1000
	defaultGCPageSize        = 250
)

// Core is the application core for the Tasks subsystem. It owns tasks, their
// per-thread head-of-line ordering, dequeuing rate limiters, and per-queue
// counters and task id sequence for one shard, storing them in the shared
// BadgerDB store under its own replica prefix, and implements
// coreapis.MoabTasksCoreApi.
type Core struct {
	badgerStore *store.BadgerStore

	replicaPrefix   []byte
	shardLowerBound cluster.ShardKey
	shardUpperBound cluster.ShardKey

	tasks        *tasksTable
	threads      *threadsTable
	counters     *countersTable
	queueState   *queueStateTable
	rateLimiters *rateLimitersTable
}

var _ coreapis.MoabTasksCoreApi = &Core{}

// NewCore constructs a Core scoped to [shardLowerBound, shardUpperBound],
// namespacing all of its keys in badgerStore under replicaPrefix so that
// multiple cores can safely share the same underlying store.
func NewCore(badgerStore *store.BadgerStore, replicaPrefix []byte, shardLowerBound cluster.ShardKey, shardUpperBound cluster.ShardKey) *Core {
	return &Core{
		badgerStore: badgerStore,

		replicaPrefix:   replicaPrefix,
		shardLowerBound: shardLowerBound,
		shardUpperBound: shardUpperBound,

		tasks:        newTasksTable(replicaPrefix),
		threads:      newThreadsTable(replicaPrefix),
		counters:     newCountersTable(replicaPrefix),
		queueState:   newQueueStateTable(replicaPrefix),
		rateLimiters: newRateLimitersTable(replicaPrefix),
	}
}

func (c *Core) snapshotSections() []honey.Section {
	return []honey.Section{
		// Threads before Tasks: a restored task's queueIndex placement
		// depends on knowing its thread's current head, so the thread
		// records must already be in place by the time tasks are restored.
		{Name: "Threads", Table: c.threads},
		{Name: "Tasks", Table: &taskRestorer{core: c}},
		{Name: "Counters", Table: c.counters},
		{Name: "QueueState", Table: c.queueState},
		{Name: "RateLimiters", Table: c.rateLimiters},
	}
}

// taskRestorer adapts tasksTable to honey.PortableTable for snapshot
// restore. tasksTable itself holds no reference to threadsTable (the two
// are independent leaf tables), so only Core — which holds both — can
// resolve a restored threaded task's head status.
type taskRestorer struct {
	core *Core
}

func (r *taskRestorer) EachEntity(txn *store.Txn, fn func(key []byte, value []byte) (bool, error)) error {
	return r.core.tasks.EachEntity(txn, fn)
}

func (r *taskRestorer) Clear(badgerStore *store.BadgerStore) error {
	return r.core.tasks.Clear(badgerStore)
}

// RestoreEntity decodes one streamed task and, if owned, inserts it and
// re-derives every secondary index entry based on its persisted state.
func (r *taskRestorer) RestoreEntity(txn *store.Txn, key []byte, value []byte, bounds honey.ShardRange) (bool, error) {
	task := &corepb.Task{}
	if err := task.UnmarshalBinary(value); err != nil {
		return false, err
	}
	if !bounds.Owns(sharding.ByAccountAndQueue(task.Id.AccountId, task.Id.QueueId)) {
		return false, nil
	}

	isHead := true
	if task.ThreadId != "" {
		if err := r.core.threads.AddToIndex(txn, task.Id.AccountId, task.Id.QueueId, task.ThreadId, task.ScheduledAt, task.Id.TaskId); err != nil {
			return false, err
		}

		thread, err := r.core.threads.Get(txn, task.Id.AccountId, task.Id.QueueId, task.ThreadId)
		if err != nil {
			return false, err
		}
		isHead = thread.HeadTaskId.TaskId == task.Id.TaskId
	}

	return true, r.core.tasks.restoreIndexes(txn, task, isHead)
}

// Snapshot returns a consistent, portable snapshot of this core's primary
// entities (a pinned view; Write streams from it concurrently with
// subsequent updates).
func (c *Core) Snapshot() monstera.ApplicationCoreSnapshot {
	return honey.NewSnapshot(c.badgerStore, "MoabTasks", c.snapshotSections())
}

// Restore replaces this core's state with the union of the entities from the
// given streams that belong to this core's shard bounds (one stream for a
// Raft restore or split seed, two for a merge seed), inserting them through
// the tables' own methods (which rebuild all secondary indexes under this
// core's prefix).
func (c *Core) Restore(readers ...io.ReadCloser) error {
	return honey.RestoreSnapshot(c.badgerStore, c.snapshotSections(),
		honey.ShardRange{Lower: c.shardLowerBound, Upper: c.shardUpperBound}, readers...)
}

// Close releases any Core-owned resources. The underlying Badger store is
// shared across cores and is not closed here.
func (c *Core) Close() {

}

// GetTask returns a task by ID. It returns a NotFound application error if
// no task with that ID exists, or if it exists but has already expired and
// is only waiting on RunTasksGarbageCollection to be swept.
func (c *Core) GetTask(req *coreapis.GetTaskRequest) (*coreapis.GetTaskResponse, error) {
	txn := c.badgerStore.View()
	defer txn.Discard()

	task, err := c.tasks.Get(txn, req.Payload.TaskId)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return &coreapis.GetTaskResponse{ApplicationError: taskNotFoundError(req.Payload.TaskId)}, nil
		}
		return nil, err
	}

	// Do not return a task that is expired but not yet GC'ed. ExpiresAt is
	// never enforced against an IN_PROGRESS task — it isn't tracked in the
	// expiration index while active, and a task genuinely still being worked
	// must never be hidden as "expired."
	if task.State != corepb.TaskState_TASK_STATE_IN_PROGRESS && task.ExpiresAt < req.Now {
		return &coreapis.GetTaskResponse{ApplicationError: taskNotFoundError(req.Payload.TaskId)}, nil
	}

	return &coreapis.GetTaskResponse{
		Payload: &corepb.GetTaskResponse{Task: task},
	}, nil
}

// GetStatistics returns a queue's task counters (enqueued, in-progress, and
// dead), the age of its oldest still-enqueued task, and its current task id
// sequence.
func (c *Core) GetStatistics(req *coreapis.GetStatisticsRequest) (*coreapis.GetStatisticsResponse, error) {
	txn := c.badgerStore.View()
	defer txn.Discard()

	accountId, queueId := req.Payload.QueueId.AccountId, req.Payload.QueueId.QueueId

	counters, err := c.counters.Get(txn, accountId, queueId)
	if err != nil {
		return nil, err
	}

	state, err := c.queueState.Get(txn, accountId, queueId)
	if err != nil {
		return nil, err
	}

	ageOfOldestEnqueuedTask, err := c.getAgeOfOldestEnqueuedTask(txn, accountId, queueId, req.Now)
	if err != nil {
		return nil, err
	}

	return &coreapis.GetStatisticsResponse{
		Payload: &corepb.GetStatisticsResponse{
			EnqueuedTasksCount:      counters.EnqueuedTasksCount,
			InProgressTasksCount:    counters.InProgressTasksCount,
			DeadTasksCount:          counters.DeadTasksCount,
			ProcessedTasksCount:     counters.ProcessedTasksCount,
			ExpiredTasksCount:       counters.ExpiredTasksCount,
			AgeOfOldestEnqueuedTask: ageOfOldestEnqueuedTask,
			TaskIdSequence:          int64(state.TaskIdSequence),
		},
	}, nil
}

// Enqueue creates a new task for each entry, except that an entry whose
// DedupeKey already points at a live task applies entry.OverwriteOnDuplicate
// to that existing task instead of creating a new one. An entry scheduled in
// the past (or not scheduled at all) is enqueued as scheduled at req.Now.
func (c *Core) Enqueue(req *coreapis.EnqueueRequest) (*coreapis.EnqueueResponse, error) {
	txn := c.badgerStore.Update()
	defer txn.Discard()

	accountId, queueId := req.Payload.QueueId.AccountId, req.Payload.QueueId.QueueId

	// Affected (enqueued or overwritten) tasks to be returned.
	tasks := make([]*corepb.Task, 0, len(req.Payload.Entries))

	state, err := c.queueState.Get(txn, accountId, queueId)
	if err != nil {
		return nil, err
	}

	for _, entry := range req.Payload.Entries {
		// If a task is scheduled in the past or not scheduled at all.
		// Equivalent to max(entry.ScheduledAt, req.Now).
		scheduledAt := entry.ScheduledAt
		if scheduledAt < req.Now {
			scheduledAt = req.Now
		}

		if entry.DedupeKey != "" {
			duplicateId, ok, err := c.getTaskIdByDedupeKey(txn, accountId, queueId, entry.DedupeKey)
			if err != nil {
				return nil, err
			}

			if ok {
				duplicate, err := c.overwriteDuplicate(txn, duplicateId, entry.OverwriteOnDuplicate, scheduledAt, entry.Payload, entry.ExpiresAt)
				if err != nil {
					return nil, err
				}

				if duplicate != nil {
					tasks = append(tasks, duplicate)
				}

				// A task already exists in the main index; no need to update counters or the task id sequence.
				continue
			}
		}

		state.TaskIdSequence = state.TaskIdSequence + 1

		task := &corepb.Task{
			Id: &corepb.TaskId{
				AccountId: accountId,
				QueueId:   queueId,
				TaskId:    state.TaskIdSequence,
			},
			Payload:                   entry.Payload,
			CreatedAt:                 req.Now,
			ScheduledAt:               scheduledAt,
			State:                     corepb.TaskState_TASK_STATE_ENQUEUED,
			Attempts:                  0,
			ExpiresAt:                 entry.ExpiresAt,
			DedupeKey:                 entry.DedupeKey,
			ThreadId:                  entry.ThreadId,
			VisibleAt:                 0,
			LastFailedAt:              0,
			RetryStrategy:             entry.RetryStrategy,
			KeepaliveTimeoutInSeconds: entry.KeepaliveTimeoutInSeconds,
		}

		if err := c.createTask(txn, task); err != nil {
			return nil, err
		}

		tasks = append(tasks, task)
	}

	if err := c.queueState.Set(txn, accountId, queueId, state); err != nil {
		return nil, err
	}

	if err := txn.Commit(); err != nil {
		return nil, err
	}

	return &coreapis.EnqueueResponse{
		Payload: &corepb.EnqueueResponse{Tasks: tasks},
	}, nil
}

// Dequeue returns up to req.Payload.DequeueLimit tasks ready to be worked.
// It first routes any in-progress task whose keepalive timeout has lapsed
// through the same failure handling an explicit ReportStatus(FAILED) would
// get — none of those are handed back directly here, but a retry
// rescheduled with a short/zero backoff can become visible to the pull
// below within this same transaction — then pulls fresh tasks from the
// front of the queue. When req.Payload.DequeuingSettings sets a rate limit
// or a max-in-progress bound, both are enforced against this call's yield.
func (c *Core) Dequeue(req *coreapis.DequeueRequest) (*coreapis.DequeueResponse, error) {
	txn := c.badgerStore.Update()
	defer txn.Discard()

	accountId, queueId := req.Payload.QueueId.AccountId, req.Payload.QueueId.QueueId

	dequeueLimit := int64(req.Payload.DequeueLimit)
	var rateLimiterState *corepb.RateLimiterState

	rateLimitingEnabled := req.Payload.DequeuingSettings != nil &&
		req.Payload.DequeuingSettings.RateLimiting != nil && req.Payload.DequeuingSettings.RateLimiting.Interval > 0 &&
		req.Payload.DequeuingSettings.RateLimiting.MaxTokens > 0
	if rateLimitingEnabled {
		state, err := c.rateLimiters.GetOrDefault(txn, accountId, queueId, req.Payload.DequeuingSettings.RateLimiting, req.Now)
		if err != nil {
			return nil, err
		}

		maxTokens := req.Payload.DequeuingSettings.RateLimiting.MaxTokens
		refillInterval := getRefillInterval(req.Payload.DequeuingSettings.RateLimiting)
		if state.Tokens < maxTokens {
			newTokens := (req.Now - state.LastRefilledAt) / refillInterval
			if newTokens > 0 {
				if state.Tokens+newTokens > maxTokens {
					// The bucket is full, LastRefilledAt time does not really matter.
					state.Tokens = maxTokens
					state.LastRefilledAt = req.Now
				} else {
					state.Tokens += newTokens
					state.LastRefilledAt += newTokens * refillInterval
				}
			}
		} else {
			// The bucket was full already, just advance LastRefilledAt time forward.
			state.LastRefilledAt = req.Now
		}
		rateLimiterState = state

		if rateLimiterState.Tokens < dequeueLimit {
			dequeueLimit = rateLimiterState.Tokens
		}
	}

	// First route any in-progress task whose keepalive has lapsed through the
	// unified failure handler — none are returned directly (see the doc
	// comment above and handleTaskFailure).
	if err := c.dequeueInProgressTasksBeforeTime(txn, accountId, queueId, req.Now, req.Payload.DeadLetterQueueConfig); err != nil {
		return nil, err
	}

	tasks, err := c.dequeueTasksBeforeTime(txn, accountId, queueId, req.Now, dequeueLimit, req.Payload.DequeuingSettings)
	if err != nil {
		return nil, err
	}
	tasksCount := int64(len(tasks))

	if rateLimiterState != nil {
		rateLimiterState.Tokens -= tasksCount
		if err := c.rateLimiters.Set(txn, accountId, queueId, rateLimiterState); err != nil {
			return nil, err
		}
	}

	if err := txn.Commit(); err != nil {
		return nil, err
	}

	return &coreapis.DequeueResponse{
		Payload: &corepb.DequeueResponse{Tasks: tasks},
	}, nil
}

// ReportStatus applies a worker's outcome for each entry to the
// corresponding task: STATUS_IN_PROGRESS extends its keepalive deadline,
// STATUS_SUCCEEDED deletes it, and STATUS_FAILED either schedules a retry
// per its RetryStrategy or moves it to dead once retries are exhausted. An
// entry for a task that no longer exists, or that is not currently in
// progress, is silently ignored.
func (c *Core) ReportStatus(req *coreapis.ReportStatusRequest) (*coreapis.ReportStatusResponse, error) {
	txn := c.badgerStore.Update()
	defer txn.Discard()

	for _, entry := range req.Payload.Entries {
		if err := c.reportStatus(txn, entry.TaskId, req.Now, entry.Status, entry.Attempt, req.Payload.DeadLetterQueueConfig); err != nil {
			return nil, err
		}
	}

	if err := txn.Commit(); err != nil {
		return nil, err
	}

	return &coreapis.ReportStatusResponse{
		Payload: &corepb.ReportStatusResponse{},
	}, nil
}

// DeleteTasks deletes the given task ids from a queue, regardless of their
// current state. Task ids that do not exist are silently ignored.
func (c *Core) DeleteTasks(req *coreapis.DeleteTasksRequest) (*coreapis.DeleteTasksResponse, error) {
	txn := c.badgerStore.Update()
	defer txn.Discard()

	accountId, queueId := req.Payload.QueueId.AccountId, req.Payload.QueueId.QueueId

	for _, taskId := range req.Payload.TaskIds {
		task, err := c.tasks.Get(txn, &corepb.TaskId{AccountId: accountId, QueueId: queueId, TaskId: taskId})
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				// Task not found, ignore.
				continue
			}
			return nil, err
		}

		if err := c.deleteTask(txn, task, taskDeletionNone); err != nil {
			return nil, err
		}
	}

	if err := txn.Commit(); err != nil {
		return nil, err
	}

	return &coreapis.DeleteTasksResponse{
		Payload: &corepb.DeleteTasksResponse{},
	}, nil
}

// RestartTasks resets each given DEAD task back to a fresh ENQUEUED arrival
// — exactly like a new Enqueue of the same payload — unless it can't be
// restarted, which is reported per task id rather than silently ignored
// (unlike DeleteTasks, a restart has outcomes an operator genuinely needs to
// see): not found, not DEAD, or its DedupeKey is currently claimed by a
// different live task (a dead task's dedupe key is released immediately on
// death so a fresh task can reuse it right away, so by the time someone
// restarts the old one, a different live task may have already claimed the
// key — restart must re-check, not assume it's still free).
func (c *Core) RestartTasks(req *coreapis.RestartTasksRequest) (*coreapis.RestartTasksResponse, error) {
	txn := c.badgerStore.Update()
	defer txn.Discard()

	accountId, queueId := req.Payload.QueueId.AccountId, req.Payload.QueueId.QueueId

	entries := make([]*corepb.RestartTasksResponseEntry, 0, len(req.Payload.Entries))

	for _, e := range req.Payload.Entries {
		task, err := c.tasks.Get(txn, &corepb.TaskId{AccountId: accountId, QueueId: queueId, TaskId: e.TaskId})
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				entries = append(entries, &corepb.RestartTasksResponseEntry{
					TaskId: e.TaskId,
					Result: corepb.RestartTasksResponseEntry_RESULT_NOT_FOUND,
				})
				continue
			}
			return nil, err
		}

		if task.State != corepb.TaskState_TASK_STATE_DEAD {
			entries = append(entries, &corepb.RestartTasksResponseEntry{
				TaskId: e.TaskId,
				Result: corepb.RestartTasksResponseEntry_RESULT_NOT_DEAD,
			})
			continue
		}

		// Its DLQ retention deadline has already passed — GC just hasn't
		// swept it yet. Treat it as already gone, consistent with what
		// GetTask/GC would already say.
		if task.ExpiresAt < req.Now {
			entries = append(entries, &corepb.RestartTasksResponseEntry{
				TaskId: e.TaskId,
				Result: corepb.RestartTasksResponseEntry_RESULT_NOT_FOUND,
			})
			continue
		}

		if task.DedupeKey != "" {
			conflictId, ok, err := c.getTaskIdByDedupeKey(txn, accountId, queueId, task.DedupeKey)
			if err != nil {
				return nil, err
			}
			if ok && conflictId.TaskId != task.Id.TaskId {
				entries = append(entries, &corepb.RestartTasksResponseEntry{
					TaskId: e.TaskId,
					Result: corepb.RestartTasksResponseEntry_RESULT_DEDUPE_CONFLICT,
				})
				continue
			}
		}

		if err := c.restartTask(txn, task, req.Now, e.ScheduledAt, e.ExpiresAt); err != nil {
			return nil, err
		}

		entries = append(entries, &corepb.RestartTasksResponseEntry{
			TaskId: e.TaskId,
			Result: corepb.RestartTasksResponseEntry_RESULT_RESTARTED,
			Task:   task,
		})
	}

	if err := txn.Commit(); err != nil {
		return nil, err
	}

	return &coreapis.RestartTasksResponse{
		Payload: &corepb.RestartTasksResponse{Entries: entries},
	}, nil
}

// restartTask resets a DEAD task to a fresh ENQUEUED arrival: Attempts back
// to 0, a brand new ScheduledAt/ExpiresAt already resolved by the caller
// (mirroring a fresh Enqueue, since this core has no way to compute
// now + Queue.ExpiresInSeconds itself), the dedupe key reclaimed, and
// re-attached to its thread exactly like a new arrival if threaded. Mutates
// task in place.
func (c *Core) restartTask(txn *store.Txn, task *corepb.Task, now, scheduledAt, expiresAt int64) error {
	if err := c.tasks.deadTasksIndex.Delete(txn, c.tasks.tablePK(task.Id.AccountId, task.Id.QueueId), deadTasksIndexItem(task.LastFailedAt, task.Id.TaskId)); err != nil {
		return err
	}
	// Drop the DLQ retention deadline this row is indexed under before
	// createTask (below) indexes it under its new delivery deadline — a task
	// only ever has one active deadline tracked at a time (its delivery
	// deadline while alive, its retention deadline while dead), never both.
	if err := c.tasks.expiration.remove(txn, task.Id, task.ExpiresAt); err != nil {
		return err
	}

	counters, err := c.counters.Get(txn, task.Id.AccountId, task.Id.QueueId)
	if err != nil {
		return err
	}
	counters.DeadTasksCount--
	if err := c.counters.Set(txn, task.Id.AccountId, task.Id.QueueId, counters); err != nil {
		return err
	}

	if scheduledAt < now {
		scheduledAt = now
	}

	task.State = corepb.TaskState_TASK_STATE_ENQUEUED
	task.Attempts = 0
	task.ScheduledAt = scheduledAt
	task.ExpiresAt = expiresAt
	task.VisibleAt = 0
	task.LastFailedAt = 0

	return c.createTask(txn, task)
}

// PurgeQueue deletes every task in a queue, regardless of state.
func (c *Core) PurgeQueue(req *coreapis.PurgeQueueRequest) (*coreapis.PurgeQueueResponse, error) {
	txn := c.badgerStore.Update()
	defer txn.Discard()

	accountId, queueId := req.Payload.QueueId.AccountId, req.Payload.QueueId.QueueId

	// TODO implement more efficient purge
	tasks, err := c.tasks.ListAll(txn, accountId, queueId)
	if err != nil {
		return nil, err
	}

	for _, task := range tasks {
		if err := c.deleteTask(txn, task, taskDeletionNone); err != nil {
			return nil, err
		}
	}

	if err := txn.Commit(); err != nil {
		return nil, err
	}

	return &coreapis.PurgeQueueResponse{
		Payload: &corepb.PurgeQueueResponse{},
	}, nil
}

// RunTasksGarbageCollection sweeps tasks whose ExpiresAt has passed, regardless
// of their current state, bounded by req.Payload.MaxVisitedTasks (total per
// call, default defaultGCMaxVisitedTasks) fetched from the expiration index
// req.Payload.PageSize entries at a time (default defaultGCPageSize).
func (c *Core) RunTasksGarbageCollection(req *coreapis.RunTasksGarbageCollectionRequest) (*coreapis.RunTasksGarbageCollectionResponse, error) {
	txn := c.badgerStore.Update()
	defer txn.Discard()

	maxVisitedTasks := int(req.Payload.MaxVisitedTasks)
	if maxVisitedTasks <= 0 {
		maxVisitedTasks = defaultGCMaxVisitedTasks
	}
	pageSize := int(req.Payload.PageSize)
	if pageSize <= 0 {
		pageSize = defaultGCPageSize
	}
	if pageSize > maxVisitedTasks {
		pageSize = maxVisitedTasks
	}

	visited := 0
	for visited < maxVisitedTasks {
		limit := pageSize
		if remaining := maxVisitedTasks - visited; limit > remaining {
			limit = remaining
		}

		expired, err := c.tasks.expiration.ListDue(txn, req.Now, limit)
		if err != nil {
			return nil, err
		}
		if len(expired) == 0 {
			break
		}

		for _, ref := range expired {
			task, err := c.tasks.Get(txn, ref.TaskId)
			if err != nil {
				if errors.Is(err, store.ErrNotFound) {
					// The expiration index outlived its task (should not
					// normally happen); drop the stale entry.
					if err := c.tasks.expiration.remove(txn, ref.TaskId, ref.ExpiresAt); err != nil {
						return nil, err
					}
					continue
				}
				return nil, err
			}

			if err := c.deleteTask(txn, task, taskDeletionExpired); err != nil {
				return nil, err
			}
		}

		visited += len(expired)
		if len(expired) < limit {
			break
		}
	}

	if err := txn.Commit(); err != nil {
		return nil, err
	}

	return &coreapis.RunTasksGarbageCollectionResponse{
		Payload: &corepb.RunTasksGarbageCollectionResponse{},
	}, nil
}

func (c *Core) reportStatus(txn *store.Txn, taskId *corepb.TaskId, now int64, status corepb.ReportStatusRequestEntry_Status, attempt int32, dlqConfig *corepb.DeadLetterQueueConfig) error {
	task, err := c.tasks.Get(txn, taskId)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			// Task not found, ignore.
			return nil
		}
		return err
	}

	// Only in-progress tasks can be heartbeated; enqueued/dead tasks ignore any report.
	if task.State != corepb.TaskState_TASK_STATE_IN_PROGRESS {
		return nil
	}

	// A stale report — the worker's lease already moved on (redelivered after
	// a keepalive timeout, or this task id was deleted and later restarted
	// under a fresh generation) — is a silent no-op, never a mutation.
	if task.Attempts != attempt {
		return nil
	}

	switch status {
	case corepb.ReportStatusRequestEntry_STATUS_IN_PROGRESS:
		return c.heartbeatTask(txn, task, now)
	case corepb.ReportStatusRequestEntry_STATUS_FAILED:
		return c.handleTaskFailure(txn, task, now, dlqConfig)
	case corepb.ReportStatusRequestEntry_STATUS_SUCCEEDED:
		return c.deleteTask(txn, task, taskDeletionSucceeded)
	}

	return nil
}

// handleTaskFailure applies the outcome of one failed delivery attempt to an
// IN_PROGRESS task. It is invoked either by an explicit
// ReportStatus(FAILED), or by dequeueInProgressTasksBeforeTime discovering a
// lease nobody renewed before its keepalive timeout — the two are treated
// identically, as the same implicit failure. ExpiresAt is checked first: an
// already-expired task is deleted outright, regardless of remaining retry
// budget or DLQ configuration, and never retried or dead-lettered. Otherwise
// it's routed to failTaskToDead (retry budget exhausted) or failTaskToRetry
// (reschedule with backoff). dlqConfig is resolved by the server from
// Queue, same as dequeuingSettings elsewhere — this core never reads queue
// config itself.
func (c *Core) handleTaskFailure(txn *store.Txn, task *corepb.Task, now int64, dlqConfig *corepb.DeadLetterQueueConfig) error {
	if task.ExpiresAt < now {
		return c.deleteTask(txn, task, taskDeletionExpired)
	}

	if task.RetryStrategy == nil || task.Attempts > int32(len(task.RetryStrategy.RetryIntervalsInSeconds)) {
		return c.failTaskToDead(txn, task, now, dlqConfig)
	}

	return c.failTaskToRetry(txn, task, now)
}

// heartbeatTask extends a task's visibility deadline; it is invoked only for
// the current head of its thread, since only a thread's head is ever
// dequeued and therefore ever heartbeated.
func (c *Core) heartbeatTask(txn *store.Txn, task *corepb.Task, now int64) error {
	if err := c.tasks.inProgressIndex.Delete(txn, c.tasks.tablePK(task.Id.AccountId, task.Id.QueueId), inProgressIndexItem(task.VisibleAt, task.Id.TaskId)); err != nil {
		return err
	}

	task.VisibleAt = now + task.KeepaliveTimeoutInSeconds*int64(time.Second)
	if err := c.tasks.set(txn, task); err != nil {
		return err
	}

	if task.ThreadId != "" {
		thread, err := c.threads.Get(txn, task.Id.AccountId, task.Id.QueueId, task.ThreadId)
		if err != nil {
			return err
		}
		if thread.HeadTaskId.TaskId != task.Id.TaskId {
			return errors.New("in progress task is not the head of its thread")
		}
		thread.VisibleAt = task.VisibleAt
		if err := c.threads.set(txn, task.Id.AccountId, task.Id.QueueId, thread); err != nil {
			return err
		}
	}

	return c.tasks.inProgressIndex.Add(txn, c.tasks.tablePK(task.Id.AccountId, task.Id.QueueId), inProgressIndexItem(task.VisibleAt, task.Id.TaskId))
}

// failTaskToDead marks a retry-exhausted, in-progress task as dead — unless
// the queue's DeadLetterQueueConfig says otherwise. A nil or disabled config
// means "no DLQ": the task is deleted outright, same as an expired one,
// rather than kept forever by accident. Otherwise its ExpiresAt is
// recomputed to a fresh retention deadline (LastFailedAt +
// RetentionPeriodInSeconds) — never the stale delivery deadline it had while
// alive, which no longer means anything once the task stops being
// delivered.
func (c *Core) failTaskToDead(txn *store.Txn, task *corepb.Task, now int64, dlqConfig *corepb.DeadLetterQueueConfig) error {
	if dlqConfig == nil || !dlqConfig.Enable {
		return c.deleteTask(txn, task, taskDeletionNone)
	}

	if err := c.tasks.inProgressIndex.Delete(txn, c.tasks.tablePK(task.Id.AccountId, task.Id.QueueId), inProgressIndexItem(task.VisibleAt, task.Id.TaskId)); err != nil {
		return err
	}

	task.State = corepb.TaskState_TASK_STATE_DEAD
	task.LastFailedAt = now
	task.ExpiresAt = task.LastFailedAt + dlqConfig.RetentionPeriodInSeconds*int64(time.Second)
	if err := c.tasks.set(txn, task); err != nil {
		return err
	}

	if err := c.tasks.expiration.add(txn, task.Id, task.ExpiresAt); err != nil {
		return err
	}

	if task.DedupeKey != "" {
		if err := c.tasks.dedupeKeysIndex.Delete(txn, c.tasks.dedupeKeysIndexPK(task.Id.AccountId, task.Id.QueueId, task.DedupeKey)); err != nil {
			return err
		}
	}

	if task.ThreadId != "" {
		if err := c.detachTaskFromThread(txn, task); err != nil {
			return err
		}
	}

	if err := c.tasks.deadTasksIndex.Add(txn, c.tasks.tablePK(task.Id.AccountId, task.Id.QueueId), deadTasksIndexItem(task.LastFailedAt, task.Id.TaskId)); err != nil {
		return err
	}

	counters, err := c.counters.Get(txn, task.Id.AccountId, task.Id.QueueId)
	if err != nil {
		return err
	}
	counters.InProgressTasksCount--
	counters.DeadTasksCount++
	if err := c.counters.Set(txn, task.Id.AccountId, task.Id.QueueId, counters); err != nil {
		return err
	}

	return c.evictExcessDeadTasks(txn, task.Id.AccountId, task.Id.QueueId, dlqConfig.MaxSize, counters.DeadTasksCount)
}

// evictExcessDeadTasks deletes the oldest dead tasks (by LastFailedAt,
// deadTasksIndex's natural sort order) until the queue's dead-task count is
// back at maxSize, making the DLQ a bounded ring buffer of the most recent
// failures rather than an unbounded log. maxSize <= 0 means unlimited (no
// eviction).
func (c *Core) evictExcessDeadTasks(txn *store.Txn, accountId, queueId uint64, maxSize int64, deadTasksCount int64) error {
	if maxSize <= 0 {
		return nil
	}

	overflow := deadTasksCount - maxSize
	if overflow <= 0 {
		return nil
	}

	oldest := make([]*corepb.Task, 0, overflow)
	err := c.tasks.deadTasksIndex.ListAll(txn, c.tasks.tablePK(accountId, queueId), func(item []byte) (bool, error) {
		taskId := extractTaskIdFromIndexItem(item)
		task, err := c.tasks.Get(txn, &corepb.TaskId{AccountId: accountId, QueueId: queueId, TaskId: taskId})
		if err != nil {
			return false, err
		}
		oldest = append(oldest, task)
		return int64(len(oldest)) < overflow, nil
	})
	if err != nil {
		return err
	}

	for _, task := range oldest {
		if err := c.deleteTask(txn, task, taskDeletionNone); err != nil {
			return err
		}
	}

	return nil
}

// failTaskToRetry returns an in-progress task to enqueued, scheduled after
// its RetryStrategy backoff for the retry about to happen. task.Attempts
// counts deliveries (including the one that just failed), so
// RetryIntervalsInSeconds[task.Attempts-1] is the delay before the next one:
// a strategy of length N allows N retries (N+1 total attempts), and every
// configured interval is used — the caller only reaches this function when
// task.Attempts <= len(RetryIntervalsInSeconds).
func (c *Core) failTaskToRetry(txn *store.Txn, task *corepb.Task, now int64) error {
	if err := c.tasks.inProgressIndex.Delete(txn, c.tasks.tablePK(task.Id.AccountId, task.Id.QueueId), inProgressIndexItem(task.VisibleAt, task.Id.TaskId)); err != nil {
		return err
	}

	oldScheduledAt := task.ScheduledAt
	task.State = corepb.TaskState_TASK_STATE_ENQUEUED
	task.ScheduledAt = now + task.RetryStrategy.RetryIntervalsInSeconds[task.Attempts-1]*int64(time.Second)
	task.LastFailedAt = now
	task.VisibleAt = 0
	if err := c.tasks.set(txn, task); err != nil {
		return err
	}

	// The task is returning to ENQUEUED, so its delivery deadline is active
	// again — ExpiresAt itself is unchanged, only re-add the index entry
	// dequeueTasksBeforeTime removed when this task first went IN_PROGRESS.
	if err := c.tasks.expiration.add(txn, task.Id, task.ExpiresAt); err != nil {
		return err
	}

	if task.ThreadId == "" {
		if err := c.tasks.queueIndex.Add(txn, c.tasks.tablePK(task.Id.AccountId, task.Id.QueueId), queueIndexItem(task.ScheduledAt, task.Id.TaskId)); err != nil {
			return err
		}
	} else {
		// The task was in progress, so it was its thread's head; this may
		// promote a sibling scheduled earlier than the new retry time ahead
		// of it (see rescheduleTaskInThread).
		if err := c.rescheduleTaskInThread(txn, task, oldScheduledAt); err != nil {
			return err
		}

		// Whichever task is head after reconciliation is back to ENQUEUED
		// (never IN_PROGRESS at this point), so the thread's mirrored
		// keepalive deadline resets along with it.
		thread, err := c.threads.Get(txn, task.Id.AccountId, task.Id.QueueId, task.ThreadId)
		if err != nil {
			return err
		}
		thread.VisibleAt = 0
		if err := c.threads.set(txn, task.Id.AccountId, task.Id.QueueId, thread); err != nil {
			return err
		}
	}

	counters, err := c.counters.Get(txn, task.Id.AccountId, task.Id.QueueId)
	if err != nil {
		return err
	}
	counters.InProgressTasksCount--
	counters.EnqueuedTasksCount++
	return c.counters.Set(txn, task.Id.AccountId, task.Id.QueueId, counters)
}

func getRefillInterval(r *corepb.TokenBucketRateLimiting) int64 {
	switch r.IntervalUnit {
	case corepb.IntervalUnit_INTERVAL_UNIT_SECONDS:
		return r.Interval * int64(time.Second) / r.MaxTokens
	case corepb.IntervalUnit_INTERVAL_UNIT_MINUTES:
		return r.Interval * int64(time.Minute) / r.MaxTokens
	case corepb.IntervalUnit_INTERVAL_UNIT_HOURS:
		return r.Interval * int64(time.Hour) / r.MaxTokens
	default:
		panic(fmt.Sprintf("unknown IntervalUnit %s", r.IntervalUnit))
	}
}

// dequeueInProgressTasksBeforeTime finds in-progress tasks whose keepalive
// timeout has passed — nobody renewed them in time — and routes each
// through handleTaskFailure, treating the timeout as an implicit failure:
// the same expired/dead/retry decision, and the same RetryStrategy backoff,
// an explicit ReportStatus(FAILED) would get. It never hands any task
// directly back to the caller — a retried task only becomes visible again
// via the normal ScheduledAt/queueIndex path (dequeueTasksBeforeTime, called
// right after this one in Dequeue), even if its backoff happens to be zero.
func (c *Core) dequeueInProgressTasksBeforeTime(txn *store.Txn, accountId, queueId uint64, now int64, dlqConfig *corepb.DeadLetterQueueConfig) error {
	timedOutTasks := make([]*corepb.Task, 0)

	state, err := c.queueState.Get(txn, accountId, queueId)
	if err != nil {
		return err
	}

	visitedTasks := 0

	err = c.tasks.inProgressIndex.ListInRange(txn, c.tasks.tablePK(accountId, queueId), itemPrefix(state.LastVisitedInProgress), itemPrefix(now), func(item []byte) (bool, error) {
		taskId := extractTaskIdFromIndexItem(item)
		task, err := c.tasks.Get(txn, &corepb.TaskId{AccountId: accountId, QueueId: queueId, TaskId: taskId})
		if err != nil {
			return false, err
		}

		visitedTasks++
		state.LastVisitedInProgress = extractTimeFromIndexItem(item)
		timedOutTasks = append(timedOutTasks, task)

		// Bounds how many timed-out tasks a single Dequeue call will process,
		// so a busy queue full of abandoned leases can't make one call scan
		// unboundedly.
		return visitedTasks < maxVisitedTasksForDequeuing, nil
	})
	if err != nil {
		return err
	}

	if visitedTasks == 0 {
		state.LastVisitedInProgress = now
	}
	if err := c.queueState.Set(txn, accountId, queueId, state); err != nil {
		return err
	}

	// Applied after the scan above completes (not inside the ListInRange
	// callback): handleTaskFailure mutates inProgressIndex, and mutating the
	// same index being iterated is unsafe.
	for _, task := range timedOutTasks {
		if err := c.handleTaskFailure(txn, task, now, dlqConfig); err != nil {
			return err
		}
	}

	return nil
}

// dequeueTasksBeforeTime dequeues tasks from the main queueIndex.
func (c *Core) dequeueTasksBeforeTime(txn *store.Txn, accountId, queueId uint64, now int64, limit int64, dequeuingSettings *corepb.DequeuingSettings) ([]*corepb.Task, error) {
	dequeuedTasks := make([]*corepb.Task, 0)
	expiredTasks := make([]*corepb.Task, 0)

	state, err := c.queueState.Get(txn, accountId, queueId)
	if err != nil {
		return nil, err
	}

	counters, err := c.counters.Get(txn, accountId, queueId)
	if err != nil {
		return nil, err
	}

	visitedTasks := 0
	sawAnyCandidate := false

	err = c.tasks.queueIndex.ListInRange(txn, c.tasks.tablePK(accountId, queueId), itemPrefix(state.LastVisitedEnqueued), itemPrefix(now), func(item []byte) (bool, error) {
		sawAnyCandidate = true

		// Stop *before* touching another item once a cap is already
		// saturated — checking only after processing the current one would
		// let exactly one extra task through every time a cap (dequeue
		// limit, visit budget, or MaxInProgressTasks) is already at its
		// limit. Leaving the cursor (state.LastVisitedEnqueued) untouched
		// here matters just as much: this item is still un-dequeued, so the
		// next call must reconsider it, not skip past it.
		if int64(len(dequeuedTasks)) >= limit || visitedTasks >= maxVisitedTasksForDequeuing {
			return false, nil
		}
		if dequeuingSettings != nil && dequeuingSettings.MaxInProgressTasks > 0 && counters.InProgressTasksCount >= dequeuingSettings.MaxInProgressTasks {
			return false, nil
		}

		taskId := extractTaskIdFromIndexItem(item)
		task, err := c.tasks.Get(txn, &corepb.TaskId{AccountId: accountId, QueueId: queueId, TaskId: taskId})
		if err != nil {
			return false, err
		}

		visitedTasks++
		state.LastVisitedEnqueued = extractTimeFromIndexItem(item)

		if task.ExpiresAt < now {
			// ExpiresAt is always set by the time a task reaches this core
			// (the server handler fills in a default from the queue's
			// ExpiresInSeconds) — there is no "never expires" sentinel value.
			expiredTasks = append(expiredTasks, task)
		} else {
			dequeuedTasks = append(dequeuedTasks, task)

			// A task was enqueued and will be in progress.
			counters.InProgressTasksCount++
		}

		// A task was enqueued, but not anymore.
		counters.EnqueuedTasksCount--

		return true, nil
	})
	if err != nil {
		return nil, err
	}

	// Only fast-forward the cursor to "now" when the scan found genuinely
	// nothing in range — not when it found something but declined to take
	// it because a cap was already saturated (sawAnyCandidate but
	// visitedTasks could still be 0 in that case).
	if !sawAnyCandidate {
		state.LastVisitedEnqueued = now
	}
	if err := c.queueState.Set(txn, accountId, queueId, state); err != nil {
		return nil, err
	}
	// Each of these was scheduled for delivery but never got there, its
	// ExpiresAt already past — the lifetime "expired" counter, distinct from
	// the current-occupancy gauges above.
	counters.ExpiredTasksCount += int64(len(expiredTasks))
	if err := c.counters.Set(txn, accountId, queueId, counters); err != nil {
		return nil, err
	}

	for _, task := range expiredTasks {
		if err := c.tasks.queueIndex.Delete(txn, c.tasks.tablePK(accountId, queueId), queueIndexItem(task.ScheduledAt, task.Id.TaskId)); err != nil {
			return nil, err
		}
		if err := c.tasks.expiration.remove(txn, task.Id, task.ExpiresAt); err != nil {
			return nil, err
		}
		if err := c.tasks.delete(txn, task.Id); err != nil {
			return nil, err
		}
		if task.DedupeKey != "" {
			if err := c.tasks.dedupeKeysIndex.Delete(txn, c.tasks.dedupeKeysIndexPK(accountId, queueId, task.DedupeKey)); err != nil {
				return nil, err
			}
		}
	}

	for i, task := range dequeuedTasks {
		if err := c.tasks.queueIndex.Delete(txn, c.tasks.tablePK(accountId, queueId), queueIndexItem(task.ScheduledAt, task.Id.TaskId)); err != nil {
			return nil, err
		}
		// A task carries at most one active deadline at a time: its delivery
		// deadline while ENQUEUED, or its keepalive lease while IN_PROGRESS —
		// never both. Drop the expiration-index entry now; failTaskToRetry
		// re-adds it if this task later returns to ENQUEUED.
		if err := c.tasks.expiration.remove(txn, task.Id, task.ExpiresAt); err != nil {
			return nil, err
		}

		task.VisibleAt = now + task.KeepaliveTimeoutInSeconds*int64(time.Second)
		task.Attempts = task.Attempts + 1
		task.State = corepb.TaskState_TASK_STATE_IN_PROGRESS
		if err := c.tasks.set(txn, task); err != nil {
			return nil, err
		}

		if err := c.tasks.inProgressIndex.Add(txn, c.tasks.tablePK(accountId, queueId), inProgressIndexItem(task.VisibleAt, task.Id.TaskId)); err != nil {
			return nil, err
		}

		dequeuedTasks[i] = task
	}

	return dequeuedTasks, nil
}

// getTaskIdByDedupeKey returns the task id for a given dedupe key. ok is
// false (with a nil error) if the key is not currently in use.
func (c *Core) getTaskIdByDedupeKey(txn *store.Txn, accountId, queueId uint64, dedupeKey string) (*corepb.TaskId, bool, error) {
	taskId, err := c.tasks.dedupeKeysIndex.Get(txn, c.tasks.dedupeKeysIndexPK(accountId, queueId, dedupeKey))
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, false, nil
		}
		return nil, false, err
	}
	return &corepb.TaskId{AccountId: accountId, QueueId: queueId, TaskId: taskId}, true, nil
}

// createTask inserts a newly enqueued task and every secondary index entry
// its thread/dedupe/expiration status requires.
func (c *Core) createTask(txn *store.Txn, task *corepb.Task) error {
	// Persist the row before any thread coordination: attachTaskToThread
	// (below) can re-read this exact task by ID via reconcileThreadHead if
	// it turns out to be the earliest member (e.g. it displaces the current
	// head, or — RestartTasks — this ID already had an older row on disk
	// under a previous life). Reading it back before it's persisted would
	// return either a stale prior version or store.ErrNotFound, not the
	// fields just set on task here.
	if err := c.tasks.set(txn, task); err != nil {
		return err
	}

	if task.DedupeKey != "" {
		if err := c.tasks.dedupeKeysIndex.Set(txn, c.tasks.dedupeKeysIndexPK(task.Id.AccountId, task.Id.QueueId, task.DedupeKey), task.Id.TaskId); err != nil {
			return err
		}
	}

	if task.ThreadId == "" {
		if err := c.tasks.queueIndex.Add(txn, c.tasks.tablePK(task.Id.AccountId, task.Id.QueueId), queueIndexItem(task.ScheduledAt, task.Id.TaskId)); err != nil {
			return err
		}
	} else {
		if err := c.attachTaskToThread(txn, task); err != nil {
			return err
		}
	}

	if err := c.tasks.expiration.add(txn, task.Id, task.ExpiresAt); err != nil {
		return err
	}

	counters, err := c.counters.Get(txn, task.Id.AccountId, task.Id.QueueId)
	if err != nil {
		return err
	}
	counters.EnqueuedTasksCount++
	return c.counters.Set(txn, task.Id.AccountId, task.Id.QueueId, counters)
}

// overwriteDuplicate applies overwriteOnDuplicate to the task a dedupe key
// already points at. Returns the (possibly overwritten) duplicate, or nil if
// there was nothing to overwrite.
func (c *Core) overwriteDuplicate(txn *store.Txn, duplicateId *corepb.TaskId, overwriteOnDuplicate []corepb.EnqueueRequestEntry_OverwriteOnDuplicate, scheduledAt int64, payload []byte, expiresAt int64) (*corepb.Task, error) {
	if len(overwriteOnDuplicate) == 0 {
		return nil, nil
	}

	duplicate, err := c.tasks.Get(txn, duplicateId)
	if err != nil {
		return nil, err
	}

	// Only tasks that have not started any processing yet (ENQUEUED) can be overwritten.
	if duplicate.State == corepb.TaskState_TASK_STATE_ENQUEUED {
		for _, o := range overwriteOnDuplicate {
			switch o {
			case corepb.EnqueueRequestEntry_OVERWRITE_ON_DUPLICATE_EXPIRES_AT:
				if err := c.tasks.expiration.remove(txn, duplicate.Id, duplicate.ExpiresAt); err != nil {
					return nil, err
				}

				duplicate.ExpiresAt = expiresAt

				if err := c.tasks.expiration.add(txn, duplicate.Id, duplicate.ExpiresAt); err != nil {
					return nil, err
				}
			case corepb.EnqueueRequestEntry_OVERWRITE_ON_DUPLICATE_PAYLOAD:
				duplicate.Payload = payload
			case corepb.EnqueueRequestEntry_OVERWRITE_ON_DUPLICATE_SCHEDULED_AT:
				oldScheduledAt := duplicate.ScheduledAt
				duplicate.ScheduledAt = scheduledAt

				if duplicate.ThreadId == "" {
					if err := c.tasks.queueIndex.Delete(txn, c.tasks.tablePK(duplicate.Id.AccountId, duplicate.Id.QueueId), queueIndexItem(oldScheduledAt, duplicate.Id.TaskId)); err != nil {
						return nil, err
					}

					if err := c.tasks.queueIndex.Add(txn, c.tasks.tablePK(duplicate.Id.AccountId, duplicate.Id.QueueId), queueIndexItem(duplicate.ScheduledAt, duplicate.Id.TaskId)); err != nil {
						return nil, err
					}
				} else {
					// rescheduleTaskInThread re-reads this task from the
					// store while reconciling its thread's head, so the new
					// ScheduledAt must already be persisted.
					if err := c.tasks.set(txn, duplicate); err != nil {
						return nil, err
					}

					if err := c.rescheduleTaskInThread(txn, duplicate, oldScheduledAt); err != nil {
						return nil, err
					}
				}
			}
		}

		if err := c.tasks.set(txn, duplicate); err != nil {
			return nil, err
		}
	}

	return duplicate, nil
}

// taskDeletionReason selects which monotonic lifetime counter (if any)
// deleteTask increments, distinct from the current-occupancy gauges it
// always adjusts based on task.State.
type taskDeletionReason int

const (
	// taskDeletionNone is for explicit/administrative removals (DeleteTasks,
	// PurgeQueue, DLQ max_size eviction, retry-exhausted-with-no-DLQ) that
	// aren't ExpiresAt-driven and don't represent a completed delivery —
	// neither lifetime counter applies.
	taskDeletionNone taskDeletionReason = iota
	// taskDeletionSucceeded increments ProcessedTasksCount.
	taskDeletionSucceeded
	// taskDeletionExpired increments ExpiredTasksCount — an ExpiresAt-driven
	// removal, whether the task was ENQUEUED, IN_PROGRESS-then-failed, or
	// DEAD past its DLQ retention deadline.
	taskDeletionExpired
)

// deleteTask removes a task and every secondary index entry that corresponds
// to its current state, promoting the next head of its thread if it was one.
func (c *Core) deleteTask(txn *store.Txn, task *corepb.Task, reason taskDeletionReason) error {
	counters, err := c.counters.Get(txn, task.Id.AccountId, task.Id.QueueId)
	if err != nil {
		return err
	}

	switch reason {
	case taskDeletionSucceeded:
		counters.ProcessedTasksCount++
	case taskDeletionExpired:
		counters.ExpiredTasksCount++
	}

	switch task.State {
	case corepb.TaskState_TASK_STATE_ENQUEUED:
		// Deleting is a no-op for a non-head threaded task, which was never
		// added here in the first place.
		if err := c.tasks.queueIndex.Delete(txn, c.tasks.tablePK(task.Id.AccountId, task.Id.QueueId), queueIndexItem(task.ScheduledAt, task.Id.TaskId)); err != nil {
			return err
		}

		if task.DedupeKey != "" {
			if err := c.tasks.dedupeKeysIndex.Delete(txn, c.tasks.dedupeKeysIndexPK(task.Id.AccountId, task.Id.QueueId, task.DedupeKey)); err != nil {
				return err
			}
		}

		if task.ThreadId != "" {
			if err := c.detachTaskFromThread(txn, task); err != nil {
				return err
			}
		}

		counters.EnqueuedTasksCount--
	case corepb.TaskState_TASK_STATE_DEAD:
		if err := c.tasks.deadTasksIndex.Delete(txn, c.tasks.tablePK(task.Id.AccountId, task.Id.QueueId), deadTasksIndexItem(task.LastFailedAt, task.Id.TaskId)); err != nil {
			return err
		}

		counters.DeadTasksCount--
	case corepb.TaskState_TASK_STATE_IN_PROGRESS:
		if err := c.tasks.inProgressIndex.Delete(txn, c.tasks.tablePK(task.Id.AccountId, task.Id.QueueId), inProgressIndexItem(task.VisibleAt, task.Id.TaskId)); err != nil {
			return err
		}

		if task.DedupeKey != "" {
			if err := c.tasks.dedupeKeysIndex.Delete(txn, c.tasks.dedupeKeysIndexPK(task.Id.AccountId, task.Id.QueueId, task.DedupeKey)); err != nil {
				return err
			}
		}

		if task.ThreadId != "" {
			if err := c.detachTaskFromThread(txn, task); err != nil {
				return err
			}
		}

		counters.InProgressTasksCount--
	}

	if err := c.tasks.expiration.remove(txn, task.Id, task.ExpiresAt); err != nil {
		return err
	}

	if err := c.tasks.delete(txn, task.Id); err != nil {
		return err
	}

	return c.counters.Set(txn, task.Id.AccountId, task.Id.QueueId, counters)
}

// --- Thread head coordination ---
//
// tasksTable and threadsTable are independent leaf tables — neither holds a
// reference to the other. The invariant that couples them ("only a thread's
// head ever appears in queueIndex, so at most one task per thread is ever
// dequeued/in-progress at a time") is entirely Core's responsibility, and
// every mutation that can affect it goes through exactly one of the four
// functions below rather than hand-rolling the queueIndex/threadedTasksIndex
// dance at each call site.

// attachTaskToThread adds a newly created ENQUEUED task to its thread,
// creating the thread record if this is its first task (making the task its
// head outright) or reconciling the head otherwise (the new task only
// displaces the current head if it is scheduled earlier and that head is
// not already being processed).
func (c *Core) attachTaskToThread(txn *store.Txn, task *corepb.Task) error {
	if err := c.threads.AddToIndex(txn, task.Id.AccountId, task.Id.QueueId, task.ThreadId, task.ScheduledAt, task.Id.TaskId); err != nil {
		return err
	}

	thread, err := c.threads.Get(txn, task.Id.AccountId, task.Id.QueueId, task.ThreadId)
	if err != nil {
		if !errors.Is(err, store.ErrNotFound) {
			return err
		}

		// No thread exists yet: this task becomes the head outright.
		thread = &corepb.Thread{
			ThreadId:    task.ThreadId,
			HeadTaskId:  task.Id,
			ScheduledAt: task.ScheduledAt,
		}
		if err := c.threads.set(txn, task.Id.AccountId, task.Id.QueueId, thread); err != nil {
			return err
		}

		return c.tasks.queueIndex.Add(txn, c.tasks.tablePK(task.Id.AccountId, task.Id.QueueId), queueIndexItem(task.ScheduledAt, task.Id.TaskId))
	}

	return c.reconcileThreadHead(txn, task.Id.AccountId, task.Id.QueueId, thread)
}

// detachTaskFromThread removes a task that is leaving its thread's active
// set for good (deleted while enqueued, or completed/dead after being in
// progress) from threadedTasksIndex, promoting the next earliest remaining
// task to head if the departing task was the head. It is a no-op beyond the
// index removal if the departing task was not the head.
func (c *Core) detachTaskFromThread(txn *store.Txn, task *corepb.Task) error {
	if err := c.threads.RemoveFromIndex(txn, task.Id.AccountId, task.Id.QueueId, task.ThreadId, task.ScheduledAt, task.Id.TaskId); err != nil {
		return err
	}

	thread, err := c.threads.Get(txn, task.Id.AccountId, task.Id.QueueId, task.ThreadId)
	if err != nil {
		return err
	}

	if thread.HeadTaskId.TaskId != task.Id.TaskId {
		return nil
	}

	return c.promoteNextThreadHead(txn, task.Id.AccountId, task.Id.QueueId, thread)
}

// rescheduleTaskInThread repositions an already-attached, still-ENQUEUED
// threaded task in threadedTasksIndex after its ScheduledAt changed
// (task.ScheduledAt and its row in tasksTable must already hold the new
// value; oldScheduledAt is the value it's indexed under until this call)
// and reconciles its thread's head, since the change may promote a sibling
// ahead of it or move it ahead of the current head.
func (c *Core) rescheduleTaskInThread(txn *store.Txn, task *corepb.Task, oldScheduledAt int64) error {
	if err := c.threads.RemoveFromIndex(txn, task.Id.AccountId, task.Id.QueueId, task.ThreadId, oldScheduledAt, task.Id.TaskId); err != nil {
		return err
	}
	if err := c.threads.AddToIndex(txn, task.Id.AccountId, task.Id.QueueId, task.ThreadId, task.ScheduledAt, task.Id.TaskId); err != nil {
		return err
	}

	thread, err := c.threads.Get(txn, task.Id.AccountId, task.Id.QueueId, task.ThreadId)
	if err != nil {
		return err
	}

	return c.reconcileThreadHead(txn, task.Id.AccountId, task.Id.QueueId, thread)
}

// reconcileThreadHead re-derives a thread's head from its threadedTasksIndex
// after a member arrived or was rescheduled (the caller must have already
// applied that index change) and keeps queueIndex in sync with whichever
// task ends up head. An outgoing head that is currently IN_PROGRESS is
// never preempted — only a thread's head is ever dequeued, so it must run
// to completion (or expire/fail) before another task can take over.
func (c *Core) reconcileThreadHead(txn *store.Txn, accountId, queueId uint64, thread *corepb.Thread) error {
	var earliest *corepb.Task
	err := c.threads.ListIndex(txn, accountId, queueId, thread.ThreadId, func(taskId uint64) (bool, error) {
		task, err := c.tasks.Get(txn, &corepb.TaskId{AccountId: accountId, QueueId: queueId, TaskId: taskId})
		if err != nil {
			return false, err
		}
		earliest = task
		// Only the earliest (first, since the index is sorted) task matters.
		return false, nil
	})
	if err != nil {
		return err
	}

	currentHead, err := c.tasks.Get(txn, thread.HeadTaskId)
	if err != nil {
		return err
	}

	pk := c.tasks.tablePK(accountId, queueId)

	if earliest.Id.TaskId == currentHead.Id.TaskId {
		if thread.ScheduledAt == earliest.ScheduledAt {
			return nil
		}

		// The head itself was rescheduled; keep queueIndex in sync if it's
		// currently sitting there (it won't be if the head is IN_PROGRESS).
		if currentHead.State == corepb.TaskState_TASK_STATE_ENQUEUED {
			if err := c.tasks.queueIndex.Delete(txn, pk, queueIndexItem(thread.ScheduledAt, currentHead.Id.TaskId)); err != nil {
				return err
			}
			if err := c.tasks.queueIndex.Add(txn, pk, queueIndexItem(earliest.ScheduledAt, currentHead.Id.TaskId)); err != nil {
				return err
			}
		}

		thread.ScheduledAt = earliest.ScheduledAt
		return c.threads.set(txn, accountId, queueId, thread)
	}

	// A different member is now earliest; only take over if the outgoing
	// head is not already being processed.
	if currentHead.State != corepb.TaskState_TASK_STATE_ENQUEUED {
		return nil
	}

	if err := c.tasks.queueIndex.Delete(txn, pk, queueIndexItem(currentHead.ScheduledAt, currentHead.Id.TaskId)); err != nil {
		return err
	}

	thread.HeadTaskId = earliest.Id
	thread.ScheduledAt = earliest.ScheduledAt
	if err := c.threads.set(txn, accountId, queueId, thread); err != nil {
		return err
	}

	return c.tasks.queueIndex.Add(txn, pk, queueIndexItem(earliest.ScheduledAt, earliest.Id.TaskId))
}

// promoteNextThreadHead finds the earliest remaining task of a thread (the
// caller must already have removed the departing task from
// threadedTasksIndex) and makes it the new head, adding it to queueIndex so
// it becomes visible for dequeuing. If none remain, the thread record itself
// is deleted. Unlike reconcileThreadHead, this never has an "outgoing head
// is busy" case to respect — the caller guarantees the previous head is
// gone for good.
func (c *Core) promoteNextThreadHead(txn *store.Txn, accountId, queueId uint64, thread *corepb.Thread) error {
	nextHeadFound := false

	err := c.threads.ListIndex(txn, accountId, queueId, thread.ThreadId, func(taskId uint64) (bool, error) {
		nextHead, err := c.tasks.Get(txn, &corepb.TaskId{AccountId: accountId, QueueId: queueId, TaskId: taskId})
		if err != nil {
			return false, err
		}

		nextHeadFound = true

		thread.VisibleAt = 0
		thread.HeadTaskId = nextHead.Id
		thread.ScheduledAt = nextHead.ScheduledAt

		if err := c.threads.set(txn, accountId, queueId, thread); err != nil {
			return false, err
		}

		if err := c.tasks.queueIndex.Add(txn, c.tasks.tablePK(accountId, queueId), queueIndexItem(nextHead.ScheduledAt, nextHead.Id.TaskId)); err != nil {
			return false, err
		}

		// Only the earliest (first, since the index is sorted) remaining task becomes head.
		return false, nil
	})
	if err != nil {
		return err
	}

	if !nextHeadFound {
		return c.threads.Delete(txn, accountId, queueId, thread.ThreadId)
	}

	return nil
}

// getAgeOfOldestEnqueuedTask returns how long the oldest currently-dequeueable
// task has been waiting: it scans queueIndex, which holds only non-threaded
// ENQUEUED tasks and thread heads, from its very front, so no dequeue-style
// cursor is needed — the first entry within [0, now] is always the oldest one
// due. A non-head member of a thread is deliberately excluded even though it
// is technically ENQUEUED: only a thread's head is ever dequeued, and when
// the head is IN_PROGRESS a sibling scheduled earlier than it does not
// preempt it (a thread's head is never taken over from a task actively being
// processed), so that sibling can sit ENQUEUED-but-not-head with an earlier
// ScheduledAt than what this scan reports. That's fine: there is nothing to
// act on for a task blocked behind a thread that's already busy, so "age of
// the oldest task actually available to dequeue" is the more useful signal.
func (c *Core) getAgeOfOldestEnqueuedTask(txn *store.Txn, accountId, queueId uint64, now int64) (int64, error) {
	age := int64(0)
	err := c.tasks.queueIndex.ListInRange(txn, c.tasks.tablePK(accountId, queueId), itemPrefix(0), itemPrefix(now), func(item []byte) (bool, error) {
		age = now - extractTimeFromIndexItem(item)

		// Only the oldest (first, since the index is sorted) entry is needed.
		return false, nil
	})
	if err != nil {
		return 0, err
	}

	return age, nil
}

func taskNotFoundError(taskId *corepb.TaskId) *mrpc.Error {
	return mrpc.NewErrorWithContext(
		mrpc.NotFound,
		"task not found",
		map[string]string{
			"task_id":  ids.EncodeTaskId(taskId.TaskId),
			"queue_id": fmt.Sprintf("%d", taskId.QueueId),
		})
}
