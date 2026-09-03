package tasks

import (
	"math/rand/v2"
	"testing"
	"time"

	mrpc "github.com/evrblk/monstera/rpc"
	"github.com/evrblk/monstera/store"
	"github.com/stretchr/testify/require"

	"github.com/evrblk/moab/pkg/coreapis"
	"github.com/evrblk/moab/pkg/corepb"
)

func TestEnqueueAndGetTask(t *testing.T) {
	core := newTasksCore(t)

	accountId, queueId := rand.Uint64(), rand.Uint64()
	now := time.Now()

	tasks := enqueue(t, core, accountId, queueId, now, entry("payload-1"))
	require.Len(t, tasks, 1)
	require.EqualValues(t, 1, tasks[0].Id.TaskId)
	require.Equal(t, corepb.TaskState_TASK_STATE_ENQUEUED, tasks[0].State)

	fetched := getTask(t, core, tasks[0].Id, now)
	require.Equal(t, []byte("payload-1"), fetched.Payload)

	// Get nonexistent task
	appErr := getTaskWithError(t, core, &corepb.TaskId{AccountId: accountId, QueueId: queueId, TaskId: 999}, now)
	require.Equal(t, mrpc.NotFound, appErr.Code)
}

func TestDedupeKeyOverwritesExistingTask(t *testing.T) {
	core := newTasksCore(t)

	accountId, queueId := rand.Uint64(), rand.Uint64()
	now := time.Now()

	e := entry("payload-1")
	e.DedupeKey = "dedupe-1"
	first := enqueue(t, core, accountId, queueId, now, e)
	require.Len(t, first, 1)

	// Enqueuing the same dedupe key again with no overwrite flags returns nothing new
	e2 := entry("payload-2")
	e2.DedupeKey = "dedupe-1"
	second := enqueue(t, core, accountId, queueId, now.Add(time.Second), e2)
	require.Len(t, second, 0)

	// The original task is unaffected
	fetched := getTask(t, core, first[0].Id, now)
	require.Equal(t, []byte("payload-1"), fetched.Payload)

	// With OVERWRITE_ON_DUPLICATE_PAYLOAD, the existing task's payload is overwritten in place (same task id)
	e3 := entry("payload-3")
	e3.DedupeKey = "dedupe-1"
	e3.OverwriteOnDuplicate = []corepb.EnqueueRequestEntry_OverwriteOnDuplicate{corepb.EnqueueRequestEntry_OVERWRITE_ON_DUPLICATE_PAYLOAD}
	third := enqueue(t, core, accountId, queueId, now.Add(2*time.Second), e3)
	require.Len(t, third, 1)
	require.Equal(t, first[0].Id.TaskId, third[0].Id.TaskId)
	require.Equal(t, []byte("payload-3"), third[0].Payload)
}

func TestDequeueRespectsMaxInProgressTasks(t *testing.T) {
	core := newTasksCore(t)

	accountId, queueId := rand.Uint64(), rand.Uint64()
	now := time.Now()

	enqueue(t, core, accountId, queueId, now, entry("a"), entry("b"), entry("c"))

	settings := &corepb.DequeuingSettings{MaxInProgressTasks: 2}
	dequeued := dequeue(t, core, accountId, queueId, now, 10, settings)
	require.Len(t, dequeued, 2)
	for _, task := range dequeued {
		require.Equal(t, corepb.TaskState_TASK_STATE_IN_PROGRESS, task.State)
	}

	stats := getStatistics(t, core, accountId, queueId, now)
	require.EqualValues(t, 1, stats.EnqueuedTasksCount)
	require.EqualValues(t, 2, stats.InProgressTasksCount)

	// The queue is already at its cap; dequeuing again must not let a third
	// task through (the cap must be checked before processing a candidate,
	// not just after).
	dequeuedAgain := dequeue(t, core, accountId, queueId, now, 10, settings)
	require.Len(t, dequeuedAgain, 0)

	stats = getStatistics(t, core, accountId, queueId, now)
	require.EqualValues(t, 1, stats.EnqueuedTasksCount)
	require.EqualValues(t, 2, stats.InProgressTasksCount)
}

func TestDequeueRespectsRateLimit(t *testing.T) {
	core := newTasksCore(t)

	accountId, queueId := rand.Uint64(), rand.Uint64()
	now := time.Now()

	enqueue(t, core, accountId, queueId, now, entry("a"), entry("b"), entry("c"))

	settings := &corepb.DequeuingSettings{
		RateLimiting: &corepb.TokenBucketRateLimiting{
			MaxTokens:    1,
			Interval:     1,
			IntervalUnit: corepb.IntervalUnit_INTERVAL_UNIT_HOURS,
		},
	}
	dequeued := dequeue(t, core, accountId, queueId, now, 10, settings)
	require.Len(t, dequeued, 1)

	// The bucket is now empty; dequeuing again immediately returns nothing.
	dequeued = dequeue(t, core, accountId, queueId, now, 10, settings)
	require.Len(t, dequeued, 0)
}

func TestReportStatusSucceeded(t *testing.T) {
	core := newTasksCore(t)

	accountId, queueId := rand.Uint64(), rand.Uint64()
	now := time.Now()

	enqueue(t, core, accountId, queueId, now, entry("a"))
	dequeued := dequeue(t, core, accountId, queueId, now, 10, nil)
	require.Len(t, dequeued, 1)

	reportStatus(t, core, dequeued[0].Id, now, corepb.ReportStatusRequestEntry_STATUS_SUCCEEDED, dequeued[0].Attempts)

	appErr := getTaskWithError(t, core, dequeued[0].Id, now)
	require.Equal(t, mrpc.NotFound, appErr.Code)

	stats := getStatistics(t, core, accountId, queueId, now)
	require.EqualValues(t, 0, stats.EnqueuedTasksCount)
	require.EqualValues(t, 0, stats.InProgressTasksCount)
}

func TestReportStatusFailedRetriesThenDies(t *testing.T) {
	core := newTasksCore(t)

	accountId, queueId := rand.Uint64(), rand.Uint64()
	now := time.Now()

	e := entry("a")
	e.RetryStrategy = &corepb.RetryStrategy{RetryIntervalsInSeconds: []int64{10}}
	enqueue(t, core, accountId, queueId, now, e)

	dequeued := dequeue(t, core, accountId, queueId, now, 10, nil)
	require.Len(t, dequeued, 1)

	// One configured interval allows one retry (two total attempts) — the
	// first failure must not kill it yet.
	reportStatus(t, core, dequeued[0].Id, now, corepb.ReportStatusRequestEntry_STATUS_FAILED, dequeued[0].Attempts)

	fetched := getTask(t, core, dequeued[0].Id, now)
	require.Equal(t, corepb.TaskState_TASK_STATE_ENQUEUED, fetched.State)
	require.Equal(t, now.Add(10*time.Second).UnixNano(), fetched.ScheduledAt)

	// The retry (second delivery attempt) also fails: the retry budget
	// (Attempts=2 > len(intervals)=1) is now exhausted, so this one dies.
	redequeued := dequeue(t, core, accountId, queueId, now.Add(time.Minute), 10, nil)
	require.Len(t, redequeued, 1)

	reportStatusWithDLQConfig(t, core, redequeued[0].Id, now.Add(time.Minute), corepb.ReportStatusRequestEntry_STATUS_FAILED, redequeued[0].Attempts, enabledDLQConfig())

	dead := getTask(t, core, redequeued[0].Id, now.Add(time.Minute))
	require.Equal(t, corepb.TaskState_TASK_STATE_DEAD, dead.State)

	stats := getStatistics(t, core, accountId, queueId, now.Add(time.Minute))
	require.EqualValues(t, 0, stats.InProgressTasksCount)
	require.EqualValues(t, 1, stats.DeadTasksCount)
}

func TestReportStatusFailedRetriesBackToEnqueued(t *testing.T) {
	core := newTasksCore(t)

	accountId, queueId := rand.Uint64(), rand.Uint64()
	now := time.Now()

	e := entry("a")
	e.RetryStrategy = &corepb.RetryStrategy{RetryIntervalsInSeconds: []int64{10, 20}}
	enqueue(t, core, accountId, queueId, now, e)

	dequeued := dequeue(t, core, accountId, queueId, now, 10, nil)
	require.Len(t, dequeued, 1)

	reportStatus(t, core, dequeued[0].Id, now, corepb.ReportStatusRequestEntry_STATUS_FAILED, dequeued[0].Attempts)

	fetched := getTask(t, core, dequeued[0].Id, now)
	require.Equal(t, corepb.TaskState_TASK_STATE_ENQUEUED, fetched.State)
	// Attempts is 1 after the one dequeue above (the attempt that just
	// failed), so RetryIntervalsInSeconds[Attempts-1] = RetryIntervalsInSeconds[0]
	// = 10s is the delay before the first retry.
	require.Equal(t, now.Add(10*time.Second).UnixNano(), fetched.ScheduledAt)

	stats := getStatistics(t, core, accountId, queueId, now)
	require.EqualValues(t, 1, stats.EnqueuedTasksCount)
	require.EqualValues(t, 0, stats.InProgressTasksCount)
}

// TestReportStatusFailedExhaustsEveryConfiguredInterval locks in the fixed
// retry accounting end to end: a strategy of length N performs N retries
// (N+1 total attempts) and every configured interval is actually used, in
// order — not just the last one.
func TestReportStatusFailedExhaustsEveryConfiguredInterval(t *testing.T) {
	core := newTasksCore(t)

	accountId, queueId := rand.Uint64(), rand.Uint64()
	now := time.Now()

	e := entry("a")
	e.RetryStrategy = &corepb.RetryStrategy{RetryIntervalsInSeconds: []int64{10, 20}}
	enqueue(t, core, accountId, queueId, now, e)

	// Attempt 1 fails: retry after RetryIntervalsInSeconds[0] = 10s.
	dequeued := dequeue(t, core, accountId, queueId, now, 10, nil)
	require.Len(t, dequeued, 1)
	reportStatus(t, core, dequeued[0].Id, now, corepb.ReportStatusRequestEntry_STATUS_FAILED, dequeued[0].Attempts)
	afterFirst := getTask(t, core, dequeued[0].Id, now)
	require.Equal(t, corepb.TaskState_TASK_STATE_ENQUEUED, afterFirst.State)
	require.Equal(t, now.Add(10*time.Second).UnixNano(), afterFirst.ScheduledAt)

	// Attempt 2 (the first retry) fails: retry after RetryIntervalsInSeconds[1] = 20s.
	redequeued := dequeue(t, core, accountId, queueId, now.Add(time.Minute), 10, nil)
	require.Len(t, redequeued, 1)
	reportStatus(t, core, redequeued[0].Id, now.Add(time.Minute), corepb.ReportStatusRequestEntry_STATUS_FAILED, redequeued[0].Attempts)
	afterSecond := getTask(t, core, redequeued[0].Id, now.Add(time.Minute))
	require.Equal(t, corepb.TaskState_TASK_STATE_ENQUEUED, afterSecond.State)
	require.Equal(t, now.Add(time.Minute).Add(20*time.Second).UnixNano(), afterSecond.ScheduledAt)

	// Attempt 3 (the second retry) fails: retry budget (Attempts=3 > len=2)
	// is exhausted, so this one dies.
	redequeued2 := dequeue(t, core, accountId, queueId, now.Add(2*time.Minute), 10, nil)
	require.Len(t, redequeued2, 1)
	reportStatusWithDLQConfig(t, core, redequeued2[0].Id, now.Add(2*time.Minute), corepb.ReportStatusRequestEntry_STATUS_FAILED, redequeued2[0].Attempts, enabledDLQConfig())
	dead := getTask(t, core, redequeued2[0].Id, now.Add(2*time.Minute))
	require.Equal(t, corepb.TaskState_TASK_STATE_DEAD, dead.State)
}

// TestFailedTaskWithoutDLQConfiguredIsDeletedOutright pins down §6/"DLQ"
// step 1: a queue with no DeadLetterQueueConfig (or Enable: false) behaves
// like "no DLQ" — a retry-exhausted task is deleted outright, never parked
// as DEAD.
func TestFailedTaskWithoutDLQConfiguredIsDeletedOutright(t *testing.T) {
	core := newTasksCore(t)

	accountId, queueId := rand.Uint64(), rand.Uint64()
	now := time.Now()

	e := entry("a")
	e.RetryStrategy = &corepb.RetryStrategy{RetryIntervalsInSeconds: []int64{10}}
	enqueue(t, core, accountId, queueId, now, e)

	dequeued := dequeue(t, core, accountId, queueId, now, 10, nil)
	require.Len(t, dequeued, 1)
	reportStatus(t, core, dequeued[0].Id, now, corepb.ReportStatusRequestEntry_STATUS_FAILED, dequeued[0].Attempts)

	redequeued := dequeue(t, core, accountId, queueId, now.Add(time.Minute), 10, nil)
	require.Len(t, redequeued, 1)

	// No DeadLetterQueueConfig passed (nil, the default): retry-exhausted
	// means gone entirely, not dead-lettered.
	reportStatus(t, core, redequeued[0].Id, now.Add(time.Minute), corepb.ReportStatusRequestEntry_STATUS_FAILED, redequeued[0].Attempts)

	appErr := getTaskWithError(t, core, redequeued[0].Id, now.Add(time.Minute))
	require.Equal(t, mrpc.NotFound, appErr.Code)

	stats := getStatistics(t, core, accountId, queueId, now.Add(time.Minute))
	require.EqualValues(t, 0, stats.InProgressTasksCount)
	require.EqualValues(t, 0, stats.DeadTasksCount)

	// Same outcome for an explicitly-disabled config, not just a nil one.
	// Fresh queue to avoid any interaction with the first scenario's dequeue
	// cursor state.
	accountId2, queueId2 := rand.Uint64(), rand.Uint64()
	e2 := entry("b")
	e2.RetryStrategy = &corepb.RetryStrategy{RetryIntervalsInSeconds: []int64{10}}
	enqueue(t, core, accountId2, queueId2, now, e2)
	dequeued2 := dequeue(t, core, accountId2, queueId2, now, 10, nil)
	require.Len(t, dequeued2, 1)
	reportStatusWithDLQConfig(t, core, dequeued2[0].Id, now, corepb.ReportStatusRequestEntry_STATUS_FAILED, dequeued2[0].Attempts, &corepb.DeadLetterQueueConfig{Enable: false})

	redequeued2 := dequeue(t, core, accountId2, queueId2, now.Add(time.Minute), 10, nil)
	require.Len(t, redequeued2, 1)
	reportStatusWithDLQConfig(t, core, redequeued2[0].Id, now.Add(time.Minute), corepb.ReportStatusRequestEntry_STATUS_FAILED, redequeued2[0].Attempts, &corepb.DeadLetterQueueConfig{Enable: false})

	appErr2 := getTaskWithError(t, core, redequeued2[0].Id, now.Add(time.Minute))
	require.Equal(t, mrpc.NotFound, appErr2.Code)
}

// TestFailedTaskWithDLQConfiguredGetsFreshRetentionExpiresAt pins down that
// a dead-lettered task's ExpiresAt is recomputed to LastFailedAt +
// RetentionPeriodInSeconds — never the original delivery deadline — and GC
// reaps it once that retention deadline passes.
func TestFailedTaskWithDLQConfiguredGetsFreshRetentionExpiresAt(t *testing.T) {
	core := newTasksCore(t)

	accountId, queueId := rand.Uint64(), rand.Uint64()
	now := time.Now()

	e := entry("a")
	e.ExpiresAt = now.Add(time.Hour).UnixNano() // original delivery deadline: far out
	e.RetryStrategy = &corepb.RetryStrategy{RetryIntervalsInSeconds: nil}
	enqueue(t, core, accountId, queueId, now, e)

	dequeued := dequeue(t, core, accountId, queueId, now, 10, nil)
	require.Len(t, dequeued, 1)

	dlqConfig := &corepb.DeadLetterQueueConfig{Enable: true, RetentionPeriodInSeconds: 60}
	reportStatusWithDLQConfig(t, core, dequeued[0].Id, now, corepb.ReportStatusRequestEntry_STATUS_FAILED, dequeued[0].Attempts, dlqConfig)

	dead := getTask(t, core, dequeued[0].Id, now)
	require.Equal(t, corepb.TaskState_TASK_STATE_DEAD, dead.State)
	require.Equal(t, now.Add(60*time.Second).UnixNano(), dead.ExpiresAt)
	require.NotEqual(t, e.ExpiresAt, dead.ExpiresAt)

	// Before the retention deadline: still there.
	runGarbageCollection(t, core, now.Add(30*time.Second))
	stillDead := getTask(t, core, dequeued[0].Id, now.Add(30*time.Second))
	require.Equal(t, corepb.TaskState_TASK_STATE_DEAD, stillDead.State)

	// Past the retention deadline: GC reaps it.
	runGarbageCollection(t, core, now.Add(90*time.Second))
	appErr := getTaskWithError(t, core, dequeued[0].Id, now.Add(90*time.Second))
	require.Equal(t, mrpc.NotFound, appErr.Code)
}

// TestDLQMaxSizeEvictsOldestDeadTasks pins down §6/"DLQ" step 3: once the
// dead-task count exceeds max_size, the oldest (by LastFailedAt) is evicted,
// making the DLQ a bounded ring buffer of the most recent failures.
func TestDLQMaxSizeEvictsOldestDeadTasks(t *testing.T) {
	core := newTasksCore(t)

	accountId, queueId := rand.Uint64(), rand.Uint64()
	now := time.Now()

	dlqConfig := &corepb.DeadLetterQueueConfig{Enable: true, RetentionPeriodInSeconds: 86400, MaxSize: 2}

	var deadIds []*corepb.TaskId
	for i, name := range []string{"first", "second", "third"} {
		e := entry(name)
		e.RetryStrategy = &corepb.RetryStrategy{RetryIntervalsInSeconds: nil}
		created := enqueue(t, core, accountId, queueId, now, e)
		require.Len(t, created, 1)

		failAt := now.Add(time.Duration(i) * time.Minute)
		dequeued := dequeue(t, core, accountId, queueId, failAt, 10, nil)
		require.Len(t, dequeued, 1)
		reportStatusWithDLQConfig(t, core, dequeued[0].Id, failAt, corepb.ReportStatusRequestEntry_STATUS_FAILED, dequeued[0].Attempts, dlqConfig)

		deadIds = append(deadIds, dequeued[0].Id)
	}

	later := now.Add(10 * time.Minute)

	stats := getStatistics(t, core, accountId, queueId, later)
	require.EqualValues(t, 2, stats.DeadTasksCount)

	// The oldest ("first") was evicted; the two most recent remain.
	firstErr := getTaskWithError(t, core, deadIds[0], later)
	require.Equal(t, mrpc.NotFound, firstErr.Code)

	second := getTask(t, core, deadIds[1], later)
	require.Equal(t, corepb.TaskState_TASK_STATE_DEAD, second.State)
	third := getTask(t, core, deadIds[2], later)
	require.Equal(t, corepb.TaskState_TASK_STATE_DEAD, third.State)
}

// TestRestartTasksNotFound reports NOT_FOUND for a task id that doesn't exist.
func TestRestartTasksNotFound(t *testing.T) {
	core := newTasksCore(t)

	accountId, queueId := rand.Uint64(), rand.Uint64()
	now := time.Now()

	entries := restartTasks(t, core, accountId, queueId, now, &corepb.RestartTasksRequestEntry{
		TaskId:    999,
		ExpiresAt: now.Add(time.Hour).UnixNano(),
	})
	require.Len(t, entries, 1)
	require.EqualValues(t, 999, entries[0].TaskId)
	require.Equal(t, corepb.RestartTasksResponseEntry_RESULT_NOT_FOUND, entries[0].Result)
}

// TestRestartTasksNotDead reports NOT_DEAD for a task that isn't DEAD —
// restarting an ENQUEUED or IN_PROGRESS task is meaningless.
func TestRestartTasksNotDead(t *testing.T) {
	core := newTasksCore(t)

	accountId, queueId := rand.Uint64(), rand.Uint64()
	now := time.Now()

	created := enqueue(t, core, accountId, queueId, now, entry("a"))
	require.Len(t, created, 1)

	entries := restartTasks(t, core, accountId, queueId, now, &corepb.RestartTasksRequestEntry{
		TaskId:    created[0].Id.TaskId,
		ExpiresAt: now.Add(time.Hour).UnixNano(),
	})
	require.Len(t, entries, 1)
	require.Equal(t, corepb.RestartTasksResponseEntry_RESULT_NOT_DEAD, entries[0].Result)
}

// TestRestartTasksRetentionExpired treats a DEAD task whose DLQ retention
// deadline has already passed (GC just hasn't swept it yet) as already gone
// — consistent with what GetTask/GC would already say.
func TestRestartTasksRetentionExpired(t *testing.T) {
	core := newTasksCore(t)

	accountId, queueId := rand.Uint64(), rand.Uint64()
	now := time.Now()

	e := entry("a")
	e.RetryStrategy = &corepb.RetryStrategy{RetryIntervalsInSeconds: nil}
	enqueue(t, core, accountId, queueId, now, e)

	dequeued := dequeue(t, core, accountId, queueId, now, 10, nil)
	require.Len(t, dequeued, 1)

	dlqConfig := &corepb.DeadLetterQueueConfig{Enable: true, RetentionPeriodInSeconds: 60}
	reportStatusWithDLQConfig(t, core, dequeued[0].Id, now, corepb.ReportStatusRequestEntry_STATUS_FAILED, dequeued[0].Attempts, dlqConfig)

	// Past the 60s retention deadline.
	later := now.Add(2 * time.Minute)
	entries := restartTasks(t, core, accountId, queueId, later, &corepb.RestartTasksRequestEntry{
		TaskId:    dequeued[0].Id.TaskId,
		ExpiresAt: later.Add(time.Hour).UnixNano(),
	})
	require.Len(t, entries, 1)
	require.Equal(t, corepb.RestartTasksResponseEntry_RESULT_NOT_FOUND, entries[0].Result)
}

// TestRestartTasksDedupeConflict refuses to restart a dead task whose
// DedupeKey has since been reclaimed by a different live task — death
// releases a dedupe key immediately, so restart must re-validate it, not
// assume it's still free.
func TestRestartTasksDedupeConflict(t *testing.T) {
	core := newTasksCore(t)

	accountId, queueId := rand.Uint64(), rand.Uint64()
	now := time.Now()

	e := entry("a")
	e.DedupeKey = "dedupe-1"
	e.RetryStrategy = &corepb.RetryStrategy{RetryIntervalsInSeconds: nil}
	enqueue(t, core, accountId, queueId, now, e)

	dequeued := dequeue(t, core, accountId, queueId, now, 10, nil)
	require.Len(t, dequeued, 1)

	dlqConfig := &corepb.DeadLetterQueueConfig{Enable: true, RetentionPeriodInSeconds: 86400}
	reportStatusWithDLQConfig(t, core, dequeued[0].Id, now, corepb.ReportStatusRequestEntry_STATUS_FAILED, dequeued[0].Attempts, dlqConfig)

	dead := getTask(t, core, dequeued[0].Id, now)
	require.Equal(t, corepb.TaskState_TASK_STATE_DEAD, dead.State)

	// The dedupe key was released on death; a fresh task claims it.
	e2 := entry("b")
	e2.DedupeKey = "dedupe-1"
	newClaim := enqueue(t, core, accountId, queueId, now, e2)
	require.Len(t, newClaim, 1)
	require.NotEqual(t, dequeued[0].Id.TaskId, newClaim[0].Id.TaskId)

	entries := restartTasks(t, core, accountId, queueId, now, &corepb.RestartTasksRequestEntry{
		TaskId:    dequeued[0].Id.TaskId,
		ExpiresAt: now.Add(time.Hour).UnixNano(),
	})
	require.Len(t, entries, 1)
	require.Equal(t, corepb.RestartTasksResponseEntry_RESULT_DEDUPE_CONFLICT, entries[0].Result)

	// The dead row is untouched — still DEAD.
	stillDead := getTask(t, core, dequeued[0].Id, now)
	require.Equal(t, corepb.TaskState_TASK_STATE_DEAD, stillDead.State)
}

// TestRestartTasksGetsFreshExpiresAt confirms a restarted task's ExpiresAt
// is the brand new value the caller supplies — never the stale original
// delivery deadline, never the DLQ retention deadline it had while dead.
func TestRestartTasksGetsFreshExpiresAt(t *testing.T) {
	core := newTasksCore(t)

	accountId, queueId := rand.Uint64(), rand.Uint64()
	now := time.Now()

	e := entry("a")
	e.ExpiresAt = now.Add(time.Hour).UnixNano()
	e.RetryStrategy = &corepb.RetryStrategy{RetryIntervalsInSeconds: nil}
	enqueue(t, core, accountId, queueId, now, e)

	dequeued := dequeue(t, core, accountId, queueId, now, 10, nil)
	require.Len(t, dequeued, 1)

	dlqConfig := &corepb.DeadLetterQueueConfig{Enable: true, RetentionPeriodInSeconds: 60}
	reportStatusWithDLQConfig(t, core, dequeued[0].Id, now, corepb.ReportStatusRequestEntry_STATUS_FAILED, dequeued[0].Attempts, dlqConfig)

	dead := getTask(t, core, dequeued[0].Id, now)
	retentionExpiresAt := dead.ExpiresAt
	require.NotEqual(t, e.ExpiresAt, retentionExpiresAt)

	freshExpiresAt := now.Add(2 * time.Hour).UnixNano()
	entries := restartTasks(t, core, accountId, queueId, now, &corepb.RestartTasksRequestEntry{
		TaskId:    dequeued[0].Id.TaskId,
		ExpiresAt: freshExpiresAt,
	})
	require.Len(t, entries, 1)
	require.Equal(t, corepb.RestartTasksResponseEntry_RESULT_RESTARTED, entries[0].Result)
	require.Equal(t, freshExpiresAt, entries[0].Task.ExpiresAt)
	require.NotEqual(t, retentionExpiresAt, entries[0].Task.ExpiresAt)
	require.NotEqual(t, e.ExpiresAt, entries[0].Task.ExpiresAt)

	restarted := getTask(t, core, dequeued[0].Id, now)
	require.Equal(t, corepb.TaskState_TASK_STATE_ENQUEUED, restarted.State)
	require.EqualValues(t, 0, restarted.Attempts)
}

// TestRestartTasksSucceedsAndCompetesForThreadHead restarts a dead threaded
// task and confirms it re-competes for its thread's head by ScheduledAt,
// exactly like a fresh arrival — not appended "at the end."
func TestRestartTasksSucceedsAndCompetesForThreadHead(t *testing.T) {
	core := newTasksCore(t)

	accountId, queueId := rand.Uint64(), rand.Uint64()
	now := time.Now()

	e1 := entry("first")
	e1.ThreadId = "thread-1"
	e1.RetryStrategy = &corepb.RetryStrategy{RetryIntervalsInSeconds: nil}
	e2 := entry("second")
	e2.ThreadId = "thread-1"
	e2.ScheduledAt = now.Add(10 * time.Hour).UnixNano()

	created := enqueue(t, core, accountId, queueId, now, e1, e2)
	require.Len(t, created, 2)
	first, second := created[0], created[1]

	dequeued := dequeue(t, core, accountId, queueId, now, 10, nil)
	require.Len(t, dequeued, 1)
	require.Equal(t, first.Id.TaskId, dequeued[0].Id.TaskId)

	dlqConfig := &corepb.DeadLetterQueueConfig{Enable: true, RetentionPeriodInSeconds: 86400}
	reportStatusWithDLQConfig(t, core, dequeued[0].Id, now, corepb.ReportStatusRequestEntry_STATUS_FAILED, dequeued[0].Attempts, dlqConfig)

	// "first" is dead; "second" (still merely ENQUEUED, ScheduledAt ten
	// hours out) is now this thread's head.
	dead := getTask(t, core, dequeued[0].Id, now)
	require.Equal(t, corepb.TaskState_TASK_STATE_DEAD, dead.State)

	// Restart "first" well before "second"'s ScheduledAt: it must displace
	// "second" as head, exactly like a fresh arrival would.
	restartAt := now.Add(time.Hour)
	entries := restartTasks(t, core, accountId, queueId, restartAt, &corepb.RestartTasksRequestEntry{
		TaskId:    first.Id.TaskId,
		ExpiresAt: restartAt.Add(time.Hour).UnixNano(),
	})
	require.Len(t, entries, 1)
	require.Equal(t, corepb.RestartTasksResponseEntry_RESULT_RESTARTED, entries[0].Result)

	dequeued2 := dequeue(t, core, accountId, queueId, restartAt.Add(time.Minute), 10, nil)
	require.Len(t, dequeued2, 1)
	require.Equal(t, first.Id.TaskId, dequeued2[0].Id.TaskId)

	// Completing the restarted "first" promotes "second" next.
	reportStatus(t, core, dequeued2[0].Id, restartAt, corepb.ReportStatusRequestEntry_STATUS_SUCCEEDED, dequeued2[0].Attempts)

	promoted := dequeue(t, core, accountId, queueId, now.Add(11*time.Hour), 10, nil)
	require.Len(t, promoted, 1)
	require.Equal(t, second.Id.TaskId, promoted[0].Id.TaskId)
}

func TestDeleteTasks(t *testing.T) {
	core := newTasksCore(t)

	accountId, queueId := rand.Uint64(), rand.Uint64()
	now := time.Now()

	tasks := enqueue(t, core, accountId, queueId, now, entry("a"), entry("b"))

	deleteTasks(t, core, accountId, queueId, tasks[0].Id.TaskId)

	appErr := getTaskWithError(t, core, tasks[0].Id, now)
	require.Equal(t, mrpc.NotFound, appErr.Code)

	// Deleting an already-gone / nonexistent task id is a no-op, not an error.
	deleteTasks(t, core, accountId, queueId, tasks[0].Id.TaskId, 99999)

	fetched := getTask(t, core, tasks[1].Id, now)
	require.Equal(t, []byte("b"), fetched.Payload)
}

func TestPurgeQueue(t *testing.T) {
	core := newTasksCore(t)

	accountId, queueId := rand.Uint64(), rand.Uint64()
	now := time.Now()

	tasks := enqueue(t, core, accountId, queueId, now, entry("a"), entry("b"), entry("c"))

	purgeQueue(t, core, accountId, queueId)

	for _, task := range tasks {
		appErr := getTaskWithError(t, core, task.Id, now)
		require.Equal(t, mrpc.NotFound, appErr.Code)
	}

	stats := getStatistics(t, core, accountId, queueId, now)
	require.EqualValues(t, 0, stats.EnqueuedTasksCount)
}

func TestRunGarbageCollection(t *testing.T) {
	core := newTasksCore(t)

	accountId, queueId := rand.Uint64(), rand.Uint64()
	now := time.Now()

	expiring := entry("expiring")
	expiring.ExpiresAt = now.Add(time.Minute).UnixNano()
	notExpiring := entry("not-expiring")
	notExpiring.ExpiresAt = now.Add(time.Hour).UnixNano()

	tasks := enqueue(t, core, accountId, queueId, now, expiring, notExpiring)

	runGarbageCollection(t, core, now.Add(2*time.Minute))

	appErr := getTaskWithError(t, core, tasks[0].Id, now.Add(2*time.Minute))
	require.Equal(t, mrpc.NotFound, appErr.Code)

	stillThere := getTask(t, core, tasks[1].Id, now.Add(2*time.Minute))
	require.Equal(t, []byte("not-expiring"), stillThere.Payload)
}

// TestGetStatisticsTracksProcessedAndExpiredCounts pins down the lifetime
// counters: ProcessedTasksCount and ExpiredTasksCount accumulate
// monotonically across every path that produces a "succeeded" or "expired"
// outcome — GC sweeping an ENQUEUED task, an explicit failure discovering
// one already past its ExpiresAt while IN_PROGRESS, and a plain success —
// unlike the current-occupancy gauges, which rise and fall, and unlike an
// explicit delete, which affects neither.
func TestGetStatisticsTracksProcessedAndExpiredCounts(t *testing.T) {
	core := newTasksCore(t)

	accountId, queueId := rand.Uint64(), rand.Uint64()
	now := time.Now()

	// One task succeeds.
	enqueue(t, core, accountId, queueId, now, entry("succeed"))
	dequeued := dequeue(t, core, accountId, queueId, now, 10, nil)
	require.Len(t, dequeued, 1)
	reportStatus(t, core, dequeued[0].Id, now, corepb.ReportStatusRequestEntry_STATUS_SUCCEEDED, dequeued[0].Attempts)

	stats := getStatistics(t, core, accountId, queueId, now)
	require.EqualValues(t, 1, stats.ProcessedTasksCount)
	require.EqualValues(t, 0, stats.ExpiredTasksCount)

	// One task expires while ENQUEUED, swept by GC.
	expiringEntry := entry("expiring")
	expiringEntry.ExpiresAt = now.Add(time.Minute).UnixNano()
	enqueue(t, core, accountId, queueId, now, expiringEntry)
	runGarbageCollection(t, core, now.Add(2*time.Minute))

	stats = getStatistics(t, core, accountId, queueId, now.Add(2*time.Minute))
	require.EqualValues(t, 1, stats.ProcessedTasksCount)
	require.EqualValues(t, 1, stats.ExpiredTasksCount)

	// One task is dequeued, then its ExpiresAt passes while it's in
	// progress, discovered at explicit-failure time.
	expiringInProgressEntry := entry("expiring-in-progress")
	expiringInProgressEntry.ExpiresAt = now.Add(3 * time.Minute).UnixNano()
	created2 := enqueue(t, core, accountId, queueId, now, expiringInProgressEntry)
	dequeued2 := dequeue(t, core, accountId, queueId, now, 10, nil)
	require.Len(t, dequeued2, 1)
	require.Equal(t, created2[0].Id.TaskId, dequeued2[0].Id.TaskId)

	later := now.Add(4 * time.Minute) // past its ExpiresAt (now+3min)
	reportStatus(t, core, dequeued2[0].Id, later, corepb.ReportStatusRequestEntry_STATUS_FAILED, dequeued2[0].Attempts)

	stats = getStatistics(t, core, accountId, queueId, later)
	require.EqualValues(t, 1, stats.ProcessedTasksCount)
	require.EqualValues(t, 2, stats.ExpiredTasksCount)

	// An explicit delete affects neither lifetime counter.
	created3 := enqueue(t, core, accountId, queueId, now, entry("explicit-delete"))
	deleteTasks(t, core, accountId, queueId, created3[0].Id.TaskId)

	final := getStatistics(t, core, accountId, queueId, later)
	require.EqualValues(t, 1, final.ProcessedTasksCount)
	require.EqualValues(t, 2, final.ExpiredTasksCount)
}

// ExpiresAt is always a real, positive deadline by the time a task reaches
// this core (the server handler and the cron worker both fill in a default
// from the queue's ExpiresInSeconds) — there is no "never expires" sentinel
// value. These tests defensively pin down that Dequeue treats ExpiresAt == 0
// the same way GetTask and RunTasksGarbageCollection already do (as already
// expired), instead of exempting it as "never expires": if that invariant
// were ever violated, the task must not be handed out, and must be deleted
// outright rather than left behind — the exact bug this closes.
func TestDequeueDeletesZeroExpiresAtEnqueuedTaskInsteadOfDequeuingIt(t *testing.T) {
	core := newTasksCore(t)

	accountId, queueId := rand.Uint64(), rand.Uint64()
	now := time.Now()

	e := entry("zero-expires-at")
	e.ExpiresAt = 0

	tasks := enqueue(t, core, accountId, queueId, now, e)
	require.Len(t, tasks, 1)

	dequeued := dequeue(t, core, accountId, queueId, now, 10, nil)
	require.Len(t, dequeued, 0)

	// It must have been deleted outright, not left behind for GetTask.
	appErr := getTaskWithError(t, core, tasks[0].Id, now)
	require.Equal(t, mrpc.NotFound, appErr.Code)

	stats := getStatistics(t, core, accountId, queueId, now)
	require.EqualValues(t, 0, stats.EnqueuedTasksCount)
}

func TestDequeueDeletesZeroExpiresAtInProgressTaskInsteadOfRedequeuingIt(t *testing.T) {
	core := newTasksCore(t)

	accountId, queueId := rand.Uint64(), rand.Uint64()
	now := time.Now()

	// Enqueue with a real ExpiresAt so the first Dequeue call accepts it and
	// puts it in progress...
	e := entry("in-progress")
	e.KeepaliveTimeoutInSeconds = 5

	tasks := enqueue(t, core, accountId, queueId, now, e)
	require.Len(t, tasks, 1)

	dequeued := dequeue(t, core, accountId, queueId, now, 10, nil)
	require.Len(t, dequeued, 1)
	require.Equal(t, corepb.TaskState_TASK_STATE_IN_PROGRESS, dequeued[0].State)

	// ...then force the stored task's ExpiresAt to 0 directly, simulating a
	// violation of the invariant that could never happen through the public
	// API (Enqueue would have swept it as expired before it ever became
	// in-progress).
	txn := core.badgerStore.Update()
	stored, err := core.tasks.Get(txn, tasks[0].Id)
	require.NoError(t, err)
	stored.ExpiresAt = 0
	require.NoError(t, core.tasks.set(txn, stored))
	require.NoError(t, txn.Commit())

	// Advance past the keepalive timeout, making it eligible for re-dequeuing.
	later := now.Add(time.Hour)
	redequeued := dequeue(t, core, accountId, queueId, later, 10, nil)
	require.Len(t, redequeued, 0)

	// It must have been deleted outright, not marked dead or left in progress.
	appErr := getTaskWithError(t, core, tasks[0].Id, later)
	require.Equal(t, mrpc.NotFound, appErr.Code)

	stats := getStatistics(t, core, accountId, queueId, later)
	require.EqualValues(t, 0, stats.InProgressTasksCount)
	require.EqualValues(t, 0, stats.DeadTasksCount)
}

// TestKeepaliveTimeoutAppliesRetryBackoffInsteadOfInstantRedelivery pins down
// that a keepalive timeout is treated exactly like an explicit
// ReportStatus(FAILED) — it applies the configured RetryStrategy backoff and
// returns the task to ENQUEUED, rather than redelivering it instantly within
// the same Dequeue call.
func TestKeepaliveTimeoutAppliesRetryBackoffInsteadOfInstantRedelivery(t *testing.T) {
	core := newTasksCore(t)

	accountId, queueId := rand.Uint64(), rand.Uint64()
	now := time.Now()

	e := entry("a")
	e.KeepaliveTimeoutInSeconds = 5
	e.RetryStrategy = &corepb.RetryStrategy{RetryIntervalsInSeconds: []int64{10, 20}}
	enqueue(t, core, accountId, queueId, now, e)

	dequeued := dequeue(t, core, accountId, queueId, now, 10, nil)
	require.Len(t, dequeued, 1)
	require.EqualValues(t, 1, dequeued[0].Attempts)

	// Advance well past the 5s keepalive timeout: the lapsed lease is
	// discovered and must NOT be redelivered instantly within this same
	// call — it goes back to ENQUEUED with a backoff instead.
	later := now.Add(time.Minute)
	redelivered := dequeue(t, core, accountId, queueId, later, 10, nil)
	require.Len(t, redelivered, 0)

	fetched := getTask(t, core, dequeued[0].Id, later)
	require.Equal(t, corepb.TaskState_TASK_STATE_ENQUEUED, fetched.State)
	require.Equal(t, later.Add(10*time.Second).UnixNano(), fetched.ScheduledAt)

	// Once its backoff elapses, it becomes dequeueable again — as a fresh
	// delivery attempt (Attempts increments), not a mere redelivery.
	afterBackoff := later.Add(10 * time.Second)
	redequeued := dequeue(t, core, accountId, queueId, afterBackoff, 10, nil)
	require.Len(t, redequeued, 1)
	require.EqualValues(t, 2, redequeued[0].Attempts)
}

// TestReportStatusIgnoresStaleAttempt pins down attempt fencing: a report
// whose Attempt no longer matches the task's current Attempts (the worker's
// lease has already moved on) is a silent no-op, never a mutation — for
// every status, not just some.
func TestReportStatusIgnoresStaleAttempt(t *testing.T) {
	core := newTasksCore(t)

	accountId, queueId := rand.Uint64(), rand.Uint64()
	now := time.Now()

	enqueue(t, core, accountId, queueId, now, entry("a"))
	dequeued := dequeue(t, core, accountId, queueId, now, 10, nil)
	require.Len(t, dequeued, 1)
	require.EqualValues(t, 1, dequeued[0].Attempts)

	staleAttempt := dequeued[0].Attempts + 1

	// A stale IN_PROGRESS (heartbeat) report must not extend the lease.
	before := getTask(t, core, dequeued[0].Id, now)
	reportStatus(t, core, dequeued[0].Id, now, corepb.ReportStatusRequestEntry_STATUS_IN_PROGRESS, staleAttempt)
	afterStaleHeartbeat := getTask(t, core, dequeued[0].Id, now)
	require.Equal(t, before.VisibleAt, afterStaleHeartbeat.VisibleAt)

	// A stale FAILED report must not retry or dead-letter it.
	reportStatus(t, core, dequeued[0].Id, now, corepb.ReportStatusRequestEntry_STATUS_FAILED, staleAttempt)
	afterStaleFailed := getTask(t, core, dequeued[0].Id, now)
	require.Equal(t, corepb.TaskState_TASK_STATE_IN_PROGRESS, afterStaleFailed.State)

	// A stale SUCCEEDED report must not complete it.
	reportStatus(t, core, dequeued[0].Id, now, corepb.ReportStatusRequestEntry_STATUS_SUCCEEDED, staleAttempt)
	stillThere := getTask(t, core, dequeued[0].Id, now)
	require.Equal(t, corepb.TaskState_TASK_STATE_IN_PROGRESS, stillThere.State)

	// The correct attempt is still honored.
	reportStatus(t, core, dequeued[0].Id, now, corepb.ReportStatusRequestEntry_STATUS_SUCCEEDED, dequeued[0].Attempts)
	appErr := getTaskWithError(t, core, dequeued[0].Id, now)
	require.Equal(t, mrpc.NotFound, appErr.Code)
}

// TestInProgressTaskIsNeverHiddenOrReapedAsExpired pins down that ExpiresAt
// is never enforced against a genuinely IN_PROGRESS task, either by GetTask
// or by background GC — only by a checkpoint (explicit failure or keepalive
// timeout) that would otherwise hand it back to ENQUEUED or DEAD.
func TestInProgressTaskIsNeverHiddenOrReapedAsExpired(t *testing.T) {
	core := newTasksCore(t)

	accountId, queueId := rand.Uint64(), rand.Uint64()
	now := time.Now()

	e := entry("a")
	e.ExpiresAt = now.Add(time.Minute).UnixNano()
	enqueue(t, core, accountId, queueId, now, e)

	dequeued := dequeue(t, core, accountId, queueId, now, 10, nil)
	require.Len(t, dequeued, 1)

	// Long past ExpiresAt, but still genuinely in progress (keepalive not
	// lapsed — entry()'s default KeepaliveTimeoutInSeconds is 30s and this
	// check runs at +2 minutes only for GetTask/GC, not by redequeuing).
	later := now.Add(2 * time.Minute)

	fetched := getTask(t, core, dequeued[0].Id, later)
	require.Equal(t, corepb.TaskState_TASK_STATE_IN_PROGRESS, fetched.State)

	runGarbageCollection(t, core, later)

	stillThere := getTask(t, core, dequeued[0].Id, later)
	require.Equal(t, corepb.TaskState_TASK_STATE_IN_PROGRESS, stillThere.State)

	stats := getStatistics(t, core, accountId, queueId, later)
	require.EqualValues(t, 1, stats.InProgressTasksCount)
}

// TestRetriedTaskIsReapedByGCOnceItsOriginalExpiresAtPasses confirms the
// other half of the expiration-index toggle: once a task returns to
// ENQUEUED after a retry, its (unchanged) ExpiresAt is tracked again, so GC
// can still reap it if that deadline has since passed.
func TestRetriedTaskIsReapedByGCOnceItsOriginalExpiresAtPasses(t *testing.T) {
	core := newTasksCore(t)

	accountId, queueId := rand.Uint64(), rand.Uint64()
	now := time.Now()

	e := entry("a")
	e.ExpiresAt = now.Add(20 * time.Second).UnixNano()
	e.RetryStrategy = &corepb.RetryStrategy{RetryIntervalsInSeconds: []int64{5}}
	enqueue(t, core, accountId, queueId, now, e)

	dequeued := dequeue(t, core, accountId, queueId, now, 10, nil)
	require.Len(t, dequeued, 1)

	reportStatus(t, core, dequeued[0].Id, now, corepb.ReportStatusRequestEntry_STATUS_FAILED, dequeued[0].Attempts)

	fetched := getTask(t, core, dequeued[0].Id, now)
	require.Equal(t, corepb.TaskState_TASK_STATE_ENQUEUED, fetched.State)

	// Past both the retry's ScheduledAt (now+5s) and the original ExpiresAt
	// (now+20s).
	later := now.Add(30 * time.Second)
	runGarbageCollection(t, core, later)

	appErr := getTaskWithError(t, core, dequeued[0].Id, later)
	require.Equal(t, mrpc.NotFound, appErr.Code)
}

func TestThreadedTasksOnlyOneHeadVisibleAtATime(t *testing.T) {
	core := newTasksCore(t)

	accountId, queueId := rand.Uint64(), rand.Uint64()
	now := time.Now()

	e1 := entry("first")
	e1.ThreadId = "thread-1"
	e2 := entry("second")
	e2.ThreadId = "thread-1"
	e2.ScheduledAt = now.Add(time.Second).UnixNano()

	created := enqueue(t, core, accountId, queueId, now, e1, e2)
	require.Len(t, created, 2)

	// Only the head (the earliest-scheduled task of the thread) is dequeueable.
	dequeued := dequeue(t, core, accountId, queueId, now.Add(time.Hour), 10, nil)
	require.Len(t, dequeued, 1)
	require.Equal(t, []byte("first"), dequeued[0].Payload)

	// Completing the head promotes the second task to head, making it dequeueable.
	reportStatus(t, core, dequeued[0].Id, now, corepb.ReportStatusRequestEntry_STATUS_SUCCEEDED, dequeued[0].Attempts)

	dequeued2 := dequeue(t, core, accountId, queueId, now.Add(time.Hour), 10, nil)
	require.Len(t, dequeued2, 1)
	require.Equal(t, []byte("second"), dequeued2[0].Payload)
}

// TestFreshEnqueueDisplacesExistingThreadHeadAcrossSeparateCalls locks in a
// fix to attachTaskToThread/reconcileThreadHead: a brand new task (never
// persisted before under its ID) that turns out to be its thread's earliest
// member must still correctly become head — reconcileThreadHead re-reads a
// task's row by ID to compare ScheduledAt, which used to happen before
// createTask persisted the new arrival's own row (harmless for a same-batch
// later-scheduled sibling, but wrong the moment the new arrival is itself
// the earliest: it read either stale prior data under a reused ID, or
// ErrNotFound). Enqueuing across two separate calls (not one multi-entry
// batch) ensures the first task is fully committed before the second one
// (the new, earlier-scheduled arrival) is created.
func TestFreshEnqueueDisplacesExistingThreadHeadAcrossSeparateCalls(t *testing.T) {
	core := newTasksCore(t)

	accountId, queueId := rand.Uint64(), rand.Uint64()
	now := time.Now()

	e1 := entry("first")
	e1.ThreadId = "thread-1"
	e1.ScheduledAt = now.Add(time.Hour).UnixNano()
	first := enqueue(t, core, accountId, queueId, now, e1)
	require.Len(t, first, 1)

	// A separate Enqueue call, scheduled well before "first": it must
	// displace "first" as head.
	e2 := entry("second")
	e2.ThreadId = "thread-1"
	second := enqueue(t, core, accountId, queueId, now, e2)
	require.Len(t, second, 1)

	dequeued := dequeue(t, core, accountId, queueId, now.Add(2*time.Hour), 10, nil)
	require.Len(t, dequeued, 1)
	require.Equal(t, second[0].Id.TaskId, dequeued[0].Id.TaskId)

	reportStatus(t, core, dequeued[0].Id, now, corepb.ReportStatusRequestEntry_STATUS_SUCCEEDED, dequeued[0].Attempts)

	promoted := dequeue(t, core, accountId, queueId, now.Add(2*time.Hour), 10, nil)
	require.Len(t, promoted, 1)
	require.Equal(t, first[0].Id.TaskId, promoted[0].Id.TaskId)
}

// TestDedupeKeyOverwriteScheduledAtOnNonHeadThreadedTaskDoesNotCorruptIndexes
// pins down the fix for the bug where OVERWRITE_ON_DUPLICATE_SCHEDULED_AT on
// a threaded task that is not currently its thread's head would insert the
// non-head task directly into queueIndex (making it dequeueable ahead of the
// real head) and orphan its old threadedTasksIndex entry.
func TestDedupeKeyOverwriteScheduledAtOnNonHeadThreadedTaskDoesNotCorruptIndexes(t *testing.T) {
	core := newTasksCore(t)

	accountId, queueId := rand.Uint64(), rand.Uint64()
	now := time.Now()

	e1 := entry("first")
	e1.ThreadId = "thread-1"
	e2 := entry("second")
	e2.ThreadId = "thread-1"
	e2.ScheduledAt = now.Add(time.Hour).UnixNano()
	e2.DedupeKey = "dedupe-1"

	created := enqueue(t, core, accountId, queueId, now, e1, e2)
	require.Len(t, created, 2)
	head, nonHead := created[0], created[1]

	// The second task is not the head: it must not be dequeueable yet.
	dequeued := dequeue(t, core, accountId, queueId, now.Add(2*time.Hour), 10, nil)
	require.Len(t, dequeued, 1)
	require.Equal(t, head.Id.TaskId, dequeued[0].Id.TaskId)

	// Now re-enqueue the non-head's dedupe key with a scheduled time in the
	// past: pre-fix, this incorrectly spliced the non-head task straight
	// into queueIndex.
	e3 := entry("second-overwritten")
	e3.DedupeKey = "dedupe-1"
	e3.OverwriteOnDuplicate = []corepb.EnqueueRequestEntry_OverwriteOnDuplicate{corepb.EnqueueRequestEntry_OVERWRITE_ON_DUPLICATE_SCHEDULED_AT}
	overwritten := enqueue(t, core, accountId, queueId, now.Add(2*time.Hour), e3)
	require.Len(t, overwritten, 1)
	require.Equal(t, nonHead.Id.TaskId, overwritten[0].Id.TaskId)

	// The head is still in progress, so the rescheduled task must still not
	// be dequeueable — only one task per thread is ever in flight.
	dequeuedAgain := dequeue(t, core, accountId, queueId, now.Add(2*time.Hour), 10, nil)
	require.Len(t, dequeuedAgain, 0)

	// Completing the head must promote the rescheduled task to head cleanly
	// (no orphaned index entry blocking or duplicating it).
	reportStatus(t, core, head.Id, now, corepb.ReportStatusRequestEntry_STATUS_SUCCEEDED, head.Attempts)

	promoted := dequeue(t, core, accountId, queueId, now.Add(3*time.Hour), 10, nil)
	require.Len(t, promoted, 1)
	require.Equal(t, nonHead.Id.TaskId, promoted[0].Id.TaskId)
	// Only OVERWRITE_ON_DUPLICATE_SCHEDULED_AT was requested, so the payload
	// is still the original ("second"), not the entry's ("second-overwritten").
	require.Equal(t, []byte("second"), promoted[0].Payload)

	// Nothing else is left dangling in the thread.
	moreDequeued := dequeue(t, core, accountId, queueId, now.Add(3*time.Hour), 10, nil)
	require.Len(t, moreDequeued, 0)
}

// TestDedupeKeyOverwriteScheduledAtCanPromoteThreadedTaskToHead exercises the
// other half of the same fix: rescheduling a non-head threaded task to
// *before* the current head must promote it to head, but only because the
// current head is still merely enqueued (not already in progress).
func TestDedupeKeyOverwriteScheduledAtCanPromoteThreadedTaskToHead(t *testing.T) {
	core := newTasksCore(t)

	accountId, queueId := rand.Uint64(), rand.Uint64()
	now := time.Now()

	e1 := entry("first")
	e1.ThreadId = "thread-1"
	e1.ScheduledAt = now.Add(30 * time.Minute).UnixNano()
	e2 := entry("second")
	e2.ThreadId = "thread-1"
	e2.ScheduledAt = now.Add(time.Hour).UnixNano()
	e2.DedupeKey = "dedupe-1"

	created := enqueue(t, core, accountId, queueId, now, e1, e2)
	require.Len(t, created, 2)
	head, nonHead := created[0], created[1]

	// Reschedule the non-head task to before the current head's 30-minute
	// mark, without dequeuing the head first (it is still merely ENQUEUED).
	e3 := entry("second-overwritten")
	e3.DedupeKey = "dedupe-1"
	e3.OverwriteOnDuplicate = []corepb.EnqueueRequestEntry_OverwriteOnDuplicate{corepb.EnqueueRequestEntry_OVERWRITE_ON_DUPLICATE_SCHEDULED_AT}
	overwritten := enqueue(t, core, accountId, queueId, now, e3)
	require.Len(t, overwritten, 1)
	require.Equal(t, nonHead.Id.TaskId, overwritten[0].Id.TaskId)

	// The rescheduled task displaced the old head and is now dequeueable.
	dequeued := dequeue(t, core, accountId, queueId, now.Add(time.Hour), 10, nil)
	require.Len(t, dequeued, 1)
	require.Equal(t, nonHead.Id.TaskId, dequeued[0].Id.TaskId)

	// Completing the new head promotes the original task next.
	reportStatus(t, core, dequeued[0].Id, now, corepb.ReportStatusRequestEntry_STATUS_SUCCEEDED, dequeued[0].Attempts)

	promoted := dequeue(t, core, accountId, queueId, now.Add(time.Hour), 10, nil)
	require.Len(t, promoted, 1)
	require.Equal(t, head.Id.TaskId, promoted[0].Id.TaskId)
}

// TestFailedThreadedTaskRetryCanBePassedByAnEarlierSibling exercises
// rescheduleTaskInThread's other caller: a thread head that fails and is
// rescheduled for retry must yield the head to a sibling that is now
// scheduled earlier than the new retry time, instead of unconditionally
// re-claiming the head as the pre-refactor code did.
func TestFailedThreadedTaskRetryCanBePassedByAnEarlierSibling(t *testing.T) {
	core := newTasksCore(t)

	accountId, queueId := rand.Uint64(), rand.Uint64()
	now := time.Now()

	e1 := entry("first")
	e1.ThreadId = "thread-1"
	// Two retry intervals: Attempts is already 1 after the first dequeue, so
	// a single-interval strategy would exhaust on this very failure (see
	// TestReportStatusFailedRetriesThenDies) instead of retrying.
	e1.RetryStrategy = &corepb.RetryStrategy{RetryIntervalsInSeconds: []int64{10, 20}}
	e2 := entry("second")
	e2.ThreadId = "thread-1"
	e2.ScheduledAt = now.Add(5 * time.Second).UnixNano()

	created := enqueue(t, core, accountId, queueId, now, e1, e2)
	require.Len(t, created, 2)
	first, second := created[0], created[1]

	dequeued := dequeue(t, core, accountId, queueId, now, 10, nil)
	require.Len(t, dequeued, 1)
	require.Equal(t, first.Id.TaskId, dequeued[0].Id.TaskId)

	// Fail it: its retry strategy schedules it 10s out, but "second" is
	// already scheduled 5s out — second must become head instead.
	reportStatus(t, core, dequeued[0].Id, now, corepb.ReportStatusRequestEntry_STATUS_FAILED, dequeued[0].Attempts)

	dequeued2 := dequeue(t, core, accountId, queueId, now.Add(time.Minute), 10, nil)
	require.Len(t, dequeued2, 1)
	require.Equal(t, second.Id.TaskId, dequeued2[0].Id.TaskId)

	// Completing "second" promotes the retried "first" back to head.
	reportStatus(t, core, dequeued2[0].Id, now, corepb.ReportStatusRequestEntry_STATUS_SUCCEEDED, dequeued2[0].Attempts)

	dequeued3 := dequeue(t, core, accountId, queueId, now.Add(time.Minute), 10, nil)
	require.Len(t, dequeued3, 1)
	require.Equal(t, first.Id.TaskId, dequeued3[0].Id.TaskId)
}

// TestAgeOfOldestEnqueuedTaskExcludesNonHeadThreadedTasks documents that
// getAgeOfOldestEnqueuedTask only scans queueIndex (thread heads and
// non-threaded tasks), so a non-head task waiting behind a long-running
// in-progress head does not count toward the statistic even though it is
// genuinely ENQUEUED: there is nothing to dequeue for that thread until its
// head finishes, so the statistic tracks the oldest task that is actually
// actionable right now.
func TestAgeOfOldestEnqueuedTaskExcludesNonHeadThreadedTasks(t *testing.T) {
	core := newTasksCore(t)

	accountId, queueId := rand.Uint64(), rand.Uint64()
	now := time.Now()

	// Enqueue and immediately dequeue the thread's head, so it becomes
	// IN_PROGRESS and leaves queueIndex.
	head := entry("head")
	head.ThreadId = "thread-1"
	enqueue(t, core, accountId, queueId, now, head)
	dequeued := dequeue(t, core, accountId, queueId, now, 10, nil)
	require.Len(t, dequeued, 1)

	// A same-thread task enqueued right after is ENQUEUED but not head — the
	// in-progress head is never preempted — so it sits waiting behind it,
	// invisible to queueIndex.
	nonHead := entry("non-head")
	nonHead.ThreadId = "thread-1"
	enqueue(t, core, accountId, queueId, now, nonHead)

	// An hour later, with the head still in progress (never reported/timed
	// out) and no other task in queueIndex, the statistic reports zero: there
	// is nothing currently dequeueable in this queue, even though the
	// non-head task has genuinely been waiting an hour.
	stats := getStatistics(t, core, accountId, queueId, now.Add(time.Hour))
	require.Zero(t, stats.AgeOfOldestEnqueuedTask)
}

func newTasksCore(t *testing.T) *Core {
	t.Helper()

	badgerStore, err := store.NewBadgerInMemoryStore()
	require.NoError(t, err)

	return NewCore(badgerStore, []byte{0x1d, 0x36, 0x00, 0x00}, 0x00000000, 0xffffffff)
}

// enabledDLQConfig returns a valid, enabled DeadLetterQueueConfig with a
// generous retention period and no max_size cap, for tests that need
// retry-exhausted tasks to actually land in the DLQ rather than being
// deleted outright (the default when no config is passed at all).
func enabledDLQConfig() *corepb.DeadLetterQueueConfig {
	return &corepb.DeadLetterQueueConfig{
		Enable:                   true,
		RetentionPeriodInSeconds: 86400,
	}
}

func entry(payload string) *corepb.EnqueueRequestEntry {
	return &corepb.EnqueueRequestEntry{
		Payload:                   []byte(payload),
		KeepaliveTimeoutInSeconds: 30,
		RetryStrategy:             &corepb.RetryStrategy{RetryIntervalsInSeconds: []int64{10, 20, 30}},
		ExpiresAt:                 time.Now().Add(24 * time.Hour).UnixNano(),
	}
}

func enqueue(t *testing.T, core *Core, accountId, queueId uint64, now time.Time, entries ...*corepb.EnqueueRequestEntry) []*corepb.Task {
	t.Helper()

	resp, err := core.Enqueue(&coreapis.EnqueueRequest{
		Payload: &corepb.EnqueueRequest{
			QueueId: &corepb.QueueId{AccountId: accountId, QueueId: queueId},
			Entries: entries,
		},
		Now: now.UnixNano(),
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Nil(t, resp.ApplicationError)
	require.NotNil(t, resp.Payload)

	return resp.Payload.Tasks
}

func getTask(t *testing.T, core *Core, taskId *corepb.TaskId, now time.Time) *corepb.Task {
	t.Helper()

	resp, err := core.GetTask(&coreapis.GetTaskRequest{
		Payload: &corepb.GetTaskRequest{TaskId: taskId},
		Now:     now.UnixNano(),
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Nil(t, resp.ApplicationError)
	require.NotNil(t, resp.Payload)
	require.NotNil(t, resp.Payload.Task)

	return resp.Payload.Task
}

func getTaskWithError(t *testing.T, core *Core, taskId *corepb.TaskId, now time.Time) *mrpc.Error {
	t.Helper()

	resp, err := core.GetTask(&coreapis.GetTaskRequest{
		Payload: &corepb.GetTaskRequest{TaskId: taskId},
		Now:     now.UnixNano(),
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.NotNil(t, resp.ApplicationError)

	return resp.ApplicationError
}

func dequeue(t *testing.T, core *Core, accountId, queueId uint64, now time.Time, limit int32, settings *corepb.DequeuingSettings) []*corepb.Task {
	t.Helper()

	resp, err := core.Dequeue(&coreapis.DequeueRequest{
		Payload: &corepb.DequeueRequest{
			QueueId:           &corepb.QueueId{AccountId: accountId, QueueId: queueId},
			DequeuingSettings: settings,
			DequeueLimit:      limit,
		},
		Now: now.UnixNano(),
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Nil(t, resp.ApplicationError)
	require.NotNil(t, resp.Payload)

	return resp.Payload.Tasks
}

func reportStatus(t *testing.T, core *Core, taskId *corepb.TaskId, now time.Time, status corepb.ReportStatusRequestEntry_Status, attempt int32) {
	t.Helper()
	reportStatusWithDLQConfig(t, core, taskId, now, status, attempt, nil)
}

// reportStatusWithDLQConfig is reportStatus with an explicit
// DeadLetterQueueConfig, for tests exercising failTaskToDead's DLQ-aware
// behavior (a nil config, what plain reportStatus passes, means "no DLQ
// configured" — retry-exhausted tasks are deleted outright, not
// dead-lettered).
func reportStatusWithDLQConfig(t *testing.T, core *Core, taskId *corepb.TaskId, now time.Time, status corepb.ReportStatusRequestEntry_Status, attempt int32, dlqConfig *corepb.DeadLetterQueueConfig) {
	t.Helper()

	resp, err := core.ReportStatus(&coreapis.ReportStatusRequest{
		Payload: &corepb.ReportStatusRequest{
			QueueId: &corepb.QueueId{AccountId: taskId.AccountId, QueueId: taskId.QueueId},
			Entries: []*corepb.ReportStatusRequestEntry{
				{TaskId: taskId, Attempt: attempt, Status: status},
			},
			DeadLetterQueueConfig: dlqConfig,
		},
		Now: now.UnixNano(),
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Nil(t, resp.ApplicationError)
}

func deleteTasks(t *testing.T, core *Core, accountId, queueId uint64, taskIds ...uint64) {
	t.Helper()

	resp, err := core.DeleteTasks(&coreapis.DeleteTasksRequest{
		Payload: &corepb.DeleteTasksRequest{
			QueueId: &corepb.QueueId{AccountId: accountId, QueueId: queueId},
			TaskIds: taskIds,
		},
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Nil(t, resp.ApplicationError)
}

func restartTasks(t *testing.T, core *Core, accountId, queueId uint64, now time.Time, entries ...*corepb.RestartTasksRequestEntry) []*corepb.RestartTasksResponseEntry {
	t.Helper()

	resp, err := core.RestartTasks(&coreapis.RestartTasksRequest{
		Payload: &corepb.RestartTasksRequest{
			QueueId: &corepb.QueueId{AccountId: accountId, QueueId: queueId},
			Entries: entries,
		},
		Now: now.UnixNano(),
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Nil(t, resp.ApplicationError)
	require.NotNil(t, resp.Payload)

	return resp.Payload.Entries
}

func purgeQueue(t *testing.T, core *Core, accountId, queueId uint64) {
	t.Helper()

	resp, err := core.PurgeQueue(&coreapis.PurgeQueueRequest{
		Payload: &corepb.PurgeQueueRequest{QueueId: &corepb.QueueId{AccountId: accountId, QueueId: queueId}},
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Nil(t, resp.ApplicationError)
}

func runGarbageCollection(t *testing.T, core *Core, now time.Time) {
	t.Helper()

	resp, err := core.RunTasksGarbageCollection(&coreapis.RunTasksGarbageCollectionRequest{
		Payload: &corepb.RunTasksGarbageCollectionRequest{},
		Now:     now.UnixNano(),
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Nil(t, resp.ApplicationError)
}

func getStatistics(t *testing.T, core *Core, accountId, queueId uint64, now time.Time) *corepb.GetStatisticsResponse {
	t.Helper()

	resp, err := core.GetStatistics(&coreapis.GetStatisticsRequest{
		Payload: &corepb.GetStatisticsRequest{QueueId: &corepb.QueueId{AccountId: accountId, QueueId: queueId}},
		Now:     now.UnixNano(),
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Nil(t, resp.ApplicationError)
	require.NotNil(t, resp.Payload)

	return resp.Payload
}
