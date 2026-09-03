package queues

import (
	"bytes"
	"io"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/adhocore/gronx"
	mrpc "github.com/evrblk/monstera/rpc"
	"github.com/evrblk/monstera/store"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"

	"github.com/evrblk/moab/pkg/coreapis"
	"github.com/evrblk/moab/pkg/corepb"
)

func TestCore_CreateAndGetQueue(t *testing.T) {
	core := newQueuesCore(t)

	accountId := rand.Uint64()
	now := time.Now()

	queue := createQueue(t, core, &corepb.QueueId{AccountId: accountId, QueueId: rand.Uint64()}, "test_queue", 20, now)
	require.Equal(t, "test description", queue.Description)
	require.Equal(t, now.UnixNano(), queue.CreatedAt)
	require.Equal(t, now.UnixNano(), queue.UpdatedAt)
	require.EqualValues(t, 15, queue.KeepaliveTimeoutInSeconds)
	require.EqualValues(t, 1, queue.Version)
	require.Equal(t, []int64{10, 20, 30}, queue.RetryStrategy.RetryIntervalsInSeconds)
	require.EqualValues(t, 100, queue.DequeuingSettings.MaxInProgressTasks)
	require.EqualValues(t, 1, queue.DequeuingSettings.RateLimiting.Interval)
	require.Equal(t, corepb.IntervalUnit_INTERVAL_UNIT_SECONDS, queue.DequeuingSettings.RateLimiting.IntervalUnit)
	require.EqualValues(t, 100, queue.DequeuingSettings.RateLimiting.MaxTokens)
	require.Equal(t, true, queue.DeadLetterQueueConfig.Enable)
	require.EqualValues(t, 100, queue.DeadLetterQueueConfig.MaxSize)
	require.EqualValues(t, 86400, queue.DeadLetterQueueConfig.RetentionPeriodInSeconds)

	// Get this newly created queue
	fetched := getQueue(t, core, queue.Id)
	require.Equal(t, queue.Id.QueueId, fetched.Queue.Id.QueueId)
	require.Equal(t, "test_queue", fetched.Queue.Name)

	// Create another queue with the same name
	appErr := createQueueWithError(t, core, &corepb.QueueId{AccountId: accountId, QueueId: rand.Uint64()}, "test_queue", 20, now)
	require.Equal(t, mrpc.AlreadyExists, appErr.Code)

	// Get nonexistent queue
	getErr := getQueueWithError(t, core, &corepb.QueueId{AccountId: accountId, QueueId: rand.Uint64()})
	require.Equal(t, mrpc.NotFound, getErr.Code)
}

func TestCore_MaxNumberOfQueues(t *testing.T) {
	core := newQueuesCore(t)

	now := time.Now()
	accountId := rand.Uint64()

	// Create the first queue with limit = 1
	createQueue(t, core, &corepb.QueueId{AccountId: accountId, QueueId: rand.Uint64()}, "test_queue_1", 1, now)

	// Create another queue for the same account with limit = 1
	appErr := createQueueWithError(t, core, &corepb.QueueId{AccountId: accountId, QueueId: rand.Uint64()}, "test_queue_2", 1, now.Add(time.Second))
	require.Equal(t, mrpc.ResourceExhausted, appErr.Code)

	// Create another queue for the same account with limit = 2
	createQueue(t, core, &corepb.QueueId{AccountId: accountId, QueueId: rand.Uint64()}, "test_queue_2", 2, now.Add(time.Second))
}

// Should reject creating a queue whose (randomly generated) ID collides with
// an existing queue's ID, even when the name is different — proving the
// collision, not the name check, is what's rejecting it.
func TestCore_CreateQueueIDCollision(t *testing.T) {
	core := newQueuesCore(t)

	now := time.Now()
	queueId := &corepb.QueueId{AccountId: rand.Uint64(), QueueId: rand.Uint64()}

	createQueue(t, core, queueId, "test_queue_1", 20, now)

	// Reusing the same QueueId with a different name: the name-uniqueness
	// check would pass, so this exercises the ID-collision check specifically.
	appErr := createQueueWithError(t, core, queueId, "test_queue_2", 20, now.Add(time.Second))
	require.Equal(t, mrpc.IDCollision, appErr.Code)
}

// Should create a schedule for a given existing queue.
func TestCore_CreateSchedule(t *testing.T) {
	core := newQueuesCore(t)

	now := time.Date(2024, 11, 9, 9, 31, 13, 0, time.UTC)
	accountId := rand.Uint64()

	queue := createQueue(t, core, &corepb.QueueId{AccountId: accountId, QueueId: rand.Uint64()}, "test_queue", 20, now)

	schedule := createSchedule(t, core, accountId, "test_queue", "test_schedule", now)
	require.Equal(t, "Just do it!", schedule.Description)
	require.Equal(t, now.UnixNano(), schedule.CreatedAt)
	require.Equal(t, now.UnixNano(), schedule.UpdatedAt)
	require.Equal(t, "*/5 * * * *", schedule.Cron)
	require.Equal(t, []byte("payload"), schedule.Payload)
	require.Equal(t, "dedupe", schedule.DedupeKey)
	require.EqualValues(t, 600, schedule.ExpiresInSeconds)
	require.EqualValues(t, 15, schedule.KeepaliveTimeoutInSeconds)
	require.EqualValues(t, 1, schedule.Version)
	require.Equal(t, "America/Los_Angeles", schedule.Timezone)

	// List schedules for this queue
	schedules := listSchedules(t, core, queue.Id, nil, 0)
	require.Len(t, schedules.Schedules, 1)
	require.Equal(t, "test_schedule", schedules.Schedules[0].Name)

	// The queue's denormalized schedule count should reflect the new schedule
	fetched := getQueue(t, core, queue.Id)
	require.EqualValues(t, 1, fetched.Queue.SchedulesCount)

	// Create another schedule with the same name
	appErr := createScheduleWithError(t, core, accountId, "test_queue", "test_schedule", now)
	require.Equal(t, mrpc.AlreadyExists, appErr.Code)

	// The schedule count should not change on a rejected create
	fetched = getQueue(t, core, queue.Id)
	require.EqualValues(t, 1, fetched.Queue.SchedulesCount)
}

// Should reject creating a schedule once the queue's schedule count reaches
// MaxNumberOfSchedulesPerQueue, checked against the denormalized
// Queue.SchedulesCount rather than a full scan of existing schedules.
func TestCore_MaxNumberOfSchedulesPerQueue(t *testing.T) {
	core := newQueuesCore(t)

	now := time.Date(2024, 11, 9, 9, 31, 13, 0, time.UTC)
	accountId := rand.Uint64()

	queue := createQueue(t, core, &corepb.QueueId{AccountId: accountId, QueueId: rand.Uint64()}, "test_queue", 20, now)

	resp1, err := core.CreateSchedule(&coreapis.CreateScheduleRequest{
		Payload: &corepb.CreateScheduleRequest{
			AccountId:                    accountId,
			QueueName:                    "test_queue",
			ScheduleId:                   rand.Uint64(),
			ScheduleName:                 "schedule_1",
			Cron:                         "*/5 * * * *",
			Timezone:                     "UTC",
			MaxNumberOfSchedulesPerQueue: 1,
		},
		Now: now.UnixNano(),
	})
	require.NoError(t, err)
	require.Nil(t, resp1.ApplicationError)

	// Creating a second schedule for the same queue with limit = 1 should be rejected
	resp2, err := core.CreateSchedule(&coreapis.CreateScheduleRequest{
		Payload: &corepb.CreateScheduleRequest{
			AccountId:                    accountId,
			QueueName:                    "test_queue",
			ScheduleId:                   rand.Uint64(),
			ScheduleName:                 "schedule_2",
			Cron:                         "*/5 * * * *",
			Timezone:                     "UTC",
			MaxNumberOfSchedulesPerQueue: 1,
		},
		Now: now.UnixNano(),
	})
	require.NoError(t, err)
	require.NotNil(t, resp2.ApplicationError)
	require.Equal(t, mrpc.ResourceExhausted, resp2.ApplicationError.Code)

	fetched := getQueue(t, core, queue.Id)
	require.EqualValues(t, 1, fetched.Queue.SchedulesCount)
}

// Should not create a schedule for a nonexistent queue.
func TestCore_CreateScheduleForNonexistentQueue(t *testing.T) {
	core := newQueuesCore(t)

	now := time.Now()
	accountId := rand.Uint64()

	appErr := createScheduleWithError(t, core, accountId, "random_queue", "test", now)
	require.Equal(t, mrpc.NotFound, appErr.Code)
}

// Should reject creating a schedule whose (randomly generated) ID collides
// with an existing schedule's ID in the same queue, even when the name is
// different. The ID check runs before the name-uniqueness check, so this
// also proves it isn't being masked by AlreadyExists.
func TestCore_CreateScheduleIDCollision(t *testing.T) {
	core := newQueuesCore(t)

	now := time.Date(2024, 11, 9, 9, 31, 13, 0, time.UTC)
	accountId := rand.Uint64()

	createQueue(t, core, &corepb.QueueId{AccountId: accountId, QueueId: rand.Uint64()}, "test_queue", 20, now)
	first := createSchedule(t, core, accountId, "test_queue", "schedule_1", now)

	resp, err := core.CreateSchedule(&coreapis.CreateScheduleRequest{
		Payload: &corepb.CreateScheduleRequest{
			AccountId:                    accountId,
			QueueName:                    "test_queue",
			ScheduleId:                   first.Id.ScheduleId,
			ScheduleName:                 "schedule_2",
			Cron:                         "*/5 * * * *",
			Timezone:                     "UTC",
			MaxNumberOfSchedulesPerQueue: 10,
		},
		Now: now.UnixNano(),
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.NotNil(t, resp.ApplicationError)
	require.Equal(t, mrpc.IDCollision, resp.ApplicationError.Code)
}

// Should reject creating a schedule with a syntactically invalid cron
// expression.
func TestCore_CreateScheduleInvalidCron(t *testing.T) {
	core := newQueuesCore(t)

	now := time.Now()
	accountId := rand.Uint64()

	createQueue(t, core, &corepb.QueueId{AccountId: accountId, QueueId: rand.Uint64()}, "test_queue", 20, now)

	resp, err := core.CreateSchedule(&coreapis.CreateScheduleRequest{
		Payload: &corepb.CreateScheduleRequest{
			AccountId:                    accountId,
			QueueName:                    "test_queue",
			ScheduleId:                   rand.Uint64(),
			ScheduleName:                 "test_schedule",
			Cron:                         "this is not a cron expression at all!!",
			Timezone:                     "UTC",
			MaxNumberOfSchedulesPerQueue: 10,
		},
		Now: now.UnixNano(),
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.NotNil(t, resp.ApplicationError)
	require.Equal(t, mrpc.InvalidRequest, resp.ApplicationError.Code)

	// It must not have been persisted under the rejected cron expression.
	getErr := getScheduleWithError(t, core, accountId, "test_queue", "test_schedule")
	require.Equal(t, mrpc.NotFound, getErr.Code)
}

// Create a queue, then update it (change every field to a different value).
// Then retrieve this queue and check all fields are updated.
func TestCore_UpdateQueue(t *testing.T) {
	core := newQueuesCore(t)

	now := time.Now()
	accountId := rand.Uint64()

	queue := createQueue(t, core, &corepb.QueueId{AccountId: accountId, QueueId: rand.Uint64()}, "test_queue", 20, now)

	updated := updateQueue(t, core, accountId, "test_queue", now.Add(time.Second))
	require.Equal(t, "test description 2", updated.Description)
	require.Equal(t, now.UnixNano(), updated.CreatedAt)
	require.Equal(t, now.Add(time.Second).UnixNano(), updated.UpdatedAt)
	require.EqualValues(t, 30, updated.KeepaliveTimeoutInSeconds)
	require.EqualValues(t, 2, updated.Version)
	require.Equal(t, []int64{20, 30, 45}, updated.RetryStrategy.RetryIntervalsInSeconds)
	require.EqualValues(t, 120, updated.DequeuingSettings.MaxInProgressTasks)
	require.EqualValues(t, 200, updated.DequeuingSettings.RateLimiting.MaxTokens)
	require.Equal(t, true, updated.DeadLetterQueueConfig.Enable)
	require.EqualValues(t, 200, updated.DeadLetterQueueConfig.MaxSize)
	require.EqualValues(t, 86400*2, updated.DeadLetterQueueConfig.RetentionPeriodInSeconds)

	// Fetch it back to double check persistence
	fetched := getQueue(t, core, queue.Id)
	require.Equal(t, "test description 2", fetched.Queue.Description)

	// Update nonexistent queue
	appErr := updateQueueWithError(t, core, accountId, "random_queue", now.Add(3*time.Second))
	require.Equal(t, mrpc.NotFound, appErr.Code)
}

func TestCore_ListQueue(t *testing.T) {
	core := newQueuesCore(t)

	now := time.Now()
	accountId1 := rand.Uint64()
	accountId2 := rand.Uint64()

	queue1 := createQueue(t, core, &corepb.QueueId{AccountId: accountId1, QueueId: rand.Uint64()}, "test_queue_1", 20, now)
	queue2 := createQueue(t, core, &corepb.QueueId{AccountId: accountId1, QueueId: rand.Uint64()}, "test_queue_2", 20, now)
	createQueue(t, core, &corepb.QueueId{AccountId: accountId2, QueueId: rand.Uint64()}, "test_queue_1", 20, now)

	resp, err := core.ListQueues(&coreapis.ListQueuesRequest{
		Payload: &corepb.ListQueuesRequest{AccountId: accountId1},
	})
	require.NoError(t, err)
	require.Nil(t, resp.ApplicationError)
	require.Len(t, resp.Payload.Queues, 2)

	names := lo.Map(resp.Payload.Queues, func(q *corepb.Queue, _ int) string { return q.Name })
	require.ElementsMatch(t, names, []string{queue1.Name, queue2.Name})
	require.EqualValues(t, accountId1, resp.Payload.Queues[0].Id.AccountId)
	require.EqualValues(t, accountId1, resp.Payload.Queues[1].Id.AccountId)
}

func TestCore_ListSchedules(t *testing.T) {
	core := newQueuesCore(t)

	now := time.Now()
	accountId := rand.Uint64()

	queue1 := createQueue(t, core, &corepb.QueueId{AccountId: accountId, QueueId: rand.Uint64()}, "test_queue_1", 20, now)
	queue2 := createQueue(t, core, &corepb.QueueId{AccountId: accountId, QueueId: rand.Uint64()}, "test_queue_2", 20, now)

	createSchedule(t, core, accountId, "test_queue_1", "schedule_1", now)
	createSchedule(t, core, accountId, "test_queue_1", "schedule_2", now)
	createSchedule(t, core, accountId, "test_queue_1", "schedule_3", now)
	createSchedule(t, core, accountId, "test_queue_2", "other_queue_schedule", now)

	// First page
	page1 := listSchedules(t, core, queue1.Id, nil, 2)
	require.Len(t, page1.Schedules, 2)
	require.NotNil(t, page1.NextPaginationToken)

	// Second (last) page, continuing from the first page's token
	page2 := listSchedules(t, core, queue1.Id, page1.NextPaginationToken, 2)
	require.Len(t, page2.Schedules, 1)
	require.Nil(t, page2.NextPaginationToken)

	names := lo.Map(append(page1.Schedules, page2.Schedules...), func(s *corepb.Schedule, _ int) string { return s.Name })
	require.ElementsMatch(t, []string{"schedule_1", "schedule_2", "schedule_3"}, names)

	// Listing schedules for the other queue should not include queue1's schedules
	otherQueueSchedules := listSchedules(t, core, queue2.Id, nil, 0)
	require.Len(t, otherQueueSchedules.Schedules, 1)
	require.Equal(t, "other_queue_schedule", otherQueueSchedules.Schedules[0].Name)
}

func TestCore_DeleteQueue(t *testing.T) {
	core := newQueuesCore(t)

	now := time.Now()
	accountId := rand.Uint64()

	queue := createQueue(t, core, &corepb.QueueId{AccountId: accountId, QueueId: rand.Uint64()}, "test_queue", 20, now)
	createSchedule(t, core, accountId, "test_queue", "test_schedule", now)

	deleteQueue(t, core, accountId, "test_queue")

	// Get this newly deleted queue
	getErr := getQueueWithError(t, core, queue.Id)
	require.Equal(t, mrpc.NotFound, getErr.Code)

	// Schedule cleanup is asynchronous: the schedule is still there
	// immediately after DeleteQueue returns, until RunQueuesGarbageCollection
	// sweeps it.
	schedules := listSchedules(t, core, queue.Id, nil, 0)
	require.Len(t, schedules.Schedules, 1)

	// Delete nonexistent queue
	appErr := deleteQueueWithError(t, core, rand.Uint64(), "random_name")
	require.Equal(t, mrpc.NotFound, appErr.Code)
}

// Should delete schedules belonging to deleted queues in bounded batches,
// leaving schedules of queues that were not deleted untouched.
func TestCore_RunQueuesGarbageCollection(t *testing.T) {
	core := newQueuesCore(t)

	now := time.Now()
	accountId := rand.Uint64()

	queue := createQueue(t, core, &corepb.QueueId{AccountId: accountId, QueueId: rand.Uint64()}, "test_queue", 20, now)
	createSchedule(t, core, accountId, "test_queue", "schedule_1", now)
	createSchedule(t, core, accountId, "test_queue", "schedule_2", now)
	createSchedule(t, core, accountId, "test_queue", "schedule_3", now)

	otherQueue := createQueue(t, core, &corepb.QueueId{AccountId: accountId, QueueId: rand.Uint64()}, "other_queue", 20, now)
	createSchedule(t, core, accountId, "other_queue", "other_schedule", now)

	deleteQueue(t, core, accountId, "test_queue")

	// Nothing is deleted yet
	schedules := listSchedules(t, core, queue.Id, nil, 0)
	require.Len(t, schedules.Schedules, 3)

	// Run GC with a budget too small to finish the queue's schedules in one pass
	runQueuesGarbageCollection(t, core, 10, 10, 2)

	schedules = listSchedules(t, core, queue.Id, nil, 0)
	require.Len(t, schedules.Schedules, 1)

	// Finish the job
	runQueuesGarbageCollection(t, core, 10, 10, 10)

	schedules = listSchedules(t, core, queue.Id, nil, 0)
	require.Len(t, schedules.Schedules, 0)

	// The other (non-deleted) queue's schedule must be untouched
	otherSchedules := listSchedules(t, core, otherQueue.Id, nil, 0)
	require.Len(t, otherSchedules.Schedules, 1)
	require.Equal(t, "other_schedule", otherSchedules.Schedules[0].Name)

	// Running GC again with nothing pending should be a no-op
	runQueuesGarbageCollection(t, core, 10, 10, 10)
}

func TestCore_DeleteSchedule(t *testing.T) {
	core := newQueuesCore(t)

	now := time.Now()
	accountId := rand.Uint64()

	queue := createQueue(t, core, &corepb.QueueId{AccountId: accountId, QueueId: rand.Uint64()}, "test_queue", 20, now)
	createSchedule(t, core, accountId, "test_queue", "test_schedule", now)

	fetched := getQueue(t, core, queue.Id)
	require.EqualValues(t, 1, fetched.Queue.SchedulesCount)

	deleteSchedule(t, core, accountId, "test_queue", "test_schedule")

	// List schedules for this queue
	schedules := listSchedules(t, core, queue.Id, nil, 0)
	require.Len(t, schedules.Schedules, 0)

	// The queue's denormalized schedule count should reflect the deletion
	fetched = getQueue(t, core, queue.Id)
	require.EqualValues(t, 0, fetched.Queue.SchedulesCount)

	// Delete nonexistent schedule
	appErr := deleteScheduleWithError(t, core, rand.Uint64(), "random_queue", "random_schedule")
	require.Equal(t, mrpc.NotFound, appErr.Code)
}

func TestCore_CreateAndGetSchedule(t *testing.T) {
	core := newQueuesCore(t)

	now := time.Date(2024, 11, 9, 9, 31, 13, 0, time.UTC)
	accountId := rand.Uint64()

	createQueue(t, core, &corepb.QueueId{AccountId: accountId, QueueId: rand.Uint64()}, "test_queue", 20, now)
	created := createSchedule(t, core, accountId, "test_queue", "test_schedule", now)

	fetched := getSchedule(t, core, accountId, "test_queue", "test_schedule")
	require.Equal(t, created.Id.ScheduleId, fetched.Id.ScheduleId)
	require.Equal(t, "Just do it!", fetched.Description)
	require.Equal(t, now.UnixNano(), fetched.CreatedAt)
	require.Equal(t, now.UnixNano(), fetched.UpdatedAt)
	require.Equal(t, "*/5 * * * *", fetched.Cron)
	require.Equal(t, []byte("payload"), fetched.Payload)
	require.Equal(t, "dedupe", fetched.DedupeKey)
	require.EqualValues(t, 600, fetched.ExpiresInSeconds)
	require.EqualValues(t, 15, fetched.KeepaliveTimeoutInSeconds)
	require.EqualValues(t, 1, fetched.Version)
	require.Equal(t, "America/Los_Angeles", fetched.Timezone)
	require.EqualValues(t, 0, fetched.LastCheckedAt)
	require.EqualValues(t, 0, fetched.LastEnqueuedFor)
	// NextScheduledAt is the next tick of the cron expression, computed in the
	// schedule's own timezone.
	tz, err := time.LoadLocation("America/Los_Angeles")
	require.NoError(t, err)
	expectedNextScheduledAt, err := gronx.NextTickAfter("*/5 * * * *", now.In(tz), false)
	require.NoError(t, err)
	require.Equal(t, expectedNextScheduledAt.UnixNano(), fetched.NextScheduledAt)
	require.Equal(t, []int64{10, 20, 30}, fetched.RetryStrategy.RetryIntervalsInSeconds)

	// Get nonexistent schedule
	appErr := getScheduleWithError(t, core, rand.Uint64(), "random_queue", "random_schedule")
	require.Equal(t, mrpc.NotFound, appErr.Code)
}

func TestCore_UpdateSchedule(t *testing.T) {
	core := newQueuesCore(t)

	now := time.Date(2024, 11, 9, 9, 31, 13, 0, time.UTC)
	accountId := rand.Uint64()

	createQueue(t, core, &corepb.QueueId{AccountId: accountId, QueueId: rand.Uint64()}, "test_queue", 20, now)
	createSchedule(t, core, accountId, "test_queue", "test_schedule", now)

	updated := updateSchedule(t, core, accountId, "test_queue", "test_schedule", now.Add(time.Second))
	require.Equal(t, "Updated description", updated.Description)
	require.Equal(t, now.UnixNano(), updated.CreatedAt) // CreatedAt should not change
	require.Equal(t, now.Add(time.Second).UnixNano(), updated.UpdatedAt)
	require.Equal(t, "*/10 * * * *", updated.Cron)
	require.Equal(t, []byte("updated payload"), updated.Payload)
	require.Equal(t, "updated dedupe", updated.DedupeKey)
	require.EqualValues(t, 1200, updated.ExpiresInSeconds)
	require.EqualValues(t, 30, updated.KeepaliveTimeoutInSeconds)
	require.EqualValues(t, 2, updated.Version) // Version should be incremented
	require.Equal(t, "UTC", updated.Timezone)
	require.EqualValues(t, 0, updated.LastCheckedAt)   // Should not change
	require.EqualValues(t, 0, updated.LastEnqueuedFor) // Should not change
	require.Equal(t, []int64{20, 40, 60}, updated.RetryStrategy.RetryIntervalsInSeconds)
	// NextScheduledAt is the next tick of the (new) cron expression, computed in
	// the (new) timezone; the old value was computed from createSchedule's cron
	// ("*/5 * * * *") and timezone ("America/Los_Angeles").
	oldTz, err := time.LoadLocation("America/Los_Angeles")
	require.NoError(t, err)
	oldNextScheduledAtTime, err := gronx.NextTickAfter("*/5 * * * *", now.In(oldTz), false)
	require.NoError(t, err)
	oldNextScheduledAt := oldNextScheduledAtTime.UnixNano()

	newNextScheduledAtTime, err := gronx.NextTickAfter("*/10 * * * *", now.Add(time.Second).In(time.UTC), false)
	require.NoError(t, err)
	newNextScheduledAt := newNextScheduledAtTime.UnixNano()

	require.Equal(t, newNextScheduledAt, updated.NextScheduledAt)

	// The scheduled index must have been updated too: dequeuing with a
	// lookahead between the old and new NextScheduledAt should no longer
	// return this schedule (proving the stale index entry was removed), but
	// dequeuing at/after the new NextScheduledAt should.
	entries := dequeSchedules(t, core, oldNextScheduledAt+int64(500*time.Millisecond))
	require.Len(t, entries, 0)

	entries = dequeSchedules(t, core, newNextScheduledAt+1)
	require.Len(t, entries, 1)
	require.Equal(t, "test_schedule", entries[0].Schedule.Name)

	// Update nonexistent schedule
	appErr := updateScheduleWithError(t, core, rand.Uint64(), "random_queue", "random_schedule", now.Add(time.Second))
	require.Equal(t, mrpc.NotFound, appErr.Code)
}

// Should reject updating a schedule with a syntactically invalid cron
// expression, leaving the previously stored schedule untouched.
func TestCore_UpdateScheduleInvalidCron(t *testing.T) {
	core := newQueuesCore(t)

	now := time.Date(2024, 11, 9, 9, 31, 13, 0, time.UTC)
	accountId := rand.Uint64()

	createQueue(t, core, &corepb.QueueId{AccountId: accountId, QueueId: rand.Uint64()}, "test_queue", 20, now)
	created := createSchedule(t, core, accountId, "test_queue", "test_schedule", now)

	resp, err := core.UpdateSchedule(&coreapis.UpdateScheduleRequest{
		Payload: &corepb.UpdateScheduleRequest{
			AccountId:       accountId,
			QueueName:       "test_queue",
			ScheduleName:    "test_schedule",
			Cron:            "this is not a cron expression at all!!",
			Timezone:        "UTC",
			ExpectedVersion: created.Version,
		},
		Now: now.Add(time.Second).UnixNano(),
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.NotNil(t, resp.ApplicationError)
	require.Equal(t, mrpc.InvalidRequest, resp.ApplicationError.Code)

	// The previously stored schedule (cron and version) must be unchanged.
	fetched := getSchedule(t, core, accountId, "test_queue", "test_schedule")
	require.Equal(t, created.Cron, fetched.Cron)
	require.Equal(t, created.Version, fetched.Version)
}

func TestCore_ReportSchedulesStatus(t *testing.T) {
	core := newQueuesCore(t)

	now := time.Date(2024, 11, 9, 9, 31, 13, 0, time.UTC)
	accountId := rand.Uint64()

	createQueue(t, core, &corepb.QueueId{AccountId: accountId, QueueId: rand.Uint64()}, "test_queue", 20, now)
	schedule := createSchedule(t, core, accountId, "test_queue", "test_schedule", now)

	newNextScheduledAt := time.Date(2024, 11, 9, 10, 0, 0, 0, time.UTC).UnixNano()
	firstCheckedAt := time.Date(2024, 11, 9, 9, 35, 0, 0, time.UTC).UnixNano()
	firstEnqueuedFor := time.Date(2024, 11, 9, 9, 30, 0, 0, time.UTC).UnixNano()

	resp, err := core.ReportSchedulesStatus(&coreapis.ReportSchedulesStatusRequest{
		Payload: &corepb.ReportSchedulesStatusRequest{
			ScheduleId:      schedule.Id,
			NextScheduledAt: newNextScheduledAt,
			LastEnqueuedFor: firstEnqueuedFor,
		},
		Now: firstCheckedAt,
	})
	require.NoError(t, err)
	require.Nil(t, resp.ApplicationError)

	fetched := getSchedule(t, core, accountId, "test_queue", "test_schedule")
	require.Equal(t, newNextScheduledAt, fetched.NextScheduledAt)
	require.Equal(t, firstCheckedAt, fetched.LastCheckedAt)
	require.Equal(t, firstEnqueuedFor, fetched.LastEnqueuedFor)

	// The old position in the scheduled index should be gone, and dequeuing at
	// the new position should find it.
	entries := dequeSchedules(t, core, time.Date(2024, 11, 9, 9, 36, 0, 0, time.UTC).UnixNano())
	require.Len(t, entries, 0)

	entries = dequeSchedules(t, core, newNextScheduledAt+1)
	require.Len(t, entries, 1)

	// A later check that enqueues nothing (LastEnqueuedFor: 0) must still
	// advance LastCheckedAt, but must leave LastEnqueuedFor at whatever it
	// was last actually enqueued for.
	secondCheckedAt := time.Date(2024, 11, 9, 10, 5, 0, 0, time.UTC).UnixNano()

	resp, err = core.ReportSchedulesStatus(&coreapis.ReportSchedulesStatusRequest{
		Payload: &corepb.ReportSchedulesStatusRequest{
			ScheduleId:      schedule.Id,
			NextScheduledAt: newNextScheduledAt,
			LastEnqueuedFor: 0,
		},
		Now: secondCheckedAt,
	})
	require.NoError(t, err)
	require.Nil(t, resp.ApplicationError)

	fetched = getSchedule(t, core, accountId, "test_queue", "test_schedule")
	require.Equal(t, secondCheckedAt, fetched.LastCheckedAt)
	require.Equal(t, firstEnqueuedFor, fetched.LastEnqueuedFor)
}

func TestCore_SnapshotAndRestore(t *testing.T) {
	core1 := newQueuesCore(t)

	now := time.Now()
	accountId := rand.Uint64()

	queue := createQueue(t, core1, &corepb.QueueId{AccountId: accountId, QueueId: rand.Uint64()}, "test_queue", 20, now)

	snapshot := core1.Snapshot()

	updateQueue(t, core1, accountId, "test_queue", now.Add(time.Second))

	buf := bytes.NewBuffer(nil)
	require.NoError(t, snapshot.Write(buf))

	core2 := newQueuesCore(t)
	require.NoError(t, core2.Restore(io.NopCloser(buf)))

	fetched := getQueue(t, core2, queue.Id)
	require.Equal(t, "test description", fetched.Queue.Description)
}

func newQueuesCore(t *testing.T) *Core {
	t.Helper()

	badgerStore, err := store.NewBadgerInMemoryStore()
	require.NoError(t, err)

	return NewCore(badgerStore, []byte{0x1d, 0x36, 0x00, 0x00}, 0x00000000, 0xffffffff)
}

func createQueue(t *testing.T, core *Core, queueId *corepb.QueueId, name string, maxNumberOfQueues int64, now time.Time) *corepb.Queue {
	t.Helper()

	resp, err := core.CreateQueue(&coreapis.CreateQueueRequest{
		Payload: &corepb.CreateQueueRequest{
			QueueId:                   queueId,
			Name:                      name,
			Description:               "test description",
			KeepaliveTimeoutInSeconds: 15,
			RetryStrategy: &corepb.RetryStrategy{
				RetryIntervalsInSeconds: []int64{10, 20, 30},
			},
			DequeuingSettings: &corepb.DequeuingSettings{
				MaxInProgressTasks: 100,
				RateLimiting: &corepb.TokenBucketRateLimiting{
					MaxTokens:    100,
					Interval:     1,
					IntervalUnit: corepb.IntervalUnit_INTERVAL_UNIT_SECONDS,
				},
			},
			DeadLetterQueueConfig: &corepb.DeadLetterQueueConfig{
				Enable:                   true,
				MaxSize:                  100,
				RetentionPeriodInSeconds: 86400,
			},
			ExpiresInSeconds:  14 * 86400,
			MaxNumberOfQueues: maxNumberOfQueues,
		},
		Now: now.UnixNano(),
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Nil(t, resp.ApplicationError)
	require.NotNil(t, resp.Payload)
	require.NotNil(t, resp.Payload.Queue)

	return resp.Payload.Queue
}

func createQueueWithError(t *testing.T, core *Core, queueId *corepb.QueueId, name string, maxNumberOfQueues int64, now time.Time) *mrpc.Error {
	t.Helper()

	resp, err := core.CreateQueue(&coreapis.CreateQueueRequest{
		Payload: &corepb.CreateQueueRequest{
			QueueId:           queueId,
			Name:              name,
			ExpiresInSeconds:  14 * 86400,
			MaxNumberOfQueues: maxNumberOfQueues,
		},
		Now: now.UnixNano(),
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.NotNil(t, resp.ApplicationError)
	require.Nil(t, resp.Payload)

	return resp.ApplicationError
}

func getQueue(t *testing.T, core *Core, queueId *corepb.QueueId) *corepb.GetQueueResponse {
	t.Helper()

	resp, err := core.GetQueue(&coreapis.GetQueueRequest{
		Payload: &corepb.GetQueueRequest{QueueId: queueId},
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Nil(t, resp.ApplicationError)
	require.NotNil(t, resp.Payload)
	require.NotNil(t, resp.Payload.Queue)

	return resp.Payload
}

func getQueueWithError(t *testing.T, core *Core, queueId *corepb.QueueId) *mrpc.Error {
	t.Helper()

	resp, err := core.GetQueue(&coreapis.GetQueueRequest{
		Payload: &corepb.GetQueueRequest{QueueId: queueId},
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.NotNil(t, resp.ApplicationError)
	require.Nil(t, resp.Payload)

	return resp.ApplicationError
}

func updateQueue(t *testing.T, core *Core, accountId uint64, queueName string, now time.Time) *corepb.Queue {
	t.Helper()

	resp, err := core.UpdateQueue(&coreapis.UpdateQueueRequest{
		Payload: &corepb.UpdateQueueRequest{
			AccountId:                 accountId,
			QueueName:                 queueName,
			Description:               "test description 2",
			KeepaliveTimeoutInSeconds: 30,
			RetryStrategy: &corepb.RetryStrategy{
				RetryIntervalsInSeconds: []int64{20, 30, 45},
			},
			DequeuingSettings: &corepb.DequeuingSettings{
				MaxInProgressTasks: 120,
				RateLimiting: &corepb.TokenBucketRateLimiting{
					MaxTokens:    200,
					Interval:     1,
					IntervalUnit: corepb.IntervalUnit_INTERVAL_UNIT_SECONDS,
				},
			},
			DeadLetterQueueConfig: &corepb.DeadLetterQueueConfig{
				Enable:                   true,
				MaxSize:                  200,
				RetentionPeriodInSeconds: 86400 * 2,
			},
			ExpiresInSeconds: 14 * 86400,
			ExpectedVersion:  1,
		},
		Now: now.UnixNano(),
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Nil(t, resp.ApplicationError)
	require.NotNil(t, resp.Payload)
	require.NotNil(t, resp.Payload.Queue)

	return resp.Payload.Queue
}

func updateQueueWithError(t *testing.T, core *Core, accountId uint64, queueName string, now time.Time) *mrpc.Error {
	t.Helper()

	resp, err := core.UpdateQueue(&coreapis.UpdateQueueRequest{
		Payload: &corepb.UpdateQueueRequest{
			AccountId:                 accountId,
			QueueName:                 queueName,
			Description:               "test description 2",
			KeepaliveTimeoutInSeconds: 30,
		},
		Now: now.UnixNano(),
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.NotNil(t, resp.ApplicationError)
	require.Nil(t, resp.Payload)

	return resp.ApplicationError
}

func deleteQueue(t *testing.T, core *Core, accountId uint64, queueName string) {
	t.Helper()

	resp, err := core.DeleteQueue(&coreapis.DeleteQueueRequest{
		Payload: &corepb.DeleteQueueRequest{AccountId: accountId, QueueName: queueName},
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Nil(t, resp.ApplicationError)
}

func deleteQueueWithError(t *testing.T, core *Core, accountId uint64, queueName string) *mrpc.Error {
	t.Helper()

	resp, err := core.DeleteQueue(&coreapis.DeleteQueueRequest{
		Payload: &corepb.DeleteQueueRequest{AccountId: accountId, QueueName: queueName},
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.NotNil(t, resp.ApplicationError)

	return resp.ApplicationError
}

func createSchedule(t *testing.T, core *Core, accountId uint64, queueName string, scheduleName string, now time.Time) *corepb.Schedule {
	t.Helper()

	resp, err := core.CreateSchedule(&coreapis.CreateScheduleRequest{
		Payload: &corepb.CreateScheduleRequest{
			AccountId:                    accountId,
			QueueName:                    queueName,
			ScheduleId:                   rand.Uint64(),
			ScheduleName:                 scheduleName,
			Description:                  "Just do it!",
			Cron:                         "*/5 * * * *", // every 5 minutes
			Payload:                      []byte("payload"),
			DedupeKey:                    "dedupe",
			ExpiresInSeconds:             600,
			KeepaliveTimeoutInSeconds:    15,
			RetryStrategy:                &corepb.RetryStrategy{RetryIntervalsInSeconds: []int64{10, 20, 30}},
			Timezone:                     "America/Los_Angeles",
			MaxNumberOfSchedulesPerQueue: 10,
		},
		Now: now.UnixNano(),
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Nil(t, resp.ApplicationError)
	require.NotNil(t, resp.Payload)
	require.NotNil(t, resp.Payload.Schedule)

	return resp.Payload.Schedule
}

func createScheduleWithError(t *testing.T, core *Core, accountId uint64, queueName string, scheduleName string, now time.Time) *mrpc.Error {
	t.Helper()

	resp, err := core.CreateSchedule(&coreapis.CreateScheduleRequest{
		Payload: &corepb.CreateScheduleRequest{
			AccountId:                    accountId,
			QueueName:                    queueName,
			ScheduleId:                   rand.Uint64(),
			ScheduleName:                 scheduleName,
			Cron:                         "*/5 * * * *",
			Timezone:                     "America/Los_Angeles",
			MaxNumberOfSchedulesPerQueue: 10,
		},
		Now: now.UnixNano(),
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.NotNil(t, resp.ApplicationError)
	require.Nil(t, resp.Payload)

	return resp.ApplicationError
}

func getSchedule(t *testing.T, core *Core, accountId uint64, queueName string, scheduleName string) *corepb.Schedule {
	t.Helper()

	resp, err := core.GetSchedule(&coreapis.GetScheduleRequest{
		Payload: &corepb.GetScheduleRequest{AccountId: accountId, QueueName: queueName, ScheduleName: scheduleName},
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Nil(t, resp.ApplicationError)
	require.NotNil(t, resp.Payload)
	require.NotNil(t, resp.Payload.Schedule)

	return resp.Payload.Schedule
}

func getScheduleWithError(t *testing.T, core *Core, accountId uint64, queueName string, scheduleName string) *mrpc.Error {
	t.Helper()

	resp, err := core.GetSchedule(&coreapis.GetScheduleRequest{
		Payload: &corepb.GetScheduleRequest{AccountId: accountId, QueueName: queueName, ScheduleName: scheduleName},
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.NotNil(t, resp.ApplicationError)
	require.Nil(t, resp.Payload)

	return resp.ApplicationError
}

func updateSchedule(t *testing.T, core *Core, accountId uint64, queueName string, scheduleName string, now time.Time) *corepb.Schedule {
	t.Helper()

	resp, err := core.UpdateSchedule(&coreapis.UpdateScheduleRequest{
		Payload: &corepb.UpdateScheduleRequest{
			AccountId:                 accountId,
			QueueName:                 queueName,
			ScheduleName:              scheduleName,
			Description:               "Updated description",
			Cron:                      "*/10 * * * *", // every 10 minutes
			Payload:                   []byte("updated payload"),
			DedupeKey:                 "updated dedupe",
			ExpiresInSeconds:          1200,
			KeepaliveTimeoutInSeconds: 30,
			RetryStrategy:             &corepb.RetryStrategy{RetryIntervalsInSeconds: []int64{20, 40, 60}},
			Timezone:                  "UTC",
			ExpectedVersion:           1,
		},
		Now: now.UnixNano(),
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Nil(t, resp.ApplicationError)
	require.NotNil(t, resp.Payload)
	require.NotNil(t, resp.Payload.Schedule)

	return resp.Payload.Schedule
}

func updateScheduleWithError(t *testing.T, core *Core, accountId uint64, queueName string, scheduleName string, now time.Time) *mrpc.Error {
	t.Helper()

	resp, err := core.UpdateSchedule(&coreapis.UpdateScheduleRequest{
		Payload: &corepb.UpdateScheduleRequest{
			AccountId:    accountId,
			QueueName:    queueName,
			ScheduleName: scheduleName,
			Description:  "Updated description",
			Cron:         "*/10 * * * *",
			Payload:      []byte("updated payload"),
			Timezone:     "UTC",
		},
		Now: now.UnixNano(),
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.NotNil(t, resp.ApplicationError)
	require.Nil(t, resp.Payload)

	return resp.ApplicationError
}

func deleteSchedule(t *testing.T, core *Core, accountId uint64, queueName string, scheduleName string) {
	t.Helper()

	resp, err := core.DeleteSchedule(&coreapis.DeleteScheduleRequest{
		Payload: &corepb.DeleteScheduleRequest{AccountId: accountId, QueueName: queueName, ScheduleName: scheduleName},
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Nil(t, resp.ApplicationError)
}

func deleteScheduleWithError(t *testing.T, core *Core, accountId uint64, queueName string, scheduleName string) *mrpc.Error {
	t.Helper()

	resp, err := core.DeleteSchedule(&coreapis.DeleteScheduleRequest{
		Payload: &corepb.DeleteScheduleRequest{AccountId: accountId, QueueName: queueName, ScheduleName: scheduleName},
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.NotNil(t, resp.ApplicationError)

	return resp.ApplicationError
}

func listSchedules(t *testing.T, core *Core, queueId *corepb.QueueId, paginationToken *corepb.PaginationToken, limit int32) *corepb.ListSchedulesResponse {
	t.Helper()

	resp, err := core.ListSchedules(&coreapis.ListSchedulesRequest{
		Payload: &corepb.ListSchedulesRequest{
			QueueId:         queueId,
			PaginationToken: paginationToken,
			Limit:           limit,
		},
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Nil(t, resp.ApplicationError)
	require.NotNil(t, resp.Payload)

	return resp.Payload
}

func runQueuesGarbageCollection(t *testing.T, core *Core, gcRecordsPageSize int32, gcRecordSchedulesPageSize int32, maxVisitedSchedules int32) {
	t.Helper()

	resp, err := core.RunQueuesGarbageCollection(&coreapis.RunQueuesGarbageCollectionRequest{
		Payload: &corepb.RunQueuesGarbageCollectionRequest{
			GcRecordsPageSize:         gcRecordsPageSize,
			GcRecordSchedulesPageSize: gcRecordSchedulesPageSize,
			MaxVisitedSchedules:       maxVisitedSchedules,
		},
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Nil(t, resp.ApplicationError)
}

func dequeSchedules(t *testing.T, core *Core, dueBefore int64) []*corepb.DequeSchedulesResponseEntry {
	t.Helper()

	resp, err := core.DequeSchedules(&coreapis.DequeSchedulesRequest{
		Payload: &corepb.DequeSchedulesRequest{DueBefore: dueBefore},
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Nil(t, resp.ApplicationError)
	require.NotNil(t, resp.Payload)

	return resp.Payload.Entries
}
