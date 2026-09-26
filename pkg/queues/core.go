package queues

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/adhocore/gronx"
	"github.com/evrblk/monstera"
	"github.com/evrblk/monstera/cluster"
	mrpc "github.com/evrblk/monstera/rpc"
	"github.com/evrblk/monstera/store"
	"github.com/evrblk/yellowstone-common/honey"

	"github.com/evrblk/moab/pkg/coreapis"
	"github.com/evrblk/moab/pkg/corepb"
	"github.com/evrblk/moab/pkg/ids"
	"github.com/evrblk/moab/pkg/pagination"
)

const (
	maxDequeueSchedules = 250

	// Defaults applied by RunQueuesGarbageCollection when the request leaves
	// the corresponding field unset (<= 0).
	defaultGCRecordsPageSize         = 100
	defaultGCRecordSchedulesPageSize = 250
	defaultGCMaxVisitedSchedules     = 1000
)

// Core is the application core for the Queues subsystem. It owns queues,
// their schedules, and per-account counters for one shard, storing them in
// the shared BadgerDB store under its own replica prefix, and implements
// coreapis.MoabQueuesCoreApi.
type Core struct {
	badgerStore *store.BadgerStore

	replicaPrefix   []byte
	shardLowerBound cluster.ShardKey
	shardUpperBound cluster.ShardKey

	queues    *queuesTable
	counters  *countersTable
	schedules *schedulesTable
	gcRecords *gcRecordsTable
}

var _ coreapis.MoabQueuesCoreApi = &Core{}

// NewCore constructs a Core scoped to [shardLowerBound, shardUpperBound],
// namespacing all of its keys in badgerStore under replicaPrefix so that
// multiple cores can safely share the same underlying store.
func NewCore(badgerStore *store.BadgerStore, replicaPrefix []byte, shardLowerBound cluster.ShardKey, shardUpperBound cluster.ShardKey) *Core {
	return &Core{
		badgerStore: badgerStore,

		replicaPrefix:   replicaPrefix,
		shardLowerBound: shardLowerBound,
		shardUpperBound: shardUpperBound,

		queues:    newQueuesTable(replicaPrefix),
		counters:  newCountersTable(replicaPrefix),
		schedules: newSchedulesTable(replicaPrefix),
		gcRecords: newGCRecordsTable(replicaPrefix),
	}
}

func (c *Core) snapshotSections() []honey.Section {
	return []honey.Section{
		{Name: "Queues", Table: c.queues},
		{Name: "Schedules", Table: c.schedules},
		{Name: "Counters", Table: c.counters},
		{Name: "GCRecords", Table: c.gcRecords},
	}
}

// Snapshot returns a consistent, portable snapshot of this core's primary
// entities (a pinned view; Write streams from it concurrently with subsequent
// updates).
func (c *Core) Snapshot() monstera.ApplicationCoreSnapshot {
	return honey.NewSnapshot(c.badgerStore, "MoabQueues", c.snapshotSections())
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

// CreateQueue creates a new queue for the requesting account, after checking
// that the queue name is not already taken, the account has not reached its
// max-number-of-queues limit, and the (randomly generated) queue ID does not
// collide with an existing one.
func (c *Core) CreateQueue(req *coreapis.CreateQueueRequest, log *slog.Logger) (*coreapis.CreateQueueResponse, error) {
	txn := c.badgerStore.Update()
	defer txn.Discard()

	// Get counters for that account
	counters, err := c.counters.Get(txn, req.Payload.QueueId.AccountId)
	if err != nil {
		return nil, err
	}

	// Checking name uniqueness
	_, err = c.queues.GetByName(txn, req.Payload.QueueId.AccountId, req.Payload.Name)
	if err != nil {
		if !errors.Is(err, store.ErrNotFound) {
			return nil, err
		}
	} else {
		return &coreapis.CreateQueueResponse{
			ApplicationError: mrpc.NewErrorWithContext(
				mrpc.AlreadyExists,
				"queue with this name already exists",
				map[string]string{
					"queue_name": req.Payload.Name,
				}),
		}, nil
	}

	// Checking max number of queues
	if counters.NumberOfQueues >= req.Payload.MaxNumberOfQueues {
		return &coreapis.CreateQueueResponse{
			ApplicationError: mrpc.NewErrorWithContext(
				mrpc.ResourceExhausted,
				"max number of queues reached",
				map[string]string{
					"limit": fmt.Sprintf("%d", req.Payload.MaxNumberOfQueues),
				}),
		}, nil
	}

	// Checking ID uniqueness. The ID is randomly generated and passed to the core,
	// so a collision is expected to be rare; when it does happen we return IDCollision so
	// the caller can regenerate the ID and retry. This is not a user-facing error.
	// Without this check c.queues.Create would silently overwrite the colliding queue.
	_, err = c.queues.Get(txn, req.Payload.QueueId)
	if err != nil {
		if !errors.Is(err, store.ErrNotFound) {
			return nil, err
		}
	} else {
		return &coreapis.CreateQueueResponse{
			ApplicationError: mrpc.NewErrorWithContext(
				mrpc.IDCollision,
				"queue with this id already exists",
				map[string]string{
					"queue_id": ids.EncodeQueueId(req.Payload.QueueId),
				}),
		}, nil
	}

	queue := &corepb.Queue{
		Id:                        req.Payload.QueueId,
		Name:                      req.Payload.Name,
		Description:               req.Payload.Description,
		CreatedAt:                 req.Now,
		UpdatedAt:                 req.Now,
		Version:                   1,
		KeepaliveTimeoutInSeconds: req.Payload.KeepaliveTimeoutInSeconds, // TODO
		RetryStrategy:             req.Payload.RetryStrategy,
		DequeuingSettings:         req.Payload.DequeuingSettings,
		DeadLetterQueueConfig:     req.Payload.DeadLetterQueueConfig,
		ExpiresInSeconds:          req.Payload.ExpiresInSeconds,
		SchedulesCount:            0,
	}

	err = c.queues.Create(txn, queue)
	if err != nil {
		return nil, err
	}

	// Update counters
	counters.NumberOfQueues = counters.NumberOfQueues + 1
	err = c.counters.Set(txn, req.Payload.QueueId.AccountId, counters)
	if err != nil {
		return nil, err
	}

	err = txn.Commit()
	if err != nil {
		return nil, err
	}

	return &coreapis.CreateQueueResponse{
		Payload: &corepb.CreateQueueResponse{
			Queue: queue,
		},
	}, nil
}

// ListQueues returns a page of queues for the requesting account, ordered by
// queue ID, continuing from req.Payload.PaginationToken if provided.
func (c *Core) ListQueues(req *coreapis.ListQueuesRequest, log *slog.Logger) (*coreapis.ListQueuesResponse, error) {
	txn := c.badgerStore.View()
	defer txn.Discard()

	result, err := c.queues.List(txn, req.Payload.AccountId, req.Payload.PaginationToken, pagination.GetLimitWithDefaults(int(req.Payload.Limit)))
	if err != nil {
		return nil, err
	}

	return &coreapis.ListQueuesResponse{
		Payload: &corepb.ListQueuesResponse{
			Queues:                  result.Queues,
			NextPaginationToken:     result.NextPaginationToken,
			PreviousPaginationToken: result.PreviousPaginationToken,
		},
	}, nil
}

// GetQueue returns a queue by ID. It returns a NotFound application error if
// no queue with that ID exists.
func (c *Core) GetQueue(req *coreapis.GetQueueRequest, log *slog.Logger) (*coreapis.GetQueueResponse, error) {
	txn := c.badgerStore.View()
	defer txn.Discard()

	queue, err := c.queues.Get(txn, req.Payload.QueueId)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return &coreapis.GetQueueResponse{
				ApplicationError: mrpc.NewErrorWithContext(
					mrpc.NotFound,
					"queue not found",
					map[string]string{
						"queue_id": ids.EncodeQueueId(req.Payload.QueueId),
					}),
			}, nil
		}

		return nil, err
	}

	return &coreapis.GetQueueResponse{
		Payload: &corepb.GetQueueResponse{
			Queue: queue,
		},
	}, nil
}

// GetQueueByName returns a queue by account ID and queue name. It returns a
// NotFound application error if no queue with that name exists for the
// account.
func (c *Core) GetQueueByName(req *coreapis.GetQueueByNameRequest, log *slog.Logger) (*coreapis.GetQueueByNameResponse, error) {
	txn := c.badgerStore.View()
	defer txn.Discard()

	queue, err := c.queues.GetByName(txn, req.Payload.AccountId, req.Payload.QueueName)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return &coreapis.GetQueueByNameResponse{
				ApplicationError: queueNotFoundError(req.Payload.QueueName),
			}, nil
		}

		return nil, err
	}

	return &coreapis.GetQueueByNameResponse{
		Payload: &corepb.GetQueueByNameResponse{
			Queue: queue,
		},
	}, nil
}

// UpdateQueue overwrites a queue's mutable settings (description, keepalive
// timeout, retry strategy, dequeuing settings, dead letter queue config, and
// expiration) and bumps its version. The queue is looked up by account ID
// and name; the name itself cannot be changed. Fields omitted from the
// request are cleared, matching CreateQueue's field-by-field replacement
// semantics rather than a partial patch.
func (c *Core) UpdateQueue(req *coreapis.UpdateQueueRequest, log *slog.Logger) (*coreapis.UpdateQueueResponse, error) {
	txn := c.badgerStore.Update()
	defer txn.Discard()

	queue, err := c.queues.GetByName(txn, req.Payload.AccountId, req.Payload.QueueName)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return &coreapis.UpdateQueueResponse{
				ApplicationError: queueNotFoundError(req.Payload.QueueName),
			}, nil
		}

		return nil, err
	}

	if queue.Version != req.Payload.ExpectedVersion {
		return &coreapis.UpdateQueueResponse{
			ApplicationError: mrpc.NewErrorWithContext(
				mrpc.InvalidRequest,
				"version mismatch",
				map[string]string{
					"queue_name":       req.Payload.QueueName,
					"actual_version":   fmt.Sprintf("%d", queue.Version),
					"expected_version": fmt.Sprintf("%d", req.Payload.ExpectedVersion),
				},
			),
		}, nil
	}

	queue.Description = req.Payload.Description
	queue.UpdatedAt = req.Now
	queue.KeepaliveTimeoutInSeconds = req.Payload.KeepaliveTimeoutInSeconds
	queue.RetryStrategy = req.Payload.RetryStrategy
	queue.DequeuingSettings = req.Payload.DequeuingSettings
	queue.DeadLetterQueueConfig = req.Payload.DeadLetterQueueConfig
	queue.ExpiresInSeconds = req.Payload.ExpiresInSeconds
	queue.Version = queue.Version + 1

	err = c.queues.Update(txn, queue)
	if err != nil {
		return nil, err
	}

	err = txn.Commit()
	if err != nil {
		return nil, err
	}

	return &coreapis.UpdateQueueResponse{
		Payload: &corepb.UpdateQueueResponse{
			Queue: queue,
		},
	}, nil
}

// DeleteQueue deletes a queue found by account ID and name, along with all
// of its schedules, and decrements the account's queue counter. The response
// carries the deleted queue's id so the caller (the server handler) can tell
// TasksCore to asynchronously drain its tasks too, via the exact same
// PurgeQueue marker SwapQueueId's rotation uses.
func (c *Core) DeleteQueue(req *coreapis.DeleteQueueRequest, log *slog.Logger) (*coreapis.DeleteQueueResponse, error) {
	txn := c.badgerStore.Update()
	defer txn.Discard()

	queue, err := c.queues.GetByName(txn, req.Payload.AccountId, req.Payload.QueueName)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return &coreapis.DeleteQueueResponse{
				ApplicationError: queueNotFoundError(req.Payload.QueueName),
			}, nil
		}

		return nil, err
	}

	// Get counters for that account
	counters, err := c.counters.Get(txn, queue.Id.AccountId)
	if err != nil {
		return nil, err
	}

	err = c.queues.Delete(txn, queue)
	if err != nil {
		return nil, err
	}

	// Update counters
	counters.NumberOfQueues = counters.NumberOfQueues - 1
	err = c.counters.Set(txn, queue.Id.AccountId, counters)
	if err != nil {
		return nil, err
	}

	// Mark the queue's schedules for asynchronous deletion instead of deleting
	// them here, so this call stays O(1) regardless of how many schedules the
	// queue has. RunQueuesGarbageCollection drains them in bounded batches.
	err = c.gcRecords.Create(txn, &corepb.QueuesGarbageCollectionRecord{
		QueueId: queue.Id,
	})
	if err != nil {
		return nil, err
	}

	err = txn.Commit()
	if err != nil {
		return nil, err
	}

	return &coreapis.DeleteQueueResponse{
		Payload: &corepb.DeleteQueueResponse{
			QueueId: queue.Id,
		},
	}, nil
}

// SwapQueueId rotates a queue found by account ID and name onto a freshly
// generated id, preserving its name, settings, and attached schedules —
// everything else about the queue is unchanged, only req.Payload.NewQueueId
// replaces its identity. This is PurgeQueue's first step: after this call
// returns, every new Enqueue for this name lands under the new id, and the
// caller is expected to hand the returned OldQueueId to TasksCore.PurgeQueue
// so the tasks left behind under it get drained asynchronously — without
// this rotation, an unbounded purge would have to delete every task
// synchronously to avoid colliding with tasks enqueued during the purge.
//
// Schedules are re-keyed onto the new id synchronously, in this same
// transaction, rather than left for async cleanup like tasks: unlike tasks,
// a queue's schedule count is bounded (MaxNumberOfSchedulesPerQueue), so
// this stays cheap, and DequeSchedules resolves a schedule's queue by this
// same id — leaving them under the old id would make them not just
// unreachable via ListSchedules, but actively break DequeSchedules the next
// time one of them comes due (a NotFound there is treated as an index
// corruption, not a skip) once TasksCore's GC starts draining the old id.
func (c *Core) SwapQueueId(req *coreapis.SwapQueueIdRequest, log *slog.Logger) (*coreapis.SwapQueueIdResponse, error) {
	txn := c.badgerStore.Update()
	defer txn.Discard()

	queue, err := c.queues.GetByName(txn, req.Payload.AccountId, req.Payload.QueueName)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return &coreapis.SwapQueueIdResponse{
				ApplicationError: queueNotFoundError(req.Payload.QueueName),
			}, nil
		}

		return nil, err
	}

	oldQueueId := queue.Id
	newQueueId := &corepb.QueueId{
		AccountId: req.Payload.AccountId,
		QueueId:   req.Payload.NewQueueId,
	}

	// Checking ID uniqueness, same convention as CreateQueue: the id is
	// randomly generated by the caller, so a collision is expected to be
	// rare; when it happens the caller regenerates and retries.
	_, err = c.queues.Get(txn, newQueueId)
	if err != nil {
		if !errors.Is(err, store.ErrNotFound) {
			return nil, err
		}
	} else {
		return &coreapis.SwapQueueIdResponse{
			ApplicationError: mrpc.NewErrorWithContext(
				mrpc.IDCollision,
				"queue with this id already exists",
				map[string]string{
					"queue_id": ids.EncodeQueueId(newQueueId),
				}),
		}, nil
	}

	newQueue := &corepb.Queue{
		Id:                        newQueueId,
		Name:                      queue.Name,
		Description:               queue.Description,
		CreatedAt:                 queue.CreatedAt,
		UpdatedAt:                 req.Now,
		Version:                   queue.Version + 1,
		KeepaliveTimeoutInSeconds: queue.KeepaliveTimeoutInSeconds,
		RetryStrategy:             queue.RetryStrategy,
		DequeuingSettings:         queue.DequeuingSettings,
		DeadLetterQueueConfig:     queue.DeadLetterQueueConfig,
		ExpiresInSeconds:          queue.ExpiresInSeconds,
		SchedulesCount:            queue.SchedulesCount,
	}

	var paginationToken *corepb.PaginationToken
	for {
		result, err := c.schedules.List(txn, oldQueueId, paginationToken, defaultGCRecordSchedulesPageSize)
		if err != nil {
			return nil, err
		}

		for _, schedule := range result.Schedules {
			if err := c.schedules.Delete(txn, schedule); err != nil {
				return nil, err
			}

			schedule.Id = &corepb.ScheduleId{
				AccountId:  newQueueId.AccountId,
				QueueId:    newQueueId.QueueId,
				ScheduleId: schedule.Id.ScheduleId,
			}
			if err := c.schedules.Create(txn, schedule); err != nil {
				return nil, err
			}
		}

		if result.NextPaginationToken == nil {
			break
		}
		paginationToken = result.NextPaginationToken
	}

	if err := c.queues.SwapId(txn, oldQueueId, newQueue); err != nil {
		return nil, err
	}

	if err := txn.Commit(); err != nil {
		return nil, err
	}

	return &coreapis.SwapQueueIdResponse{
		Payload: &corepb.SwapQueueIdResponse{
			Queue:      newQueue,
			OldQueueId: oldQueueId,
		},
	}, nil
}

// CreateSchedule creates a new cron schedule attached to an existing queue,
// after checking that the (randomly generated) schedule ID does not collide
// with an existing one, enforcing the account's max-schedules-per-queue
// limit, and checking that the schedule name is not already taken within
// the queue.NextScheduledAt is computed from the cron
// expression and timezone relative to req.Now.
func (c *Core) CreateSchedule(req *coreapis.CreateScheduleRequest, log *slog.Logger) (*coreapis.CreateScheduleResponse, error) {
	txn := c.badgerStore.Update()
	defer txn.Discard()

	queue, err := c.queues.GetByName(txn, req.Payload.AccountId, req.Payload.QueueName)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return &coreapis.CreateScheduleResponse{
				ApplicationError: queueNotFoundError(req.Payload.QueueName),
			}, nil
		}

		return nil, err
	}

	scheduleId := &corepb.ScheduleId{
		AccountId:  req.Payload.AccountId,
		QueueId:    queue.Id.QueueId,
		ScheduleId: req.Payload.ScheduleId,
	}

	// Checking ID uniqueness. The ID is randomly generated and passed to the core,
	// so a collision is expected to be rare; when it does happen we return IDCollision so
	// the caller can regenerate the ID and retry. This is not a user-facing error.
	// Without this check c.schedules.Create would silently overwrite the colliding schedule.
	_, err = c.schedules.Get(txn, scheduleId)
	if err != nil {
		if !errors.Is(err, store.ErrNotFound) {
			return nil, err
		}
	} else {
		return &coreapis.CreateScheduleResponse{
			ApplicationError: mrpc.NewErrorWithContext(
				mrpc.IDCollision,
				"schedule with this id already exists",
				map[string]string{
					"schedule_id": ids.EncodeScheduleId(scheduleId),
				}),
		}, nil
	}

	// Checking max number of schedules per queue.
	if queue.SchedulesCount >= req.Payload.MaxNumberOfSchedulesPerQueue {
		return &coreapis.CreateScheduleResponse{
			ApplicationError: mrpc.NewErrorWithContext(
				mrpc.ResourceExhausted,
				"max number of schedules per queue reached",
				map[string]string{
					"limit": fmt.Sprintf("%d", req.Payload.MaxNumberOfSchedulesPerQueue),
				}),
		}, nil
	}

	// Checking name uniqueness
	_, err = c.schedules.GetByName(txn, queue.Id, req.Payload.ScheduleName)
	if err != nil {
		if !errors.Is(err, store.ErrNotFound) {
			return nil, err
		}
	} else {
		return &coreapis.CreateScheduleResponse{
			ApplicationError: mrpc.NewErrorWithContext(
				mrpc.AlreadyExists,
				"schedule with this name already exists",
				map[string]string{
					"schedule_name": req.Payload.ScheduleName,
					"queue_name":    queue.Name,
				}),
		}, nil
	}

	// Cron expression and timezone are validated by CreateScheduleRequest.Validate.
	tz, err := time.LoadLocation(req.Payload.Timezone)
	if err != nil {
		return nil, err
	}

	now := time.Unix(0, req.Now).In(tz)
	nextTime, err := gronx.NextTickAfter(req.Payload.Cron, now, false)
	if err != nil {
		return nil, err
	}

	schedule := &corepb.Schedule{
		Id:               scheduleId,
		Name:             req.Payload.ScheduleName,
		Description:      req.Payload.Description,
		CreatedAt:        req.Now,
		UpdatedAt:        req.Now,
		Version:          1,
		Cron:             req.Payload.Cron,
		Payload:          req.Payload.Payload,
		DedupeKey:        req.Payload.DedupeKey,
		ExpiresInSeconds: req.Payload.ExpiresInSeconds,
		RetryStrategy:    req.Payload.RetryStrategy,
		Timezone:         req.Payload.Timezone,
		LastCheckedAt:    0,
		NextScheduledAt:  nextTime.UnixNano(),
		LastEnqueuedFor:  0,
	}

	err = c.schedules.Create(txn, schedule)
	if err != nil {
		return nil, err
	}

	// Update the schedule count on the queue
	queue.SchedulesCount = queue.SchedulesCount + 1
	err = c.queues.Update(txn, queue)
	if err != nil {
		return nil, err
	}

	err = txn.Commit()
	if err != nil {
		return nil, err
	}

	return &coreapis.CreateScheduleResponse{
		Payload: &corepb.CreateScheduleResponse{
			Schedule: schedule,
		},
	}, nil
}

// GetSchedule returns a schedule by queue name and schedule name. It returns
// a NotFound application error if the queue or the schedule does not exist.
func (c *Core) GetSchedule(req *coreapis.GetScheduleRequest, log *slog.Logger) (*coreapis.GetScheduleResponse, error) {
	txn := c.badgerStore.View()
	defer txn.Discard()

	queue, err := c.queues.GetByName(txn, req.Payload.AccountId, req.Payload.QueueName)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return &coreapis.GetScheduleResponse{
				ApplicationError: queueNotFoundError(req.Payload.QueueName),
			}, nil
		}

		return nil, err
	}

	schedule, err := c.schedules.GetByName(txn, queue.Id, req.Payload.ScheduleName)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return &coreapis.GetScheduleResponse{
				ApplicationError: scheduleNotFoundError(queue.Name, req.Payload.ScheduleName),
			}, nil
		} else {
			return nil, err
		}
	}

	return &coreapis.GetScheduleResponse{
		Payload: &corepb.GetScheduleResponse{
			Schedule: schedule,
		},
	}, nil
}

// ListSchedules returns a page of schedules belonging to the given queue,
// ordered by schedule ID, continuing from req.Payload.PaginationToken if
// provided.
func (c *Core) ListSchedules(req *coreapis.ListSchedulesRequest, log *slog.Logger) (*coreapis.ListSchedulesResponse, error) {
	txn := c.badgerStore.View()
	defer txn.Discard()

	result, err := c.schedules.List(txn, req.Payload.QueueId, req.Payload.PaginationToken, pagination.GetLimitWithDefaults(int(req.Payload.Limit)))
	if err != nil {
		return nil, err
	}

	return &coreapis.ListSchedulesResponse{
		Payload: &corepb.ListSchedulesResponse{
			Schedules:               result.Schedules,
			NextPaginationToken:     result.NextPaginationToken,
			PreviousPaginationToken: result.PreviousPaginationToken,
		},
	}, nil
}

// UpdateSchedule overwrites a schedule's mutable settings, recomputes
// NextScheduledAt relative to req.Now, and bumps its version. The schedule
// is looked up by queue name and schedule name; the name itself cannot be
// changed.
func (c *Core) UpdateSchedule(req *coreapis.UpdateScheduleRequest, log *slog.Logger) (*coreapis.UpdateScheduleResponse, error) {
	txn := c.badgerStore.Update()
	defer txn.Discard()

	queue, err := c.queues.GetByName(txn, req.Payload.AccountId, req.Payload.QueueName)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return &coreapis.UpdateScheduleResponse{
				ApplicationError: queueNotFoundError(req.Payload.QueueName),
			}, nil
		}

		return nil, err
	}

	schedule, err := c.schedules.GetByName(txn, queue.Id, req.Payload.ScheduleName)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return &coreapis.UpdateScheduleResponse{
				ApplicationError: scheduleNotFoundError(queue.Name, req.Payload.ScheduleName),
			}, nil
		} else {
			return nil, err
		}
	}

	if schedule.Version != req.Payload.ExpectedVersion {
		return &coreapis.UpdateScheduleResponse{
			ApplicationError: mrpc.NewErrorWithContext(
				mrpc.InvalidRequest,
				"version mismatch",
				map[string]string{
					"queue_name":       req.Payload.QueueName,
					"schedule_name":    req.Payload.ScheduleName,
					"actual_version":   fmt.Sprintf("%d", schedule.Version),
					"expected_version": fmt.Sprintf("%d", req.Payload.ExpectedVersion),
				},
			),
		}, nil
	}

	schedule.Description = req.Payload.Description
	schedule.UpdatedAt = req.Now
	schedule.Payload = req.Payload.Payload
	schedule.DedupeKey = req.Payload.DedupeKey
	schedule.Cron = req.Payload.Cron
	schedule.RetryStrategy = req.Payload.RetryStrategy
	schedule.ExpiresInSeconds = req.Payload.ExpiresInSeconds
	schedule.Timezone = req.Payload.Timezone
	schedule.Version = schedule.Version + 1

	// Cron expression and timezone are validated by UpdateScheduleRequest.Validate.
	tz, err := time.LoadLocation(req.Payload.Timezone)
	if err != nil {
		return nil, err
	}

	// Calculating next tick from now based on a given cron schedule
	now := time.Unix(0, req.Now).In(tz)
	nextTime, err := gronx.NextTickAfter(req.Payload.Cron, now, false)
	if err != nil {
		return nil, err
	}

	// Update NextScheduledAt
	schedule.NextScheduledAt = nextTime.UnixNano()

	// Update the schedule
	err = c.schedules.Update(txn, schedule)
	if err != nil {
		return nil, err
	}

	err = txn.Commit()
	if err != nil {
		return nil, err
	}

	return &coreapis.UpdateScheduleResponse{
		Payload: &corepb.UpdateScheduleResponse{
			Schedule: schedule,
		},
	}, nil
}

// DeleteSchedule deletes a schedule found by queue name and schedule name.
func (c *Core) DeleteSchedule(req *coreapis.DeleteScheduleRequest, log *slog.Logger) (*coreapis.DeleteScheduleResponse, error) {
	txn := c.badgerStore.Update()
	defer txn.Discard()

	queue, err := c.queues.GetByName(txn, req.Payload.AccountId, req.Payload.QueueName)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return &coreapis.DeleteScheduleResponse{
				ApplicationError: queueNotFoundError(req.Payload.QueueName),
			}, nil
		}

		return nil, err
	}

	schedule, err := c.schedules.GetByName(txn, queue.Id, req.Payload.ScheduleName)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return &coreapis.DeleteScheduleResponse{
				ApplicationError: scheduleNotFoundError(queue.Name, req.Payload.ScheduleName),
			}, nil
		} else {
			return nil, err
		}
	}

	err = c.schedules.Delete(txn, schedule)
	if err != nil {
		return nil, err
	}

	// Update the denormalized schedule count on the queue
	queue.SchedulesCount = queue.SchedulesCount - 1
	err = c.queues.Update(txn, queue)
	if err != nil {
		return nil, err
	}

	err = txn.Commit()
	if err != nil {
		return nil, err
	}

	return &coreapis.DeleteScheduleResponse{
		Payload: &corepb.DeleteScheduleResponse{},
	}, nil
}

// DequeSchedules returns the schedules that are due at or before
// req.Payload.LookaheadTime (up to maxDequeueSchedules, ordered by
// NextScheduledAt), each paired with its owning queue, so a caller can
// enqueue tasks for schedules that have come due.
func (c *Core) DequeSchedules(req *coreapis.DequeSchedulesRequest, log *slog.Logger) (*coreapis.DequeSchedulesResponse, error) {
	txn := c.badgerStore.View()
	defer txn.Discard()

	schedules, err := c.schedules.ListDue(txn, req.Payload.DueBefore, maxDequeueSchedules)
	if err != nil {
		return nil, err
	}

	entries := make([]*corepb.DequeSchedulesResponseEntry, len(schedules))
	for i, schedule := range schedules {
		queueId := &corepb.QueueId{
			AccountId: schedule.Id.AccountId,
			QueueId:   schedule.Id.QueueId,
		}
		queue, err := c.queues.Get(txn, queueId)
		if err != nil {
			// Return any error, including NotFound (meaning the index is corrupted)
			return nil, err
		}
		entries[i] = &corepb.DequeSchedulesResponseEntry{
			Queue:    queue,
			Schedule: schedule,
		}
	}

	return &coreapis.DequeSchedulesResponse{
		Payload: &corepb.DequeSchedulesResponse{
			Entries: entries,
		},
	}, nil
}

// ReportSchedulesStatus advances a schedule's NextScheduledAt to
// req.Payload.NextScheduledAt once its due tick has been processed (e.g. a
// task enqueued for it). It also always records LastCheckedAt as req.Now
// (regardless of whether anything was enqueued) and, only if the worker
// actually enqueued something this tick (req.Payload.LastEnqueuedFor != 0),
// advances LastEnqueuedFor to it. Every other field on the schedule is left
// untouched.
func (c *Core) ReportSchedulesStatus(req *coreapis.ReportSchedulesStatusRequest, log *slog.Logger) (*coreapis.ReportSchedulesStatusResponse, error) {
	txn := c.badgerStore.Update()
	defer txn.Discard()

	schedule, err := c.schedules.Get(txn, req.Payload.ScheduleId)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return &coreapis.ReportSchedulesStatusResponse{
				ApplicationError: mrpc.NewErrorWithContext(
					mrpc.NotFound,
					"schedule not found",
					map[string]string{
						"schedule_id": fmt.Sprintf("%d", req.Payload.ScheduleId.ScheduleId),
					}),
			}, nil
		} else {
			return nil, err
		}
	}

	// Update NextScheduledAt
	schedule.NextScheduledAt = req.Payload.NextScheduledAt

	// LastCheckedAt is recorded on every call, whether or not anything was
	// enqueued: it's a diagnostic signal for a worker that stopped checking
	// this schedule at all. req.Now is the RPC-layer timestamp (Monstera's
	// request wrapper), not a separate field — it's already the moment the
	// worker made this call.
	schedule.LastCheckedAt = req.Now

	// LastEnqueuedFor only advances when the worker actually enqueued
	// something this tick (0 means "nothing enqueued this tick"); the
	// previous value is left in place so it keeps reflecting the last tick
	// for which something really was scheduled.
	if req.Payload.LastEnqueuedFor != 0 {
		schedule.LastEnqueuedFor = req.Payload.LastEnqueuedFor
	}

	// Update the schedule
	err = c.schedules.Update(txn, schedule)
	if err != nil {
		return nil, err
	}

	err = txn.Commit()
	if err != nil {
		return nil, err
	}

	return &coreapis.ReportSchedulesStatusResponse{
		Payload: &corepb.ReportSchedulesStatusResponse{},
	}, nil
}

// RunQueuesGarbageCollection processes one page of pending GC work: for each
// deleted queue recorded by DeleteQueue, deletes a page of its remaining
// schedules, dropping the GC record once none are left. The amount of work
// per call is bounded by req.Payload.MaxVisitedSchedules; records that don't
// fully drain within budget are left for the next GC tick.
func (c *Core) RunQueuesGarbageCollection(req *coreapis.RunQueuesGarbageCollectionRequest, log *slog.Logger) (*coreapis.RunQueuesGarbageCollectionResponse, error) {
	txn := c.badgerStore.Update()
	defer txn.Discard()

	gcRecordsPageSize := int(req.Payload.GcRecordsPageSize)
	if gcRecordsPageSize <= 0 {
		gcRecordsPageSize = defaultGCRecordsPageSize
	}
	gcRecordSchedulesPageSize := int(req.Payload.GcRecordSchedulesPageSize)
	if gcRecordSchedulesPageSize <= 0 {
		gcRecordSchedulesPageSize = defaultGCRecordSchedulesPageSize
	}
	maxVisitedSchedules := int(req.Payload.MaxVisitedSchedules)
	if maxVisitedSchedules <= 0 {
		maxVisitedSchedules = defaultGCMaxVisitedSchedules
	}

	gcRecords, err := c.gcRecords.List(txn, gcRecordsPageSize)
	if err != nil {
		return nil, err
	}

	visitedSchedules := 0
recordsLoop:
	for _, gcRecord := range gcRecords {
		result, err := c.schedules.List(txn, gcRecord.QueueId, nil, gcRecordSchedulesPageSize)
		if err != nil {
			return nil, err
		}

		for _, schedule := range result.Schedules {
			err = c.schedules.Delete(txn, schedule)
			if err != nil {
				return nil, err
			}

			visitedSchedules++
			if visitedSchedules >= maxVisitedSchedules {
				break recordsLoop
			}
		}

		// No schedules left for this queue: the GC record has fully drained.
		if result.NextPaginationToken == nil {
			err = c.gcRecords.Delete(txn, gcRecord)
			if err != nil {
				return nil, err
			}
		}
	}

	err = txn.Commit()
	if err != nil {
		return nil, err
	}

	return &coreapis.RunQueuesGarbageCollectionResponse{
		Payload: &corepb.RunQueuesGarbageCollectionResponse{},
	}, nil
}

func queueNotFoundError(queueName string) *mrpc.Error {
	return mrpc.NewErrorWithContext(
		mrpc.NotFound,
		"queue not found",
		map[string]string{
			"queue_name": queueName,
		},
	)
}

func scheduleNotFoundError(queueName string, scheduleName string) *mrpc.Error {
	return mrpc.NewErrorWithContext(
		mrpc.NotFound,
		"schedule not found",
		map[string]string{
			"schedule_name": scheduleName,
			"queue_name":    queueName,
		},
	)
}
