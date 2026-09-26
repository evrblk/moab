package v0

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	mrpc "github.com/evrblk/monstera/rpc"
	"github.com/evrblk/yellowstone-common/cache"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	moabpb "github.com/evrblk/evrblk-go/moab/v0"
	"github.com/evrblk/moab/pkg/coreapis"
	"github.com/evrblk/moab/pkg/corepb"
	"github.com/evrblk/moab/pkg/ids"
	"github.com/evrblk/moab/pkg/moab"
)

const (
	// maxIDGenerationAttempts bounds how many times a handler regenerates a random
	// entity ID and retries when the core reports an ID collision.
	maxIDGenerationAttempts = 5

	queuesCacheTTL         = 1 * time.Second
	queuesCacheNegativeTTL = 1 * time.Second
)

type MoabApiServerHandler struct {
	moabClient  coreapis.MoabClientApi
	queuesCache *cache.Cache[string, *corepb.Queue]
}

func (s *MoabApiServerHandler) Stop() {
}

func (s *MoabApiServerHandler) CreateQueue(ctx context.Context, req *moabpb.CreateQueueRequest, accountId uint64, limits moab.ServiceLimits) (*moabpb.CreateQueueResponse, error) {
	// The queue ID is randomly generated here. On the rare ID collision
	// the core returns IDCollision; we regenerate the ID and retry.
	for range maxIDGenerationAttempts {
		resp1, err := s.moabClient.CreateQueue(ctx, &corepb.CreateQueueRequest{
			QueueId: &corepb.QueueId{
				AccountId: accountId,
				QueueId:   rand.Uint64(),
			},
			Name:                      req.Name,
			Description:               req.Description,
			KeepaliveTimeoutInSeconds: req.KeepaliveTimeoutInSeconds,
			RetryStrategy:             retryStrategyToCore(req.RetryStrategy),
			DequeuingSettings:         dequeuingSettingsToCore(req.DequeuingSettings),
			DeadLetterQueueConfig:     deadLetterQueueConfigToCore(req.DeadLetterQueueConfig),
			ExpiresInSeconds:          req.ExpiresInSeconds,
			MaxNumberOfQueues:         limits.MaxNumberOfQueues,
		})
		if err != nil {
			if isIDCollision(err) {
				continue
			}
			return nil, mrpc.ErrorToGRPC(err)
		}

		return &moabpb.CreateQueueResponse{
			Queue: queueToFront(resp1.Queue),
		}, nil
	}

	return nil, status.Error(codes.Internal, "failed to generate a unique id")
}

func (s *MoabApiServerHandler) GetQueue(ctx context.Context, req *moabpb.GetQueueRequest, accountId uint64, limits moab.ServiceLimits) (*moabpb.GetQueueResponse, error) {
	resp1, err := s.moabClient.GetQueueByName(ctx, &corepb.GetQueueByNameRequest{
		AccountId: accountId,
		QueueName: req.QueueName,
	})
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	var meta mrpc.ResponseMeta
	resp2, err := s.moabClient.GetStatistics(ctx, &corepb.GetStatisticsRequest{
		QueueId: resp1.Queue.Id,
	}, mrpc.WithResponseMeta(&meta))
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	return &moabpb.GetQueueResponse{
		Queue: queueToFront(resp1.Queue),
		Stats: queueStatsToFront(resp2),
		Now:   meta.Now,
	}, nil
}

func (s *MoabApiServerHandler) UpdateQueue(ctx context.Context, req *moabpb.UpdateQueueRequest, accountId uint64, limits moab.ServiceLimits) (*moabpb.UpdateQueueResponse, error) {
	resp1, err := s.moabClient.UpdateQueue(ctx, &corepb.UpdateQueueRequest{
		AccountId:                 accountId,
		QueueName:                 req.QueueName,
		Description:               req.Description,
		KeepaliveTimeoutInSeconds: req.KeepaliveTimeoutInSeconds,
		ExpiresInSeconds:          req.ExpiresInSeconds,
		RetryStrategy:             retryStrategyToCore(req.RetryStrategy),
		DequeuingSettings:         dequeuingSettingsToCore(req.DequeuingSettings),
		DeadLetterQueueConfig:     deadLetterQueueConfigToCore(req.DeadLetterQueueConfig),
		ExpectedVersion:           req.ExpectedVersion,
	})
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	return &moabpb.UpdateQueueResponse{
		Queue: queueToFront(resp1.Queue),
	}, nil
}

// DeleteQueue removes a queue and, like PurgeQueue, marks its tasks for
// asynchronous cleanup instead of leaving them to rot forever unreachable:
// the queue's id is retired for good (never reused, unlike PurgeQueue's
// rotation), so TasksCore.PurgeQueue's marker fits it exactly. This also
// closes the same cross-gateway cache gap PurgeQueue has: another gateway
// process's queuesCache can still hand out this deleted queue's stale id for
// up to queuesCacheTTL, so without the marker, a request landing there would
// silently write into (or read from) a queue_id nothing will ever look at
// again, rather than getting the NotFound that lets the handler retry (or,
// here, correctly fail once the retry's fresh lookup finds nothing at all).
func (s *MoabApiServerHandler) DeleteQueue(ctx context.Context, req *moabpb.DeleteQueueRequest, accountId uint64, limits moab.ServiceLimits) (*moabpb.DeleteQueueResponse, error) {
	resp1, err := s.moabClient.DeleteQueue(ctx, &corepb.DeleteQueueRequest{
		AccountId: accountId,
		QueueName: req.QueueName,
	})
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	// The queue is gone under this id for good; this process's own cached
	// entry must not survive it (same reasoning as PurgeQueue's).
	s.queuesCache.Delete(fmt.Sprintf("%d/%s", accountId, req.QueueName))

	_, err = s.moabClient.PurgeQueue(ctx, &corepb.PurgeQueueRequest{
		QueueId: resp1.QueueId,
	})
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	return &moabpb.DeleteQueueResponse{}, nil
}

func (s *MoabApiServerHandler) ListQueues(ctx context.Context, req *moabpb.ListQueuesRequest, accountId uint64, limits moab.ServiceLimits) (*moabpb.ListQueuesResponse, error) {
	// Decode pagination token from base64-encoded format
	paginationToken, err := paginationTokenToCore(req.PaginationToken)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	resp1, err := s.moabClient.ListQueues(ctx, &corepb.ListQueuesRequest{
		AccountId:       accountId,
		PaginationToken: paginationToken,
		Limit:           req.Limit,
	})
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	// Encode pagination tokens for response
	nextPaginationToken, err := paginationTokenToFront(resp1.NextPaginationToken)
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}
	previousPaginationToken, err := paginationTokenToFront(resp1.PreviousPaginationToken)
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	return &moabpb.ListQueuesResponse{
		Queues:                  queuesToFront(resp1.Queues),
		NextPaginationToken:     nextPaginationToken,
		PreviousPaginationToken: previousPaginationToken,
	}, nil
}

func (s *MoabApiServerHandler) Enqueue(ctx context.Context, req *moabpb.EnqueueRequest, accountId uint64, limits moab.ServiceLimits) (*moabpb.EnqueueResponse, error) {
	if int32(len(req.Entries)) > limits.MaxEnqueueBatchSize {
		return nil, status.Errorf(codes.InvalidArgument, "too many entries") // TODO: unified error message
	}

	now := time.Now()

	queue, err := s.getQueue(ctx, accountId, req.QueueName)
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	tasks := make([]*corepb.Task, 0)

	entries := make([]*corepb.EnqueueRequestEntry, len(req.Entries))
	for i, e := range req.Entries {
		// Take RetryStrategy from request (if it is specified) or from queue as a default
		retryStrategy := retryStrategyToCore(e.RetryStrategy)
		if retryStrategy == nil {
			retryStrategy = queue.RetryStrategy
		}

		_, expiresAt, err := resolveScheduledAtAndExpiresAt(now, e.ScheduledAt, e.ExpiresAt, queue.ExpiresInSeconds, limits.MaxScheduledDelayInSeconds)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "EnqueueRequest.Entries[%d].%s", i, err)
		}

		entries[i] = &corepb.EnqueueRequestEntry{
			Payload:              e.Payload,
			ScheduledAt:          e.ScheduledAt,
			ExpiresAt:            expiresAt,
			DedupeKey:            e.DedupeKey,
			ThreadId:             e.ThreadId,
			RetryStrategy:        retryStrategy,
			OverwriteOnDuplicate: overwriteOnDuplicateToCore(e.OverwriteOnDuplicate),
		}
	}

	var meta mrpc.ResponseMeta
	enqueueResponse, err := s.moabClient.Enqueue(ctx, &corepb.EnqueueRequest{
		QueueId: queue.Id,
		Entries: entries,
	}, mrpc.WithResponseMeta(&meta))
	if err != nil {
		freshQueue, refreshErr := s.retryWithFreshQueueIfPurged(ctx, accountId, req.QueueName, err)
		if refreshErr != nil {
			return nil, mrpc.ErrorToGRPC(refreshErr)
		}
		if freshQueue == nil {
			return nil, mrpc.ErrorToGRPC(err)
		}

		enqueueResponse, err = s.moabClient.Enqueue(ctx, &corepb.EnqueueRequest{
			QueueId: freshQueue.Id,
			Entries: entries,
		}, mrpc.WithResponseMeta(&meta))
		if err != nil {
			return nil, mrpc.ErrorToGRPC(err)
		}
	}

	tasks = append(tasks, enqueueResponse.Tasks...)

	return &moabpb.EnqueueResponse{
		Tasks: tasksToFront(tasks),
		Now:   meta.Now,
	}, nil
}

// resolveScheduledAtAndExpiresAt applies the shared Enqueue/RestartTasks
// rules for ScheduledAt and ExpiresAt. ScheduledAt clamps up to now if
// unset/past (an obviously-correct interpretation), but is rejected — not
// clamped — if it's beyond maxScheduledDelaySeconds out: an over-far value
// is far more often a unit-confusion bug than a deliberate request, and
// silently clamping it would move when the task's real work happens.
// ExpiresAt defaults to (and is clamped down to, never rejected)
// scheduledAt + queueExpiresInSeconds if unset or past that ceiling — an
// over-long request there only trims a safety margin, so it's safe to cap
// rather than reject.
func resolveScheduledAtAndExpiresAt(now time.Time, requestedScheduledAt, requestedExpiresAt int64, queueExpiresInSeconds int64, maxScheduledDelaySeconds int64) (scheduledAt int64, expiresAt int64, err error) {
	maxScheduledAt := now.Add(time.Second * time.Duration(maxScheduledDelaySeconds)).UnixNano()
	if requestedScheduledAt > maxScheduledAt {
		return 0, 0, fmt.Errorf("ScheduledAt is too far in the future")
	}

	scheduledAt = requestedScheduledAt
	if scheduledAt < now.UnixNano() {
		scheduledAt = now.UnixNano()
	}

	ceilingExpiresAt := scheduledAt + queueExpiresInSeconds*int64(time.Second)
	expiresAt = requestedExpiresAt
	if expiresAt == 0 || expiresAt > ceilingExpiresAt {
		expiresAt = ceilingExpiresAt
	}

	return scheduledAt, expiresAt, nil
}

func (s *MoabApiServerHandler) Dequeue(ctx context.Context, req *moabpb.DequeueRequest, accountId uint64, limits moab.ServiceLimits) (*moabpb.DequeueResponse, error) {
	if req.BatchSize > limits.MaxDequeueBatchSize {
		return nil, status.Errorf(codes.InvalidArgument, "too many entries")
	}

	queue, err := s.getQueue(ctx, accountId, req.QueueName)
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	// Check if dequeuing is paused at all and return an empty list with no errors
	if queue.DequeuingSettings != nil && queue.DequeuingSettings.DequeuingPaused {
		return &moabpb.DequeueResponse{}, nil
	}

	dequeueLimit := req.BatchSize
	if dequeueLimit == 0 {
		// If dequeue limit is not set (is 0) then 1 is default
		dequeueLimit = 1
	}

	// Take keepaliveTimeout from the request (if it is specified) or from the
	// queue as a default. Unlike Enqueue, this is the consumer's own call —
	// it, not the producer, is the one that knows how long it needs to hold
	// the lease.
	keepaliveTimeout := req.KeepaliveTimeoutInSeconds
	if keepaliveTimeout == 0 {
		keepaliveTimeout = queue.KeepaliveTimeoutInSeconds
	}

	var meta mrpc.ResponseMeta
	dequeueResponse, err := s.moabClient.Dequeue(ctx, &corepb.DequeueRequest{
		QueueId:                   queue.Id,
		DequeuingSettings:         queue.DequeuingSettings,
		DequeueLimit:              dequeueLimit,
		DeadLetterQueueConfig:     queue.DeadLetterQueueConfig,
		KeepaliveTimeoutInSeconds: keepaliveTimeout,
	}, mrpc.WithResponseMeta(&meta))
	if err != nil {
		freshQueue, refreshErr := s.retryWithFreshQueueIfPurged(ctx, accountId, req.QueueName, err)
		if refreshErr != nil {
			return nil, mrpc.ErrorToGRPC(refreshErr)
		}
		if freshQueue == nil {
			return nil, mrpc.ErrorToGRPC(err)
		}

		freshKeepaliveTimeout := req.KeepaliveTimeoutInSeconds
		if freshKeepaliveTimeout == 0 {
			freshKeepaliveTimeout = freshQueue.KeepaliveTimeoutInSeconds
		}

		dequeueResponse, err = s.moabClient.Dequeue(ctx, &corepb.DequeueRequest{
			QueueId:                   freshQueue.Id,
			DequeuingSettings:         freshQueue.DequeuingSettings,
			DequeueLimit:              dequeueLimit,
			DeadLetterQueueConfig:     freshQueue.DeadLetterQueueConfig,
			KeepaliveTimeoutInSeconds: freshKeepaliveTimeout,
		}, mrpc.WithResponseMeta(&meta))
		if err != nil {
			return nil, mrpc.ErrorToGRPC(err)
		}
	}

	return &moabpb.DequeueResponse{
		Tasks: tasksToFront(dequeueResponse.Tasks),
		Now:   meta.Now,
	}, nil
}

func (s *MoabApiServerHandler) ReportStatus(ctx context.Context, req *moabpb.ReportStatusRequest, accountId uint64, limits moab.ServiceLimits) (*moabpb.ReportStatusResponse, error) {
	resp1, err := s.moabClient.GetQueueByName(ctx, &corepb.GetQueueByNameRequest{
		AccountId: accountId,
		QueueName: req.QueueName,
	})
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	entries := make([]*corepb.ReportStatusRequestEntry, len(req.Entries))
	for i, e := range req.Entries {
		taskId, err := ids.DecodeTaskId(e.TaskId)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid TaskId")
		}

		reportedStatus, err := reportedStatusToCore(e.Status)
		if err != nil {
			return nil, mrpc.ErrorToGRPC(err)
		}

		// Take keepaliveTimeout from the request (if it is specified) or from
		// the queue as a default — same as Dequeue's. Only meaningful for a
		// STATUS_IN_PROGRESS heartbeat, but harmless to resolve unconditionally.
		keepaliveTimeout := e.KeepaliveTimeoutInSeconds
		if keepaliveTimeout == 0 {
			keepaliveTimeout = resp1.Queue.KeepaliveTimeoutInSeconds
		}

		entries[i] = &corepb.ReportStatusRequestEntry{
			TaskId: &corepb.TaskId{
				AccountId: accountId,
				QueueId:   resp1.Queue.Id.QueueId,
				TaskId:    taskId,
			},
			Status:                    reportedStatus,
			Attempt:                   e.Attempt,
			KeepaliveTimeoutInSeconds: keepaliveTimeout,
		}
	}

	reportResp, err := s.moabClient.ReportStatus(ctx, &corepb.ReportStatusRequest{
		QueueId:               resp1.Queue.Id,
		Entries:               entries,
		DeadLetterQueueConfig: resp1.Queue.DeadLetterQueueConfig,
	})
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	return &moabpb.ReportStatusResponse{
		Entries: reportStatusResponseEntriesToFront(reportResp.Entries),
	}, nil
}

func (s *MoabApiServerHandler) DeleteTasks(ctx context.Context, req *moabpb.DeleteTasksRequest, accountId uint64, limits moab.ServiceLimits) (*moabpb.DeleteTasksResponse, error) {
	resp1, err := s.moabClient.GetQueueByName(ctx, &corepb.GetQueueByNameRequest{
		AccountId: accountId,
		QueueName: req.QueueName,
	})
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	taskIds := make([]uint64, len(req.TaskIds))
	for i, t := range req.TaskIds {
		taskId, err := ids.DecodeTaskId(t)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid TaskId")
		}

		taskIds[i] = taskId
	}

	_, err = s.moabClient.DeleteTasks(ctx, &corepb.DeleteTasksRequest{
		QueueId: resp1.Queue.Id,
		TaskIds: taskIds,
	})
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	return &moabpb.DeleteTasksResponse{}, nil
}

func (s *MoabApiServerHandler) RestartTasks(ctx context.Context, req *moabpb.RestartTasksRequest, accountId uint64, limits moab.ServiceLimits) (*moabpb.RestartTasksResponse, error) {
	now := time.Now()

	resp1, err := s.moabClient.GetQueueByName(ctx, &corepb.GetQueueByNameRequest{
		AccountId: accountId,
		QueueName: req.QueueName,
	})
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}
	queue := resp1.Queue

	entries := make([]*corepb.RestartTasksRequestEntry, len(req.Entries))
	for i, e := range req.Entries {
		taskId, err := ids.DecodeTaskId(e.TaskId)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid TaskId")
		}

		// A restarted task is treated like a fresh arrival, so it's bounded
		// exactly like a new Enqueue.
		_, expiresAt, err := resolveScheduledAtAndExpiresAt(now, e.ScheduledAt, e.ExpiresAt, queue.ExpiresInSeconds, limits.MaxScheduledDelayInSeconds)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "RestartTasksRequest.Entries[%d].%s", i, err)
		}

		entries[i] = &corepb.RestartTasksRequestEntry{
			TaskId:      taskId,
			ScheduledAt: e.ScheduledAt,
			ExpiresAt:   expiresAt,
		}
	}

	var meta mrpc.ResponseMeta
	restartResponse, err := s.moabClient.RestartTasks(ctx, &corepb.RestartTasksRequest{
		QueueId: queue.Id,
		Entries: entries,
	}, mrpc.WithResponseMeta(&meta))
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	return &moabpb.RestartTasksResponse{
		Entries: restartTasksResponseEntriesToFront(restartResponse.Entries),
		Now:     meta.Now,
	}, nil
}

func (s *MoabApiServerHandler) CreateSchedule(ctx context.Context, req *moabpb.CreateScheduleRequest, accountId uint64, limits moab.ServiceLimits) (*moabpb.CreateScheduleResponse, error) {
	// The queue ID is randomly generated here. On the rare ID collision
	// the core returns IDCollision; we regenerate the ID and retry.
	for range maxIDGenerationAttempts {
		res, err := s.moabClient.CreateSchedule(ctx, &corepb.CreateScheduleRequest{
			AccountId:                    accountId,
			QueueName:                    req.QueueName,
			ScheduleId:                   rand.Uint64(),
			ScheduleName:                 req.Name,
			Description:                  req.Description,
			RetryStrategy:                retryStrategyToCore(req.RetryStrategy),
			Cron:                         req.Cron,
			Payload:                      req.Payload,
			DedupeKey:                    req.DedupeKey,
			ExpiresInSeconds:             req.ExpiresInSeconds,
			Timezone:                     req.Timezone,
			MaxNumberOfSchedulesPerQueue: limits.MaxNumberOfSchedulesPerQueue,
		})
		if err != nil {
			if isIDCollision(err) {
				continue
			}
			return nil, mrpc.ErrorToGRPC(err)
		}

		return &moabpb.CreateScheduleResponse{
			Schedule: scheduleToFront(res.Schedule, req.QueueName),
		}, nil
	}

	return nil, status.Error(codes.Internal, "failed to generate a unique id")
}

func (s *MoabApiServerHandler) GetSchedule(ctx context.Context, req *moabpb.GetScheduleRequest, accountId uint64, limits moab.ServiceLimits) (*moabpb.GetScheduleResponse, error) {
	res, err := s.moabClient.GetSchedule(ctx, &corepb.GetScheduleRequest{
		AccountId:    accountId,
		QueueName:    req.QueueName,
		ScheduleName: req.ScheduleName,
	})
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	return &moabpb.GetScheduleResponse{
		Schedule: scheduleToFront(res.Schedule, req.QueueName),
	}, nil
}

func (s *MoabApiServerHandler) UpdateSchedule(ctx context.Context, req *moabpb.UpdateScheduleRequest, accountId uint64, limits moab.ServiceLimits) (*moabpb.UpdateScheduleResponse, error) {
	res, err := s.moabClient.UpdateSchedule(ctx, &corepb.UpdateScheduleRequest{
		AccountId:        accountId,
		QueueName:        req.QueueName,
		ScheduleName:     req.ScheduleName,
		Description:      req.Description,
		RetryStrategy:    retryStrategyToCore(req.RetryStrategy),
		Cron:             req.Cron,
		Payload:          req.Payload,
		DedupeKey:        req.DedupeKey,
		ExpiresInSeconds: req.ExpiresInSeconds,
		Timezone:         req.Timezone,
		ExpectedVersion:  req.ExpectedVersion,
	})
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	return &moabpb.UpdateScheduleResponse{
		Schedule: scheduleToFront(res.Schedule, req.QueueName),
	}, nil
}

func (s *MoabApiServerHandler) DeleteSchedule(ctx context.Context, req *moabpb.DeleteScheduleRequest, accountId uint64, limits moab.ServiceLimits) (*moabpb.DeleteScheduleResponse, error) {
	_, err := s.moabClient.DeleteSchedule(ctx, &corepb.DeleteScheduleRequest{
		AccountId:    accountId,
		QueueName:    req.QueueName,
		ScheduleName: req.ScheduleName,
	})
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	return &moabpb.DeleteScheduleResponse{}, nil
}

func (s *MoabApiServerHandler) ListSchedules(ctx context.Context, req *moabpb.ListSchedulesRequest, accountId uint64, limits moab.ServiceLimits) (*moabpb.ListSchedulesResponse, error) {
	queue, err := s.getQueue(ctx, accountId, req.QueueName)
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	// Decode pagination token from base64-encoded format
	paginationToken, err := paginationTokenToCore(req.PaginationToken)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	resp1, err := s.moabClient.ListSchedules(ctx, &corepb.ListSchedulesRequest{
		QueueId:         queue.Id,
		PaginationToken: paginationToken,
		Limit:           req.Limit,
	})
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	nextPaginationToken, err := paginationTokenToFront(resp1.NextPaginationToken)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%s", err)
	}

	previousPaginationToken, err := paginationTokenToFront(resp1.PreviousPaginationToken)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%s", err)
	}

	return &moabpb.ListSchedulesResponse{
		Schedules:               schedulesToFront(resp1.Schedules, req.QueueName),
		NextPaginationToken:     nextPaginationToken,
		PreviousPaginationToken: previousPaginationToken,
	}, nil
}

// PurgeQueue empties a queue in one quick call regardless of how many tasks
// it holds: it rotates the queue onto a freshly generated id via
// SwapQueueId (same name, same settings, schedules carried over) so every
// task from this point on lands under the new id, then tells TasksCore to
// asynchronously drain whatever is left under the old one. The ID
// generation/retry convention mirrors CreateQueue's.
func (s *MoabApiServerHandler) PurgeQueue(ctx context.Context, req *moabpb.PurgeQueueRequest, accountId uint64, limits moab.ServiceLimits) (*moabpb.PurgeQueueResponse, error) {
	for range maxIDGenerationAttempts {
		resp1, err := s.moabClient.SwapQueueId(ctx, &corepb.SwapQueueIdRequest{
			AccountId:  accountId,
			QueueName:  req.QueueName,
			NewQueueId: rand.Uint64(),
		})
		if err != nil {
			if isIDCollision(err) {
				continue
			}
			return nil, mrpc.ErrorToGRPC(err)
		}

		// The swap already committed at this point, so getQueue's cached
		// pre-purge entry (up to queuesCacheTTL stale) must not survive it —
		// otherwise a call that lands in that window (e.g. Enqueue) would
		// resolve this name to the old id and write a task that's about to
		// be asynchronously deleted out from under it.
		s.queuesCache.Delete(fmt.Sprintf("%d/%s", accountId, req.QueueName))

		// TODO return stats
		_, err = s.moabClient.PurgeQueue(ctx, &corepb.PurgeQueueRequest{
			QueueId: resp1.OldQueueId,
		})
		if err != nil {
			return nil, mrpc.ErrorToGRPC(err)
		}

		return &moabpb.PurgeQueueResponse{}, nil
	}

	return nil, status.Error(codes.Internal, "failed to generate a unique id")
}

func (s *MoabApiServerHandler) GetTask(ctx context.Context, req *moabpb.GetTaskRequest, accountId uint64, limits moab.ServiceLimits) (*moabpb.GetTaskResponse, error) {
	resp1, err := s.moabClient.GetQueueByName(ctx, &corepb.GetQueueByNameRequest{
		AccountId: accountId,
		QueueName: req.QueueName,
	})
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	taskId, err := ids.DecodeTaskId(req.TaskId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	var meta mrpc.ResponseMeta
	resp2, err := s.moabClient.GetTask(ctx, &corepb.GetTaskRequest{
		TaskId: &corepb.TaskId{
			AccountId: accountId,
			QueueId:   resp1.Queue.Id.QueueId,
			TaskId:    taskId,
		},
	}, mrpc.WithResponseMeta(&meta))
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	return &moabpb.GetTaskResponse{
		Task: taskToFront(resp2.Task),
		Now:  meta.Now,
	}, nil
}

func (s *MoabApiServerHandler) ListTasks(ctx context.Context, req *moabpb.ListTasksRequest, accountId uint64, limits moab.ServiceLimits) (*moabpb.ListTasksResponse, error) {
	queue, err := s.getQueue(ctx, accountId, req.QueueName)
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	// Decode pagination token from base64-encoded format
	paginationToken, err := paginationTokenToCore(req.PaginationToken)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s", err)
	}

	var meta mrpc.ResponseMeta
	resp1, err := s.moabClient.ListTasks(ctx, &corepb.ListTasksRequest{
		QueueId:         queue.Id,
		PaginationToken: paginationToken,
		Limit:           req.Limit,
		State:           taskStateFilterToCore(req.State),
	}, mrpc.WithResponseMeta(&meta))
	if err != nil {
		freshQueue, refreshErr := s.retryWithFreshQueueIfPurged(ctx, accountId, req.QueueName, err)
		if refreshErr != nil {
			return nil, mrpc.ErrorToGRPC(refreshErr)
		}
		if freshQueue == nil {
			return nil, mrpc.ErrorToGRPC(err)
		}

		resp1, err = s.moabClient.ListTasks(ctx, &corepb.ListTasksRequest{
			QueueId:         freshQueue.Id,
			PaginationToken: paginationToken,
			Limit:           req.Limit,
			State:           taskStateFilterToCore(req.State),
		}, mrpc.WithResponseMeta(&meta))
		if err != nil {
			return nil, mrpc.ErrorToGRPC(err)
		}
	}

	// Encode pagination tokens for response
	nextPaginationToken, err := paginationTokenToFront(resp1.NextPaginationToken)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%s", err)
	}
	previousPaginationToken, err := paginationTokenToFront(resp1.PreviousPaginationToken)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%s", err)
	}

	return &moabpb.ListTasksResponse{
		Tasks:                   tasksToFront(resp1.Tasks),
		NextPaginationToken:     nextPaginationToken,
		PreviousPaginationToken: previousPaginationToken,
		Now:                     meta.Now,
	}, nil
}

// retryWithFreshQueueIfPurged reports whether err is TasksCore rejecting a
// call keyed by queue's (possibly stale, cached) id because that id has been
// rotated out by a PurgeQueue — which can only invalidate queuesCache on the
// gateway process that handled it, so any other gateway process keeps
// serving the pre-purge Queue out of its own cache until its TTL naturally
// expires. On that specific NotFound, it drops the stale entry and returns a
// freshly resolved queue to retry the call against; every other error
// (including an ordinary NotFound for the actual resource requested, e.g. a
// genuinely missing task) is passed through unchanged with a nil queue,
// telling the caller not to retry.
func (s *MoabApiServerHandler) retryWithFreshQueueIfPurged(ctx context.Context, accountId uint64, queueName string, err error) (*corepb.Queue, error) {
	if !isNotFound(err) {
		return nil, nil
	}

	s.queuesCache.Delete(fmt.Sprintf("%d/%s", accountId, queueName))

	return s.getQueue(ctx, accountId, queueName)
}

func (s *MoabApiServerHandler) getQueue(ctx context.Context, accountId uint64, queueName string) (*corepb.Queue, error) {
	cacheKey := fmt.Sprintf("%d/%s", accountId, queueName)
	return s.queuesCache.GetOrLoad(cacheKey, func() (*corepb.Queue, error) {
		resp1, err := s.moabClient.GetQueueByName(ctx, &corepb.GetQueueByNameRequest{
			AccountId: accountId,
			QueueName: queueName,
		})
		if err != nil {
			return nil, err
		}
		return resp1.Queue, nil
	})
}

func NewMoabApiServerHandler(moabClient coreapis.MoabClientApi) *MoabApiServerHandler {
	return &MoabApiServerHandler{
		moabClient: moabClient,

		// The queues cache holds positive entries to keep hot queues out
		// of the core's path while staying fresh enough to pick up changes, and
		// negatively caches NotFound errors so lookups of a missing queues
		// don't repeatedly hit the core. Expired entries are swept every 5m.
		queuesCache: cache.New[string, *corepb.Queue](
			cache.WithTTL(queuesCacheTTL),
			cache.WithNegativeTTL(queuesCacheNegativeTTL),
			cache.WithCleaningInterval(5*time.Minute),
			cache.WithCacheableError(isNotFound),
		),
	}
}

func isIDCollision(err error) bool {
	var appErr *mrpc.Error
	return errors.As(err, &appErr) && appErr.Code == mrpc.IDCollision
}

func isNotFound(err error) bool {
	var appErr *mrpc.Error
	return errors.As(err, &appErr) && appErr.Code == mrpc.NotFound
}
