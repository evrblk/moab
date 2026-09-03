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

	resp2, err := s.moabClient.GetStatistics(ctx, &corepb.GetStatisticsRequest{
		QueueId: resp1.Queue.Id,
	})
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	return &moabpb.GetQueueResponse{
		Queue: queueToFront(resp1.Queue),
		Stats: queueStatsToFront(resp2),
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

func (s *MoabApiServerHandler) DeleteQueue(ctx context.Context, req *moabpb.DeleteQueueRequest, accountId uint64, limits moab.ServiceLimits) (*moabpb.DeleteQueueResponse, error) {
	_, err := s.moabClient.DeleteQueue(ctx, &corepb.DeleteQueueRequest{
		AccountId: accountId,
		QueueName: req.QueueName,
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
		return nil, status.Errorf(codes.InvalidArgument, "too many entries")
	}

	now := time.Now()

	queue, err := s.getQueue(ctx, accountId, req.QueueName)
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	tasks := make([]*corepb.Task, 0)

	entries := make([]*corepb.EnqueueRequestEntry, len(req.Entries))
	for i, e := range req.Entries {
		// Take keepaliveTimeout from request (if it is specified) or from queue as a default
		keepaliveTimeout := e.KeepaliveTimeoutInSeconds
		if keepaliveTimeout == 0 {
			keepaliveTimeout = queue.KeepaliveTimeoutInSeconds
		}

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
			Payload:                   e.Payload,
			ScheduledAt:               e.ScheduledAt,
			ExpiresAt:                 expiresAt,
			DedupeKey:                 e.DedupeKey,
			ThreadId:                  e.ThreadId,
			KeepaliveTimeoutInSeconds: keepaliveTimeout,
			RetryStrategy:             retryStrategy,
			OverwriteOnDuplicate:      overwriteOnDuplicateToCore(e.OverwriteOnDuplicate),
		}
	}

	enqueueResponse, err := s.moabClient.Enqueue(ctx, &corepb.EnqueueRequest{
		QueueId: queue.Id,
		Entries: entries,
	})
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	tasks = append(tasks, enqueueResponse.Tasks...)

	return &moabpb.EnqueueResponse{
		Tasks: tasksToFront(tasks),
	}, nil
}

// resolveScheduledAtAndExpiresAt applies the shared Enqueue/RestartTasks
// rules for ScheduledAt and ExpiresAt — kept as one function so the two
// handlers can't drift apart. ScheduledAt clamps up to now if unset/past (an
// obviously-correct
// interpretation), but is rejected — not clamped — if it's beyond
// maxScheduledDelaySeconds out: an over-far value is far more often a
// unit-confusion bug than a deliberate request, and silently clamping it
// would move when the task's real work happens. ExpiresAt defaults to (and
// is clamped down to, never rejected) scheduledAt + queueExpiresInSeconds if
// unset or past that ceiling — an over-long request there only trims a
// safety margin, so it's safe to cap rather than reject.
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
	dequeueResponse, err := s.moabClient.Dequeue(ctx, &corepb.DequeueRequest{
		QueueId:               queue.Id,
		DequeuingSettings:     queue.DequeuingSettings,
		DequeueLimit:          dequeueLimit,
		DeadLetterQueueConfig: queue.DeadLetterQueueConfig,
	})
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	return &moabpb.DequeueResponse{
		Tasks: tasksToFront(dequeueResponse.Tasks),
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

		entries[i] = &corepb.ReportStatusRequestEntry{
			TaskId: &corepb.TaskId{
				AccountId: accountId,
				QueueId:   resp1.Queue.Id.QueueId,
				TaskId:    taskId,
			},
			Status:  reportedStatus,
			Attempt: e.Attempt,
		}
	}

	_, err = s.moabClient.ReportStatus(ctx, &corepb.ReportStatusRequest{
		QueueId:               resp1.Queue.Id,
		Entries:               entries,
		DeadLetterQueueConfig: resp1.Queue.DeadLetterQueueConfig,
	})
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	return &moabpb.ReportStatusResponse{}, nil
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

	restartResponse, err := s.moabClient.RestartTasks(ctx, &corepb.RestartTasksRequest{
		QueueId: queue.Id,
		Entries: entries,
	})
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	return &moabpb.RestartTasksResponse{
		Entries: restartTasksResponseEntriesToFront(restartResponse.Entries),
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
			KeepaliveTimeoutInSeconds:    req.KeepaliveTimeoutInSeconds,
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
		AccountId:                 accountId,
		QueueName:                 req.QueueName,
		ScheduleName:              req.ScheduleName,
		Description:               req.Description,
		KeepaliveTimeoutInSeconds: req.KeepaliveTimeoutInSeconds,
		RetryStrategy:             retryStrategyToCore(req.RetryStrategy),
		Cron:                      req.Cron,
		Payload:                   req.Payload,
		DedupeKey:                 req.DedupeKey,
		ExpiresInSeconds:          req.ExpiresInSeconds,
		Timezone:                  req.Timezone,
		ExpectedVersion:           req.ExpectedVersion,
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

func (s *MoabApiServerHandler) PurgeQueue(ctx context.Context, req *moabpb.PurgeQueueRequest, accountId uint64, limits moab.ServiceLimits) (*moabpb.PurgeQueueResponse, error) {
	resp1, err := s.moabClient.GetQueueByName(ctx, &corepb.GetQueueByNameRequest{
		AccountId: accountId,
		QueueName: req.QueueName,
	})
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	// TODO return stats
	_, err = s.moabClient.PurgeQueue(ctx, &corepb.PurgeQueueRequest{
		QueueId: resp1.Queue.Id,
	})
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	return &moabpb.PurgeQueueResponse{}, nil
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

	resp2, err := s.moabClient.GetTask(ctx, &corepb.GetTaskRequest{
		TaskId: &corepb.TaskId{
			AccountId: accountId,
			QueueId:   resp1.Queue.Id.QueueId,
			TaskId:    taskId,
		},
	})
	if err != nil {
		return nil, mrpc.ErrorToGRPC(err)
	}

	return &moabpb.GetTaskResponse{
		Task: taskToFront(resp2.Task),
	}, nil
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
