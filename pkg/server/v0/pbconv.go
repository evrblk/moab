package v0

import (
	"encoding/base64"

	"github.com/samber/lo"
	"google.golang.org/protobuf/proto"

	moabpb "github.com/evrblk/evrblk-go/moab/v0"
	"github.com/evrblk/moab/pkg/corepb"
	"github.com/evrblk/moab/pkg/ids"
)

func queuesToFront(l []*corepb.Queue) []*moabpb.Queue {
	return lo.Map(l, func(q *corepb.Queue, _ int) *moabpb.Queue {
		return queueToFront(q)
	})
}

func queueToFront(q *corepb.Queue) *moabpb.Queue {
	if q != nil {
		return &moabpb.Queue{
			Name:                      q.Name,
			Description:               q.Description,
			CreatedAt:                 q.CreatedAt,
			UpdatedAt:                 q.UpdatedAt,
			Version:                   q.Version,
			KeepaliveTimeoutInSeconds: q.KeepaliveTimeoutInSeconds,
			ExpiresInSeconds:          q.ExpiresInSeconds,
			RetryStrategy:             retryStrategyToFront(q.RetryStrategy),
			DequeuingSettings:         dequeuingSettingsToFront(q.DequeuingSettings),
			DeadLetterQueueConfig:     deadLetterQueueConfigToFront(q.DeadLetterQueueConfig),
		}
	} else {
		return nil
	}
}

func retryStrategyToFront(s *corepb.RetryStrategy) *moabpb.RetryStrategy {
	if s != nil {
		return &moabpb.RetryStrategy{
			RetryIntervalsInSeconds: s.RetryIntervalsInSeconds,
		}
	} else {
		return nil
	}
}

func dequeuingSettingsToFront(s *corepb.DequeuingSettings) *moabpb.DequeuingSettings {
	if s != nil {
		return &moabpb.DequeuingSettings{
			MaxInProgressTasks: s.MaxInProgressTasks,
			RateLimiting:       rateLimitingToFront(s.RateLimiting),
			DequeuingPaused:    s.DequeuingPaused,
		}
	} else {
		return nil
	}
}

func rateLimitingToFront(r *corepb.TokenBucketRateLimiting) *moabpb.TokenBucketRateLimiting {
	if r != nil {
		return &moabpb.TokenBucketRateLimiting{
			MaxTokens:    r.MaxTokens,
			Interval:     r.Interval,
			IntervalUnit: intervalUnitToFront(r.IntervalUnit),
		}
	} else {
		return nil
	}
}

func intervalUnitToFront(u corepb.IntervalUnit) moabpb.IntervalUnit {
	switch u {
	case corepb.IntervalUnit_INTERVAL_UNIT_SECONDS:
		return moabpb.IntervalUnit_INTERVAL_UNIT_SECONDS
	case corepb.IntervalUnit_INTERVAL_UNIT_MINUTES:
		return moabpb.IntervalUnit_INTERVAL_UNIT_MINUTES
	case corepb.IntervalUnit_INTERVAL_UNIT_HOURS:
		return moabpb.IntervalUnit_INTERVAL_UNIT_HOURS
	default:
		return moabpb.IntervalUnit_INTERVAL_UNIT_SECONDS
	}
}

func deadLetterQueueConfigToFront(c *corepb.DeadLetterQueueConfig) *moabpb.DeadLetterQueueConfig {
	if c != nil {
		return &moabpb.DeadLetterQueueConfig{
			Enable:                   c.Enable,
			MaxSize:                  c.MaxSize,
			RetentionPeriodInSeconds: c.RetentionPeriodInSeconds,
		}
	} else {
		return nil
	}
}

func retryStrategyToCore(s *moabpb.RetryStrategy) *corepb.RetryStrategy {
	if s != nil {
		return &corepb.RetryStrategy{
			RetryIntervalsInSeconds: s.RetryIntervalsInSeconds,
		}
	} else {
		return nil
	}
}

func overwriteOnDuplicateToCore(l []moabpb.EnqueueRequestEntry_OverwriteOnDuplicate) []corepb.EnqueueRequestEntry_OverwriteOnDuplicate {
	return lo.Map(l, func(o moabpb.EnqueueRequestEntry_OverwriteOnDuplicate, _ int) corepb.EnqueueRequestEntry_OverwriteOnDuplicate {
		switch o {
		case moabpb.EnqueueRequestEntry_OVERWRITE_ON_DUPLICATE_SCHEDULED_AT:
			return corepb.EnqueueRequestEntry_OVERWRITE_ON_DUPLICATE_SCHEDULED_AT
		case moabpb.EnqueueRequestEntry_OVERWRITE_ON_DUPLICATE_PAYLOAD:
			return corepb.EnqueueRequestEntry_OVERWRITE_ON_DUPLICATE_PAYLOAD
		case moabpb.EnqueueRequestEntry_OVERWRITE_ON_DUPLICATE_EXPIRES_AT:
			return corepb.EnqueueRequestEntry_OVERWRITE_ON_DUPLICATE_EXPIRES_AT
		default:
			return corepb.EnqueueRequestEntry_OVERWRITE_ON_DUPLICATE_INVALID
		}
	})
}

func dequeuingSettingsToCore(s *moabpb.DequeuingSettings) *corepb.DequeuingSettings {
	if s != nil {
		return &corepb.DequeuingSettings{
			MaxInProgressTasks: s.MaxInProgressTasks,
			DequeuingPaused:    s.DequeuingPaused,
			RateLimiting:       rateLimitingToCore(s.RateLimiting),
		}
	} else {
		return nil
	}
}

func rateLimitingToCore(r *moabpb.TokenBucketRateLimiting) *corepb.TokenBucketRateLimiting {
	if r != nil {
		return &corepb.TokenBucketRateLimiting{
			MaxTokens:    r.MaxTokens,
			Interval:     r.Interval,
			IntervalUnit: intervalUnitToCore(r.IntervalUnit),
		}
	} else {
		return nil
	}
}

func intervalUnitToCore(u moabpb.IntervalUnit) corepb.IntervalUnit {
	switch u {
	case moabpb.IntervalUnit_INTERVAL_UNIT_SECONDS:
		return corepb.IntervalUnit_INTERVAL_UNIT_SECONDS
	case moabpb.IntervalUnit_INTERVAL_UNIT_MINUTES:
		return corepb.IntervalUnit_INTERVAL_UNIT_MINUTES
	case moabpb.IntervalUnit_INTERVAL_UNIT_HOURS:
		return corepb.IntervalUnit_INTERVAL_UNIT_HOURS
	default:
		return corepb.IntervalUnit_INTERVAL_UNIT_SECONDS
	}
}

func deadLetterQueueConfigToCore(c *moabpb.DeadLetterQueueConfig) *corepb.DeadLetterQueueConfig {
	if c != nil {
		return &corepb.DeadLetterQueueConfig{
			Enable:                   c.Enable,
			MaxSize:                  c.MaxSize,
			RetentionPeriodInSeconds: c.RetentionPeriodInSeconds,
		}
	} else {
		return nil
	}
}

func queueStatsToFront(r *corepb.GetStatisticsResponse) *moabpb.QueueStats {
	return &moabpb.QueueStats{
		EnqueuedTasksCount:      r.EnqueuedTasksCount,
		InProgressTasksCount:    r.InProgressTasksCount,
		DeadTasksCount:          r.DeadTasksCount,
		AgeOfOldestEnqueuedTask: r.AgeOfOldestEnqueuedTask,
	}
}

func tasksToFront(l []*corepb.Task) []*moabpb.Task {
	return lo.Map(l, func(t *corepb.Task, _ int) *moabpb.Task {
		return taskToFront(t)
	})
}

func taskToFront(t *corepb.Task) *moabpb.Task {
	if t != nil {
		return &moabpb.Task{
			Id:          ids.EncodeTaskId(t.Id.TaskId),
			Payload:     t.Payload,
			CreatedAt:   t.CreatedAt,
			ScheduledAt: t.ScheduledAt,
			ExpiresAt:   t.ExpiresAt,
			Attempts:    t.Attempts,
			DedupeKey:   t.DedupeKey,
			ThreadId:    t.ThreadId,
		}
	} else {
		return nil
	}
}

func restartTasksResponseEntriesToFront(l []*corepb.RestartTasksResponseEntry) []*moabpb.RestartTasksResponseEntry {
	return lo.Map(l, func(e *corepb.RestartTasksResponseEntry, _ int) *moabpb.RestartTasksResponseEntry {
		return &moabpb.RestartTasksResponseEntry{
			TaskId: ids.EncodeTaskId(e.TaskId),
			Result: restartResultToFront(e.Result),
			Task:   taskToFront(e.Task),
		}
	})
}

func restartResultToFront(r corepb.RestartTasksResponseEntry_Result) moabpb.RestartTasksResponseEntry_Result {
	switch r {
	case corepb.RestartTasksResponseEntry_RESULT_RESTARTED:
		return moabpb.RestartTasksResponseEntry_RESULT_RESTARTED
	case corepb.RestartTasksResponseEntry_RESULT_NOT_FOUND:
		return moabpb.RestartTasksResponseEntry_RESULT_NOT_FOUND
	case corepb.RestartTasksResponseEntry_RESULT_NOT_DEAD:
		return moabpb.RestartTasksResponseEntry_RESULT_NOT_DEAD
	case corepb.RestartTasksResponseEntry_RESULT_DEDUPE_CONFLICT:
		return moabpb.RestartTasksResponseEntry_RESULT_DEDUPE_CONFLICT
	default:
		return moabpb.RestartTasksResponseEntry_RESULT_INVALID
	}
}

func reportedStatusToCore(s moabpb.ReportStatusRequestEntry_Status) (corepb.ReportStatusRequestEntry_Status, error) {
	switch s {
	case moabpb.ReportStatusRequestEntry_STATUS_FAILED:
		return corepb.ReportStatusRequestEntry_STATUS_FAILED, nil
	case moabpb.ReportStatusRequestEntry_STATUS_IN_PROGRESS:
		return corepb.ReportStatusRequestEntry_STATUS_IN_PROGRESS, nil
	case moabpb.ReportStatusRequestEntry_STATUS_SUCCEEDED:
		return corepb.ReportStatusRequestEntry_STATUS_SUCCEEDED, nil
	default:
		return 0, nil
	}
}

func schedulesToFront(l []*corepb.Schedule, queueName string) []*moabpb.Schedule {
	return lo.Map(l, func(s *corepb.Schedule, _ int) *moabpb.Schedule {
		return scheduleToFront(s, queueName)
	})
}

func scheduleToFront(s *corepb.Schedule, queueName string) *moabpb.Schedule {
	if s != nil {
		return &moabpb.Schedule{
			Name:                      s.Name,
			Description:               s.Description,
			QueueName:                 queueName,
			CreatedAt:                 s.CreatedAt,
			UpdatedAt:                 s.UpdatedAt,
			Cron:                      s.Cron,
			Version:                   s.Version,
			Payload:                   s.Payload,
			DedupeKey:                 s.DedupeKey,
			ExpiresInSeconds:          s.ExpiresInSeconds,
			KeepaliveTimeoutInSeconds: s.KeepaliveTimeoutInSeconds,
			RetryStrategy:             retryStrategyToFront(s.RetryStrategy),
			Timezone:                  s.Timezone,
			// LastExecutedAt:            s.LastExecutedAt,
			//NextScheduledAt:           s.NextScheduledAt,
		}
	} else {
		return nil
	}
}

func paginationTokenToFront(paginationToken *corepb.PaginationToken) (string, error) {
	if paginationToken == nil {
		return "", nil
	}

	data, err := proto.Marshal(paginationToken)
	if err != nil {
		return "", err
	}

	return base64.StdEncoding.EncodeToString(data), nil
}

func paginationTokenToCore(paginationTokenBase64 string) (*corepb.PaginationToken, error) {
	if paginationTokenBase64 == "" {
		return nil, nil
	}

	paginationTokenBytes, err := base64.StdEncoding.DecodeString(paginationTokenBase64)
	if err != nil {
		return nil, err
	}

	paginationToken := &corepb.PaginationToken{}
	err = proto.Unmarshal(paginationTokenBytes, paginationToken)
	if err != nil {
		return nil, err
	}

	return paginationToken, nil
}
