package corepb

import (
	"github.com/evrblk/monstera/cluster"

	"github.com/evrblk/moab/pkg/sharding"
)

// CreateQueueRequest

func (r *CreateQueueRequest) ShardKey() cluster.ShardKey {
	return sharding.ByAccount(r.QueueId.AccountId)
}

// CreateScheduleRequest

func (r *CreateScheduleRequest) ShardKey() cluster.ShardKey {
	return sharding.ByAccount(r.AccountId)
}

// DeleteQueueRequest

func (r *DeleteQueueRequest) ShardKey() cluster.ShardKey {
	return sharding.ByAccount(r.AccountId)
}

// DeleteScheduleRequest

func (r *DeleteScheduleRequest) ShardKey() cluster.ShardKey {
	return sharding.ByAccount(r.AccountId)
}

// DeleteTasksRequest

func (r *DeleteTasksRequest) ShardKey() cluster.ShardKey {
	return sharding.ByAccountAndQueue(r.QueueId.AccountId, r.QueueId.QueueId)
}

// DequeueRequest

func (r *DequeueRequest) ShardKey() cluster.ShardKey {
	return sharding.ByAccountAndQueue(r.QueueId.AccountId, r.QueueId.QueueId)
}

// EnqueueRequest

func (r *EnqueueRequest) ShardKey() cluster.ShardKey {
	return sharding.ByAccountAndQueue(r.QueueId.AccountId, r.QueueId.QueueId)
}

// GetQueueRequest

func (r *GetQueueRequest) ShardKey() cluster.ShardKey {
	return sharding.ByAccount(r.QueueId.AccountId)
}

// GetQueueByNameRequest

func (r *GetQueueByNameRequest) ShardKey() cluster.ShardKey {
	return sharding.ByAccount(r.AccountId)
}

// GetScheduleRequest

func (r *GetScheduleRequest) ShardKey() cluster.ShardKey {
	return sharding.ByAccount(r.AccountId)
}

// GetStatisticsRequest

func (r *GetStatisticsRequest) ShardKey() cluster.ShardKey {
	return sharding.ByAccountAndQueue(r.QueueId.AccountId, r.QueueId.QueueId)
}

// GetTaskRequest

func (r *GetTaskRequest) ShardKey() cluster.ShardKey {
	return sharding.ByAccountAndQueue(r.TaskId.AccountId, r.TaskId.QueueId)
}

// ListQueuesRequest

func (r *ListQueuesRequest) ShardKey() cluster.ShardKey {
	return sharding.ByAccount(r.AccountId)
}

// ListSchedulesRequest

func (r *ListSchedulesRequest) ShardKey() cluster.ShardKey {
	return sharding.ByAccount(r.QueueId.AccountId)
}

// PurgeQueueRequest

func (r *PurgeQueueRequest) ShardKey() cluster.ShardKey {
	return sharding.ByAccountAndQueue(r.QueueId.AccountId, r.QueueId.QueueId)
}

// ReportStatusRequest

func (r *ReportStatusRequest) ShardKey() cluster.ShardKey {
	return sharding.ByAccountAndQueue(r.QueueId.AccountId, r.QueueId.QueueId)
}

// RestartTasksRequest

func (r *RestartTasksRequest) ShardKey() cluster.ShardKey {
	return sharding.ByAccountAndQueue(r.QueueId.AccountId, r.QueueId.QueueId)
}

// UpdateQueueRequest

func (r *UpdateQueueRequest) ShardKey() cluster.ShardKey {
	return sharding.ByAccount(r.AccountId)
}

// UpdateScheduleRequest

func (r *UpdateScheduleRequest) ShardKey() cluster.ShardKey {
	return sharding.ByAccount(r.AccountId)
}
