package workers

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/evrblk/yellowstone-common/metrics"
	"github.com/evrblk/yellowstone-common/workers"

	"github.com/evrblk/moab/pkg/coreapis"
	"github.com/evrblk/moab/pkg/corepb"
)

// MoabTasksGCWorker periodically sweeps expired and dead tasks from every
// MoabTasks shard.
type MoabTasksGCWorker struct {
	coreApiClient coreapis.MoabClientApi
	logger        *slog.Logger

	worker *workers.IntervalWorker
}

// NewMoabTasksGCWorker builds a MoabTasksGCWorker logging to logger, or to
// slog.Default() if logger is nil.
func NewMoabTasksGCWorker(coreApiClient coreapis.MoabClientApi, logger *slog.Logger) *MoabTasksGCWorker {
	if logger == nil {
		logger = slog.Default()
	}
	return &MoabTasksGCWorker{
		coreApiClient: coreApiClient,
		logger:        logger,
		worker:        workers.NewIntervalWorker(time.Duration(5) * time.Second),
	}
}

func (w *MoabTasksGCWorker) Start() {
	w.worker.Start(w.handler)
}

func (w *MoabTasksGCWorker) Stop() {
	w.worker.Stop()
}

func (w *MoabTasksGCWorker) handler() {
	shards, err := w.coreApiClient.ListShards("MoabTasks")
	if err != nil {
		w.logger.Error("ListShards failed", "error", err)
		return
	}

	now := time.Now()

	done := &sync.WaitGroup{}
	done.Add(len(shards))

	for _, shard := range shards {
		go func(shardId string, now time.Time, done *sync.WaitGroup) {
			defer done.Done()

			w.runGarbageCollection(shardId, now)
		}(shard, now, done)
	}

	done.Wait()
}

func (w *MoabTasksGCWorker) runGarbageCollection(shardId string, now time.Time) {
	defer metrics.MeasureSince(moabTasksGCWorkerDuration.WithLabelValues(shardId), time.Now())

	_, err := w.coreApiClient.RunTasksGarbageCollection(context.TODO(), &corepb.RunTasksGarbageCollectionRequest{
		MaxVisitedTasks: 1000,
		PageSize:        100,
	}, shardId)
	if err != nil {
		moabTasksGCWorkerErrorsTotal.WithLabelValues(shardId).Inc()
		w.logger.Error("RunTasksGarbageCollection failed", "shard_id", shardId, "error", err)
	}

	// Drains tasks left behind by PurgeQueue under a rotated-out queue id.
	// A separate pass from the expiry sweep above: it deletes unconditionally
	// (not just what's expired) but only for queue ids PurgeQueue has marked.
	_, err = w.coreApiClient.RunPurgeQueueGarbageCollection(context.TODO(), &corepb.RunPurgeQueueGarbageCollectionRequest{
		GcRecordsPageSize:     100,
		GcRecordTasksPageSize: 250,
		MaxVisitedTasks:       1000,
	}, shardId)
	if err != nil {
		moabTasksGCWorkerErrorsTotal.WithLabelValues(shardId).Inc()
		w.logger.Error("RunPurgeQueueGarbageCollection failed", "shard_id", shardId, "error", err)
	}
}
