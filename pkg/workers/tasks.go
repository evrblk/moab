package workers

import (
	"context"
	"log"
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

	worker *workers.IntervalWorker
}

func NewMoabTasksGCWorker(coreApiClient coreapis.MoabClientApi) *MoabTasksGCWorker {
	return &MoabTasksGCWorker{
		coreApiClient: coreApiClient,
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
		log.Printf("ListShards(\"MoabTasks\"): %v", err)
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
		log.Printf("RunTasksGarbageCollection failed: %v", err)
	}
}
