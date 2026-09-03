package workers

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/adhocore/gronx"
	"github.com/evrblk/yellowstone-common/metrics"
	"github.com/evrblk/yellowstone-common/workers"

	"github.com/evrblk/moab/pkg/coreapis"
	"github.com/evrblk/moab/pkg/corepb"
)

// MoabQueuesCronWorker periodically dequeues due schedules from every
// MoabQueues shard and enqueues the resulting tasks.
type MoabQueuesCronWorker struct {
	coreApiClient coreapis.MoabClientApi

	worker *workers.IntervalWorker
}

func NewMoabQueuesCronWorker(coreApiClient coreapis.MoabClientApi) *MoabQueuesCronWorker {
	return &MoabQueuesCronWorker{
		coreApiClient: coreApiClient,
		worker:        workers.NewIntervalWorker(time.Duration(5) * time.Second),
	}
}

func (w *MoabQueuesCronWorker) Start() {
	w.worker.Start(w.handler)
}

func (w *MoabQueuesCronWorker) Stop() {
	w.worker.Stop()
}

func (w *MoabQueuesCronWorker) handler() {
	shards, err := w.coreApiClient.ListShards("MoabQueues")
	if err != nil {
		log.Printf("ListShards(\"MoabQueues\"): %v", err)
		return
	}

	now := time.Now()

	done := &sync.WaitGroup{}
	done.Add(len(shards))

	for _, shard := range shards {
		go func(shardId string, now time.Time, done *sync.WaitGroup) {
			defer done.Done()

			err := w.fetch(shardId, now)
			if err != nil {
				log.Printf("MoabQueuesCronWorker failed to process shard %s: %v", shardId, err)
			}
		}(shard, now, done)
	}

	done.Wait()
}

func (w *MoabQueuesCronWorker) fetch(shardId string, now time.Time) error {
	defer metrics.MeasureSince(moabQueuesCronWorkerDuration.WithLabelValues(shardId), time.Now())

	dueBefore := now.Add(time.Second * 60)

	for {
		ctx := context.TODO()

		resp, err := w.coreApiClient.DequeSchedules(ctx, &corepb.DequeSchedulesRequest{
			DueBefore: dueBefore.UnixNano(),
		}, shardId)
		if err != nil {
			moabQueuesCronWorkerErrorsTotal.WithLabelValues(shardId).Inc()
			log.Printf("DequeSchedules failed: %v", err)
			return err
		}

		moabQueuesCronWorkerSchedulesTotal.WithLabelValues(shardId).Add(float64(len(resp.Entries)))

		if len(resp.Entries) == 0 {
			return nil
		}

		for _, entry := range resp.Entries {
			queue := entry.Queue
			schedule := entry.Schedule

			retryStrategy := queue.RetryStrategy
			if schedule.RetryStrategy != nil {
				retryStrategy = schedule.RetryStrategy
			}

			entries := make([]*corepb.EnqueueRequestEntry, 0)

			// The greatest ScheduledAt among tasks actually enqueued below,
			// or 0 if none are. Ticks are visited in increasing order, so
			// whichever entry is appended last holds the greatest value.
			var lastEnqueuedFor int64

			scheduledAt := schedule.NextScheduledAt

			for scheduledAt <= dueBefore.UnixNano() {
				// Only enqueue tasks that are actually scheduled for the future. If there was a gap in this
				// worker's availability and schedule.NextScheduledAt is lagging behind, those missing tasks
				// are skipped.
				if scheduledAt >= now.UnixNano() {
					// Take ExpiresInSeconds from the schedule (if it overrides it) or
					// from the queue as a default — every task must have a real,
					// positive ExpiresAt; there is no "never expires" sentinel value.
					expiresInSeconds := queue.ExpiresInSeconds
					if schedule.ExpiresInSeconds > 0 {
						expiresInSeconds = schedule.ExpiresInSeconds
					}
					expiresAt := scheduledAt + expiresInSeconds*int64(time.Second)

					if expiresAt > now.UnixNano() {
						entries = append(entries, &corepb.EnqueueRequestEntry{
							Payload:                   schedule.Payload,
							ScheduledAt:               scheduledAt,
							ExpiresAt:                 expiresAt,
							DedupeKey:                 schedule.DedupeKey,
							KeepaliveTimeoutInSeconds: schedule.KeepaliveTimeoutInSeconds,
							RetryStrategy:             retryStrategy,
						})
						lastEnqueuedFor = scheduledAt
					}
				}

				scheduledAt, err = nextTick(scheduledAt, schedule)
				if err != nil {
					moabQueuesCronWorkerErrorsTotal.WithLabelValues(shardId).Inc()
					log.Printf("next tick failed: %v", err)
					return err
				}
			}

			_, err = w.coreApiClient.Enqueue(ctx, &corepb.EnqueueRequest{
				QueueId: queue.Id,
				Entries: entries,
			})
			if err != nil {
				moabQueuesCronWorkerErrorsTotal.WithLabelValues(shardId).Inc()
				log.Printf("Enqueue failed: %v", err)
				return err
			}

			_, err = w.coreApiClient.ReportSchedulesStatus(ctx, &corepb.ReportSchedulesStatusRequest{
				ScheduleId:      schedule.Id,
				NextScheduledAt: scheduledAt,
				LastEnqueuedFor: lastEnqueuedFor,
			}, shardId)
			if err != nil {
				moabQueuesCronWorkerErrorsTotal.WithLabelValues(shardId).Inc()
				log.Printf("ReportSchedulesStatus failed: %v", err)
				return err
			}
		}
	}
}

func nextTick(nanos int64, schedule *corepb.Schedule) (int64, error) {
	tz, err := time.LoadLocation(schedule.Timezone)
	if err != nil {
		return 0, err
	}

	t, err := gronx.NextTickAfter(schedule.Cron, time.Unix(0, nanos).In(tz), false)
	if err != nil {
		return 0, err
	}
	return t.UnixNano(), nil
}

// MoabQueuesGCWorker periodically sweeps dead schedules from every
// MoabQueues shard.
type MoabQueuesGCWorker struct {
	coreApiClient coreapis.MoabClientApi

	worker *workers.IntervalWorker
}

func NewMoabQueuesGCWorker(coreApiClient coreapis.MoabClientApi) *MoabQueuesGCWorker {
	return &MoabQueuesGCWorker{
		coreApiClient: coreApiClient,
		worker:        workers.NewIntervalWorker(time.Duration(5) * time.Second),
	}
}

func (w *MoabQueuesGCWorker) Start() {
	w.worker.Start(w.handler)
}

func (w *MoabQueuesGCWorker) Stop() {
	w.worker.Stop()
}

func (w *MoabQueuesGCWorker) handler() {
	shards, err := w.coreApiClient.ListShards("MoabQueues")
	if err != nil {
		log.Printf("ListShards(\"MoabQueues\"): %v", err)
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

func (w *MoabQueuesGCWorker) runGarbageCollection(shardId string, now time.Time) {
	defer metrics.MeasureSince(moabQueuesGCWorkerDuration.WithLabelValues(shardId), time.Now())

	_, err := w.coreApiClient.RunQueuesGarbageCollection(context.TODO(), &corepb.RunQueuesGarbageCollectionRequest{
		GcRecordsPageSize:         100,
		GcRecordSchedulesPageSize: 250,
		MaxVisitedSchedules:       1000,
	}, shardId)
	if err != nil {
		moabQueuesGCWorkerErrorsTotal.WithLabelValues(shardId).Inc()
		log.Printf("RunQueuesGarbageCollection failed: %v", err)
	}
}
