# Concepts

A __queue__ is an ordered collection of tasks. A __task__ is a unit of work (or a message) that carries a useful 
`payload`. A payload is opaque to Moab, it can be end-to-end encrypted if needed.

Internally, those tasks are indexed by several properties. The main index (Enqueued index) is ordered by `scheduled_at`
timestamp. Below, a task `A` is scheduled at a time `t1`, task `B` at `t2` where `t1 < t2`, and so on:

![Concepts](/docs/images/concepts-1.png)

## Enqueuing

__Enqueuing__ is adding a task to the queue. Moab queue is durable. If `Enqueue` returns a successful response it 
guarantees all tasks have been persisted and replicated.

When enqueued, a task is in `ENQUEUED` state and is scheduled for `now` by default, meaning it is available for
processing immediately (__Fig. A__). A task can also be scheduled into the future (delayed) by explicitly specifying a
`scheduled_at` timestamp (__Fig. B__). If provided `scheduled_at` is in the past (`scheduled_at < now`) then it will be 
replaced with `now`. The timestamp `scheduled_at` is not a unique property, several tasks can be scheduled at the same
time. Timestamp `now` is measured by API gateway servers, there might be drifts in time between those servers and the
client which is enqueuing a task, or between servers themselves. Because of that, there is no guarantee that two
consequent calls from the same client will enqueue tasks in exactly the same order. FIFO order is best-effort
acccounting only to any possible clock drift between API gateway servers and clients.

![Concepts](/docs/images/concepts-2.png)

There is only one ordering function. A task's position in its thread (or in the plain queue, for non-threaded tasks) 
is always `(ScheduledAt, TaskId)`, ascending. Fresh enqueues, dedupe-key `ScheduledAt` overwrites, retry reschedules, 
and DLQ restarts are all just "a task's `ScheduledAt` changed or a task newly entered the pool" — the same comparison
decides who's next every time. There is no separate "retries go to the end" or "restarts jump the queue" rule.

## Dequeuing 

__Dequeuing__ is picking up (pulling) a task from the queue. A __worker__ is a program which periodically dequeues
tasks and processes them (a consumer).

On dequeuing Moab returns only the tasks where `scheduled_at <= now` starting from the oldest tasks, so FIFO order is
maintained. A task can be dequeued only to a single worker and can never be delivered twice. Strictly speaking, this 
is __at most once__ delivery guarantee for each attempt.

![Concepts](/docs/images/concepts-3.png)

When dequeued, tasks go to `IN_PROGRESS` state and `attempts = 1` indicates that it is picked up for the first time. 
They are removed from the main index and added to InProgress index. A task is considered in-progress for 
`keepalive_timeout_in_seconds` period, so it is placed to the InProgress index at `now + keepalive_timeout_in_seconds` 
position. Optional `keepalive_timeout_in_seconds` can be set in the `Dequeue` request for the batch of tasks it picks
up, otherwise a default `keepalive_timeout_in_seconds` from the queue is used. It is set on `Dequeue` rather than
`Enqueue` because it is the worker dequeuing a task, not whatever enqueued it, that knows how long it actually needs
to hold the lease.

![Concepts](/docs/images/concepts-4.png)

A worker must report a status of an in-progress task within this `keepalive_timeout_in_seconds` period. A status report
can be any of:

* `SUCCEEDED` - the worker has successfully processed this task.
* `IN_PROGRESS` - the worker is still processing this task and wants to renew its lease. There is no upper limit for
  long-processing tasks as long as a worker keeps reporting before its lease expires. This report can itself carry
  an optional `keepalive_timeout_in_seconds`, same as `Dequeue`'s — 0 (or omitted) inherits the queue's default,
  otherwise it overrides it for this renewal. The task never stores a keepalive timeout of its own: each `Dequeue`
  and each `IN_PROGRESS` report resolves its own, independently.
* `FAILED` - there was a failure while processing this task and the worker recognized this failure.

Succeeded tasks are removed from the queue. In-progress tasks are moved in InProgress index further to the next 
`keepalive_timeout_in_seconds`. Failed tasks are retried.

![Concepts](/docs/images/concepts-5.png)

If a worker is dead or for any reason has not reported any status within given timeout a task is considered `FAILED`. A
task has to be explicitly reported as `SUCCEEDED` in order to be deleted from the queue. This makes Moab resilient to
all sorts of failures (with network or workers). See [Retries](/docs/moab/retries/) section for more details.
