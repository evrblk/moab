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
position. Optional `keepalive_timeout_in_seconds` can be set in `Enqueue` request for each task individually, otherwise
a default `keepalive_timeout_in_seconds` from the queue is used. 

![Concepts](/docs/images/concepts-4.png)

A worker must report a status of an in-progress task within this `keepalive_timeout_in_seconds` period. A status report
can be any of:

* `SUCCEEDED` - the worker has successfully processed this task.
* `IN_PROGRESS` - the worker is still processing this task. There is no upper limit for long-processing tasks as long
  as a worker is able to report status every `keepalive_timeout_in_seconds`.
* `FAILED` - there was a failure while processing this task and the worker recognized this failure.

Succeeded tasks are removed from the queue. In-progress tasks are moved in InProgress index further to the next 
`keepalive_timeout_in_seconds`. Failed tasks are retried.

![Concepts](/docs/images/concepts-5.png)

If a worker is dead or for any reason has not reported any status within given timeout a task is considered `FAILED`. A
task has to be explicitly reported as `SUCCEEDED` in order to be deleted from the queue. This makes Moab resilient to
all sorts of failures (with network or workers). See [Retries](/docs/moab/retries/) section for more details.



## 3. Eight design laws

**Law 1 — one ordering function, no special cases.** A task's position in its thread (or in the
plain queue, for non-threaded tasks) is always `(ScheduledAt, TaskId)`, ascending, full stop. Fresh
enqueues, dedupe-key `ScheduledAt` overwrites, retry reschedules, and DLQ restarts are all just
"a task's `ScheduledAt` changed or a task newly entered the pool" — the same comparison decides
who's next every time. There is no separate "retries go to the end" or "restarts jump the queue"
rule (this directly answers the "does a retried threaded task go to the end of the thread?"
question in §7).

**Law 2 — one deadline field, state-dependent meaning.** `ExpiresAt` means three different things
depending on state, and a task only ever has one of them active at a time:
- `ENQUEUED`: "don't bother dequeuing me after this" (a delivery deadline, anchored at
  `ScheduledAt + ExpiresInSeconds`, computed once at creation/restart).
- `IN_PROGRESS`: inert. Not enforced while the lease is being renewed (Law 3).
- `DEAD`: "purge me from the DLQ after this" (a retention deadline, anchored at `LastFailedAt +
  DeadLetterQueueConfig.retention_period_in_seconds`, recomputed at the moment of death).

Reusing one field for both "alive" and "dead" deadlines, recomputed at the transition, means the
expiration index and GC sweep stay a single generic mechanism regardless of what a row currently
means — no second TTL system needed for the DLQ.



**Law 4 — dedupe keys guard live uniqueness, not historical identity.** A `DedupeKey` is claimed by
at most one live (`ENQUEUED` or `IN_PROGRESS`) task at a time. It is released the instant a task
stops being live — on success, on delete, and on death — so a fresh task can reuse the key
immediately after a failure. Because of that, any *out-of-band* resurrection of an old task under
that key (i.e. `RestartTasks`) must re-validate the key against current state rather than assume
it's still free — restart is not exempt from the live-uniqueness guarantee just because it's an
explicit operator action (§7, first question).

**Law 5 — a restart is a birth, not a resume.** Restarting a `DEAD` task produces exactly the state
a fresh `Enqueue` of the same payload would: `Attempts` resets to 0, it competes for its thread's
head exactly like a new arrival (Law 1), and it gets a brand new forward-looking `ExpiresAt`
computed the same way a fresh enqueue's is (`now + ExpiresInSeconds`) — never the stale original
absolute deadline, and never an extension of the DLQ retention deadline it had while dead (§7,
second and third questions).

**Law 6 — failure handling is unified and fenced.** There is exactly one failure-handling function,
invoked either by an explicit `ReportStatus(FAILED)` or by keepalive-timeout discovery treating a
silently-abandoned lease as an implicit failure. Both apply the *same* `RetryStrategy` backoff, the
*same* expiry check (Law 3), and the *same* thread reconciliation. Every `ReportStatus` call
(`SUCCEEDED`/`FAILED`/`IN_PROGRESS` alike) is fenced by `entry.Attempt == task.Attempts`; a
mismatch means a stale worker is reporting against an attempt that's no longer current, and it must
be a silent no-op, not a mutation. This closes a real gap in the current implementation (§8).



**Law 8 — no unlimited delay, either, and its violations are rejected, not clamped.**
`ScheduledAt`'s *low* end is already handled correctly and should stay a clamp: unset or
past-scheduled means "run ASAP," so `pkg/tasks/core.go`'s `Enqueue` clamping it up to `now` is
exactly right — there's an obviously-correct interpretation of that input. `ScheduledAt`'s *high*
end has no bound at all today (`validators.go` only rejects `< 0`; nothing rejects a `ScheduledAt`
decades out), and it needs one for the same storage-hazard reason Law 7 bounds expiration: an
unbounded delay lets a task sit `ENQUEUED`-but-not-yet-due, occupying `queueIndex`/
`threadedTasksIndex` and inflating `EnqueuedTasksCount`, for as long as the caller likes — a gap
Law 7 doesn't close, since a far-future task's `ExpiresAt` is still computed relative to that
far-future `ScheduledAt` and is individually bounded regardless of how far out `ScheduledAt` is.

Unlike `ExpiresAt`'s ceiling, though, a `ScheduledAt` that's absurdly far in the future should be
**rejected, not clamped**. The two ceilings look symmetric but carry different risk: clamping an
over-long `ExpiresAt` only trims a safety margin — the task still runs at the time the caller
intended, just with less room to fail and retry. Clamping an over-far `ScheduledAt` would instead
silently change *when the task's real work happens* — and an absurdly-far `ScheduledAt` is far more
likely to be a genuine bug (a millis/nanos unit mixup is the classic case) than a deliberate choice.
Silently running such a task at some clamped date is worse than either running it at the caller's
actual intended time or telling them immediately that the request is out of bounds — so this is the
one place in the design where "reject" beats "clamp."
