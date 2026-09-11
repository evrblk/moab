# Expiration

A task will expire if it has not been dequeued before its expiration time. Expiration time can be set with optional
parameter `expires_at` when enqueuing (`expires_at` is defined as a timestamp in the future, not a delta). By default,
all tasks will expire in `expires_in_seconds` retention period specified on the queue itself. 

`ExpiresAt` is evaluated at two moment: (a) in `Dequeue` just before an `ENQUEUED` task would be handed to a worker,
and (b) just before a task that just stopped being actively worked (explicit failure, or keepalive-timeout discovery)
would be handed back to `ENQUEUED` or filed to `DEAD`. It is never evaluated while a task is genuinely `IN_PROGRESS`
and being renewed, and it never overrides an explicit `SUCCEEDED`. A background GC sweep additionally reaps `ENQUEUED`
and `DEAD` rows nobody is actively polling — but it must never touch `IN_PROGRESS` rows.

Internally, all tasks are indexed by `expires_at` (Expiration index). Pictured below, a task `A` is sheduled at `t1` \
and will expire in `e` seconds (queue retention period) at moment `t1 + e`:

![Expiration](/docs/images/expiration-1.png)

When a task is enqueued it is added to Expiration index on a default or specified expiration time, even if a task
is delayed.

![Expiration](/docs/images/expiration-2.png)

Expiration happens under the hood, tasks will be garbage collected when `expires_at <= now`.

![Expiration](/docs/images/expiration-3.png)

[//]: # (TODO expiration and retries)


There is no unlimited expiration: every task expires, and every DLQ entry expires.
* `queue.expires_in_seconds` is required and bounded on `CreateQueue`/`UpdateQueue`. It cannot be 0, and it cannot be
  unbounded — "long" is fine (weeks), "forever" is not.
* Whenever `dead_letter_queue_config` is configured, `retention_period_in_seconds` is required and bounded the same
  way. If a caller wants a very long retention window, they ask for one explicitly, within the same ceiling.
* A per-task override of either value (`enqueue_request_entry.expires_at`, and the `expires_at` a `RestartTasks` call
  supplies) may be `0`, but `0` means "inherit the queue's default," never "no deadline." A non-zero override that
  would exceed the queue's own ceiling is clamped down to
  that ceiling, not rejected — consistent with how `ScheduledAt` already clamps up to `now` rather
  than rejecting a past-scheduled entry (`pkg/tasks/core.go`'s `Enqueue`).

The one deliberately unbounded knob left is `DeadLetterQueueConfig.max_size` (`0` = unlimited
*count*) — a different axis (how many dead rows to keep) from expiration (how long to keep any one
of them), and out of scope for this law.
