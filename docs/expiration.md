# Expiration

A task will expire if it has not been dequeued before its expiration time. Expiration time can be set with optional
parameter `expires_at` when enqueuing (`expires_at` is defined as a timestamp in the future, not a delta). By default,
all tasks will expire in `expires_in_seconds` retention period specified on the queue itself. An expired task will never
be returned from `Dequeue` method.

Internally, all tasks are also indexed by `expires_at` (Expiration index). Pictured below, a task `A` is sheduled at `t1` \
and will expire in `e` seconds (queue retention period) at moment `t1 + e`:

![Expiration](/docs/images/expiration-1.png)

When a task is enqueued it is added to Expiration index on a default or specified expiration time, even if a task
is delayed.

![Expiration](/docs/images/expiration-2.png)

Expiration happens under the hood, tasks will be garbage collected when `expires_at <= now`.

![Expiration](/docs/images/expiration-3.png)

[//]: # (TODO expiration and retries)
