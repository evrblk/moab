# Retries

If a worker crashes or is not able to report the status on time for any reason, a task will be moved to `ENQUEUED` state,
removed from InProgress index, and added to the main index, so that it can be dequeued again. Counter `attempts`
will be incremented on the task. The same happens when a worker explicitly reports `FAILED` status.

![Retries](/docs/images/retries-1.png)

A failed task can be retried according to `retry_strategy`. When all retry attempts are exhausted a task moves
to `DEAD` state. Dead tasks can be persisted in Dead Letter Queue which is configured with `dead_letter_queue_config` at the
queue level. Dead tasks can be inspected with `GetTask` and `ListTasks` RPC and retried again with `RestartTasks`
RPC. While reporting `FAILED` status a worker can add an arbitrary string `debug_info` (such as a stacktrace) which can
be helpful for an inspection later. If no Dead Letter Queue is configured all dead tasks will be immediately deleted.

A restart is a birth, not a resume. Restarting a `DEAD` task produces exactly the state a fresh `Enqueue` of the same 
payload would: `Attempts` resets to 0, it competes for its thread's head exactly like a new arrival, and it gets a brand 
new forward-looking `ExpiresAt` computed the same way a fresh enqueue's is (`now + ExpiresInSeconds`) — never the stale 
original absolute deadline, and never an extension of the DLQ retention deadline it had while dead.

Failure handling is unified and fenced. There is exactly one failure-handling function, invoked either by an explicit
`ReportStatus(FAILED)` or by keepalive-timeout discovery treating a silently-abandoned lease as an implicit failure.
Both apply the *same* `RetryStrategy` backoff, the *same* expiry check (Law 3), and the *same* thread reconciliation.
Every `ReportStatus` call (`SUCCEEDED`/`FAILED`/`IN_PROGRESS` alike) is fenced by `entry.Attempt == task.Attempts`; a
mismatch means a stale worker is reporting against an attempt that's no longer current, and it must be a silent no-op.
