# Retries

If a worker crashes or is not able to report the status on time for any reason, a task will be moved to `ENQUEUED` state,
removed from Inflight index, and added to the main index, so that it can be dequeued again. Counter `attempts`
will be incremented on the task.

![Retries](/docs/images/retries-1.png)

If a worker reports `FAILED` status... __TODO__

The same applies to optional `retry_strategy` in `Enqueue` request. This is a per-task
override of the `retry_strategy` set on the queue level.











A failed task can be retried according to `retry_strategy`. __TODO__. When all retry attempts are exhausted a task moves
to `DEAD` state. Dead tasks can be persisted in Dead Letter Queue which is configured with `dead_letter_queue_config` at the
queue level. Dead tasks can be inspected with `GetTask` and __TODO list dts__ RPC and retried again with `RestartTasks`
RPC. While reporting `FAILED` status a worker can add an arbitrary string `debug_info` (such as a stacktrace) which can
be helpful for an inspection later. If no Dead Letter Queue is configured all dead tasks will be immediately deleted.
