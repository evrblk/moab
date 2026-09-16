# ReportStatus

Reports the outcome of one or more `IN_PROGRESS` tasks that a worker previously picked up with `Dequeue`.

* `SUCCEEDED` removes the task.
* `FAILED` moves the task back to `ENQUEUED` (or to `DEAD` once its `retry_strategy` is exhausted), incrementing
  `attempts`. See [Retries](/docs/retries.md).
* `IN_PROGRESS` renews the task's keepalive lease without completing it, useful for long-running work.

Each entry is fenced by `attempt`, which must match the task's current `attempts` counter. If a keepalive timeout has
already caused the task to be reclaimed (and possibly picked up by another worker), a report against the stale
`attempt` is silently ignored.

## Request

* `entries[].attempt` must match the task's current `attempts` counter (from `Dequeue`/`GetTask`), otherwise the
  entry is silently ignored.
* `entries[].status` is one of `STATUS_SUCCEEDED`, `STATUS_IN_PROGRESS`, `STATUS_FAILED`.

```json
{
  "queue_name": "SquirrelQueue",
  "entries": [
    {
      "task_id": "tsk_CQenhoeqha2IS5J5CqFlrbSnabvVSkfWH",
      "attempt": 1,
      "status": "STATUS_SUCCEEDED"
    },
    {
      "task_id": "tsk_xjmxhujMkxWEBcNWk7e8M5qgKkoY2kVaB",
      "attempt": 2,
      "status": "STATUS_FAILED"
    }
  ]
}
```

## Response

* Returns `NotFound` if the queue does not exist.

```json
{}
```
