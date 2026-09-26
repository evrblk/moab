# ReportStatus

Reports the outcome of one or more `IN_PROGRESS` tasks that a worker previously picked up with `Dequeue`.

* `SUCCEEDED` removes the task.
* `FAILED` moves the task back to `ENQUEUED` (or to `DEAD` once its `retry_strategy` is exhausted), incrementing
  `attempts`. See [Retries](/docs/retries.md).
* `IN_PROGRESS` renews the task's keepalive lease without completing it, useful for long-running work.

Each entry is fenced by `attempt`, which must match the task's current `attempts` counter. If a keepalive timeout has
already caused the task to be reclaimed (and possibly picked up by another worker), a report against the stale
`attempt` is not applied — see the response below, which reports exactly this per entry rather than staying silent
about it.

## Request

* `entries[].attempt` must match the task's current `attempts` counter (from `Dequeue`/`GetTask`), otherwise the
  entry is silently ignored.
* `entries[].status` is one of `STATUS_SUCCEEDED`, `STATUS_IN_PROGRESS`, `STATUS_FAILED`.
* `entries[].keepalive_timeout_in_seconds` is meaningful only when `status` is `STATUS_IN_PROGRESS`: it overrides the
  queue's default keepalive timeout, extending the lease by this many seconds from now. 0 (or omitted) inherits the
  queue's default; a non-zero value must be between [5; 60]. Like `Dequeue`'s field of the same name, this is the
  consumer's own call — it, not the server, knows how long it needs before its next heartbeat. Ignored for other
  statuses.

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
    },
    {
      "task_id": "tsk_ISfFsVup2QS",
      "attempt": 1,
      "status": "STATUS_IN_PROGRESS",
      "keepalive_timeout_in_seconds": 30
    }
  ]
}
```

## Response

* Returns `NotFound` if the queue does not exist.
* `entries` has the same length and order as the request's `entries`, one-to-one. Each `result` is one of:
  * `RESULT_OK` — the status was applied: the task deleted (`STATUS_SUCCEEDED`), retried or dead-lettered
    (`STATUS_FAILED`), or its lease renewed (`STATUS_IN_PROGRESS`).
  * `RESULT_NOT_FOUND` — no task exists under this id (never did, or an earlier report/reclaim already deleted it).
  * `RESULT_NOT_IN_PROGRESS` — the task exists but isn't `IN_PROGRESS` — already completed, dead-lettered, or
    re-enqueued by the time this report arrived.
  * `RESULT_STALE_ATTEMPT` — `attempt` didn't match the task's current `attempts`: its lease already lapsed and was
    reclaimed (and possibly redelivered) before this report arrived.

```json
{
  "entries": [
    {
      "task_id": "tsk_CQenhoeqha2IS5J5CqFlrbSnabvVSkfWH",
      "result": "RESULT_OK"
    },
    {
      "task_id": "tsk_xjmxhujMkxWEBcNWk7e8M5qgKkoY2kVaB",
      "result": "RESULT_STALE_ATTEMPT"
    },
    {
      "task_id": "tsk_ISfFsVup2QS",
      "result": "RESULT_OK"
    }
  ]
}
```
