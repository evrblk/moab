# ListTasks

Lists the tasks for a given queue, paginated, optionally filtered to a single state
(`ENQUEUED`, `IN_PROGRESS`, or `DEAD`). Leaving `state` unset lists tasks in every state.

Note that `ENQUEUED` only includes a thread's head (or a non-threaded task) — a non-head
thread member, though genuinely `ENQUEUED`, is never independently dequeued and so is
excluded from this filter (it's still returned by an unfiltered listing).

Since an unfiltered or `ENQUEUED`/`IN_PROGRESS`-filtered listing scrolls through a queue that
may be constantly changing by enqueuing and dequeuing, don't expect a fully consistent
snapshot across pages of a single listing. A `DEAD`-filtered listing (the dead-letter queue)
is more stable, since dead tasks only leave that state via an explicit `RestartTasks`/
`DeleteTasks` call, DLQ retention expiring, or `max_size` eviction.

Read-only and safe to retry.

## Request

* `state` is Optional, one of `TASK_STATE_ENQUEUED`/`TASK_STATE_IN_PROGRESS`/`TASK_STATE_DEAD`.
  When unset lists every state.
* Leave `pagination_token` empty for the first page.
* `limit` sets the number of entries per page.

```json
{
  "queue_name": "SquirrelQueue",
  "state": "TASK_STATE_DEAD",
  "pagination_token": "",
  "limit": 100
}
```

## Response

* Returns `NotFound` if the queue does not exist.

```json
{
  "tasks": [
    {
      "id": "tsk_...",
      "queue_name": "SquirrelQueue",
      "payload": "",
      "created_at": 1695826549671432000,
      "scheduled_at": 1695826549671432000,
      "expires_at": 1695826549671432000,
      "dedupe_key": "",
      "attempts": 3,
      "thread_id": "",
      "state": "TASK_STATE_DEAD"
    }
  ],
  "next_pagination_token": "",
  "previous_pagination_token": ""
}
```
