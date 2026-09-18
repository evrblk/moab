# RestartTasks

Restarts `DEAD` tasks. A restart is a birth, not a resume: it produces exactly the state a
fresh `Enqueue` of the same payload would — `attempts` resets to 0, the task competes for its
thread's head like a new arrival, and it gets a brand-new forward-looking `expires_at` (never
the stale original deadline, or an extension of the retention deadline it had while dead). See
[Retries](/docs/retries.md).

Only `DEAD` tasks can be restarted. A restart also re-validates `dedupe_key` against current
state — the live-uniqueness guarantee applies to restarts too. See [Unique tasks](/docs/unique-tasks.md).

## Request

* `entries[].scheduled_at` set to `0` defaults to now (mirrors `Enqueue`'s `scheduled_at`).
* `entries[].expires_at` set to `0` defaults to the queue's `expires_in_seconds` after
  `scheduled_at` (mirrors `Enqueue`'s `expires_at`).

```json
{
  "queue_name": "SquirrelQueue",
  "entries": [
    {
      "task_id": "tsk_CQenhoeqha2IS5J5CqFlrbSnabvVSkfWH",
      "scheduled_at": 0,
      "expires_at": 0
    },
    {
      "task_id": "tsk_xjmxhujMkxWEBcNWk7e8M5qgKkoY2kVaB",
      "scheduled_at": 0,
      "expires_at": 0
    }
  ]
}
```

## Response

* Returns `NotFound` if the queue does not exist.
* `entries[].result` is one of `RESULT_RESTARTED`, `RESULT_NOT_FOUND`, `RESULT_NOT_DEAD` (the
  task exists but isn't `DEAD`), or `RESULT_DEDUPE_CONFLICT` (another live task already holds
  this task's `dedupe_key`). `entries[].task` is set only when `result` is `RESULT_RESTARTED`.
* `now` is the server clock (Unix nanoseconds) at the moment this response was produced — use
  it, not your local clock, to compute remaining time against a restarted task's
  `scheduled_at`/`expires_at`.

```json
{
  "entries": [
    {
      "task_id": "tsk_CQenhoeqha2IS5J5CqFlrbSnabvVSkfWH",
      "result": "RESULT_RESTARTED",
      "task": {
        "id": "tsk_CQenhoeqha2IS5J5CqFlrbSnabvVSkfWH",
        "queue_name": "SquirrelQueue",
        "payload": "{\"key\": \"payload\"}",
        "created_at": 1695826539671432000,
        "scheduled_at": 1695826839671432000,
        "expires_at": 1695866939671432000,
        "dedupe_key": "",
        "attempts": 0,
        "thread_id": "",
        "state": "TASK_STATE_ENQUEUED"
      }
    },
    {
      "task_id": "tsk_xjmxhujMkxWEBcNWk7e8M5qgKkoY2kVaB",
      "result": "RESULT_NOT_FOUND"
    }
  ],
  "now": 1695826839671432000
}
```
