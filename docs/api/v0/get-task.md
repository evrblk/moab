# GetTask

Returns a task by id. Works for a task in any state (`ENQUEUED`, `IN_PROGRESS`, or `DEAD`).

Read-only and safe to retry.

## Request

```json
{
  "queue_name": "SquirrelQueue",
  "task_id": "tsk_CQenhoeqha2IS5J5CqFlrbSnabvVSkfWH"
}
```

## Response

* Returns `NotFound` if the queue or the task does not exist.
* `now` is the server clock (Unix nanoseconds) at the moment this response was produced — use
  it, not your local clock, to compute remaining time against `task.scheduled_at`/
  `task.expires_at`.

```json
{
  "task": {
    "id": "tsk_CQenhoeqha2IS5J5CqFlrbSnabvVSkfWH",
    "queue_name": "SquirrelQueue",
    "payload": "{\"key\": \"payload\"}",
    "created_at": 1695826539671432000,
    "scheduled_at": 1695826539671432000,
    "expires_at": 1695866639671432000,
    "dedupe_key": "",
    "attempts": 1,
    "thread_id": "",
    "state": "TASK_STATE_ENQUEUED"
  },
  "now": 1695827039671432000
}
```
