# Dequeue

Picks up tasks from the queue. Only `ENQUEUED` tasks that have `scheduled_at <= now` are ready. If no tasks are
ready then an empty successful response will be returned. `batch_size` defines a maximum number of tasks that will
be picked up at once if that many is ready. Default is `1`.

Optional `keepalive_timeout_in_seconds` overrides the queue's default keepalive timeout for the batch of tasks this
call picks up: how long the caller has to report each task's status (via `ReportStatus`) before Moab reclaims it as
abandoned. 0 (or omitted) inherits the queue's default; a non-zero value must be between [5; 60]. Since it is the
consumer, not the producer, that knows how long it needs to hold a lease, this is set here rather than on `Enqueue`.

__Note:__ This method is eventually consistent. Queues definitions are cached to reduce the load on control plane. So 
any change made by `UpdateQueue`, such as changing dequeuing settings, will be propagated here in about 1 second.

## Request

```json
{
  "queue_name": "SquirrelQueue",
  "batch_size": 10,
  "keepalive_timeout_in_seconds": 30
}
```

## Response

* Returns `NotFound` if the queue does not exist.
* `now` is the server clock (Unix nanoseconds) at the moment this response was produced — use
  it, not your local clock, to compute remaining time against each task's `expires_at`. This is
  the lease handoff to the worker, so getting this right matters.
* Each task's `visible_at` is the keepalive deadline: report the task (via `ReportStatus`) before
  this instant or Moab reclaims it as abandoned. It reflects whichever keepalive timeout applied —
  this request's `keepalive_timeout_in_seconds` override, or the queue's default.

```json
{
  "tasks": [
    {
      "id": "tsk_ISfFsVup2QS",
      "queue_name": "SquirrelQueue",
      "payload": "{\"key\": \"payload\"}",
      "created_at": 1695826539671432000,
      "scheduled_at": 1695826539671432000,
      "expires_at": 1695866639671432000,
      "dedupe_key": "",
      "attempts": 1,
      "visible_at": 1695826669671432000
    }
  ],
  "now": 1695826639671432000
}
```
