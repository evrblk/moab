# Dequeue

Picks up tasks from the queue. Only `ENQUEUED` tasks that have `scheduled_at <= now` are ready. If no tasks are
ready then an empty successful response will be returned. `batch_size` defines a maximum number of tasks that will
be picked up at once if that many is ready. Default is `1`.

__Note:__ This method is eventually consistent. Queues definitions are cached to reduce the load on control plane. So 
any change made by `UpdateQueue`, such as changing dequeuing settings, will be propagated here in about 1 second.

## Request

```json
{
  "queue_name": "SquirrelQueue",
  "batch_size": 10
}
```

## Response

* Returns `NotFound` if the queue does not exist.
* `now` is the server clock (Unix nanoseconds) at the moment this response was produced — use
  it, not your local clock, to compute remaining time against each task's `expires_at`. This is
  the lease handoff to the worker, so getting this right matters.

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
      "attempts": 1
    }
  ],
  "now": 1695826639671432000
}
```
