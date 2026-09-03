# Enqueue

Puts a batch of tasks into the queue.

Optional parameters `keepalive_timeout_in_seconds`, `expires_at`, and `retry_strategy` override corresponding values from 
the queue. That means each task can have individual retry strategy within the queue.

A task can be scheduled into the future by specifying `scheduled_at` timestamp (Unix time in nanoseconds). If it is set
to 0 it will default to `now` and the task will be available for dequeing immediately.

Unique tasks are deduplicated by optional `dedupe_key`. If it is set then Moab will make sure there is only a single copy
of a task with a given key in the queue. `overwrite_on_duplicate` specifies what to do with the duplicate. If it is 
empty, then the duplicate will be simply skipped. Fields listed in `overwrite_on_duplicate` will be overwritten on the
original task with new values from the duplicate task. `overwrite_on_duplicate` can be set only if `dedupe_key` is set.

__Note:__ This method is eventually consistent. Queues definitions are cached to reduce the load on control plane. So any 
change made by `UpdateQueue`, such as changing default keepalive timeout, will be reflected here after about 10 seconds.

Response is a list of tasks actually enqueued or modified. Each task has an id which can be used to track task status
(with `GetTask`) or to delete a task before it is dequeued. Tasks without `dedupe_key` will be enqueued and returned in 
the response. Deduplicated (without `overwrite_on_duplicate`) tasks will be skipped and not returned in the response. 
Deduplicated (with some `overwrite_on_duplicate` set) tasks will be modified and returned in the response.

## Request

```json
{
  "entries": [
    {
      "payload": "{\"user_id\": 123, \"event_type\": \"updated\"}",
      "scheduled_at": 0,
      "expires_at": 0,
      "dedupe_key": "unique_key_123",
      "keepalive_timeout_in_seconds": 60,
      "retry_strategy": {
      },
      "overwrite_on_duplicate": []
    }
  ],
  "queue_name": "MyQueue1"
}
```

## Response

```json
{
  "tasks": [
    {
      "id": "tsk_ISfFsVup2QS",
      "queue_name": "MyQueue1",
      "payload": "{\"user_id\": 123, \"event_type\": \"updated\"}",
      "created_at": 1695826539671432000,
      "scheduled_at": 1695826539671432000,
      "expires_at": 1695866639671432000,
      "dedupe_key": "unique_key_123",
      "attempts": 1
    }
  ]
}
```

__EnqueueRequest__

| Parameter  | Type                  |                                             |
|------------|-----------------------|---------------------------------------------|
| queue_name | String                | Required, max 128 chars, `/[-_0-9a-zA-Z]*/` |
| entries    | EnqueueRequestEntry[] | Required, between [1; 10] entries           |
 
__EnqueueRequestEntry__

| Parameter                    | Type                 |                                             |
|------------------------------|----------------------|---------------------------------------------|
| payload                      | String               | Optional, max 64kb                          |
| scheduled_at                 | Integer              | Optional, default 0                         |
| expires_at                   | Integer              | Optional, default 0                         |
| dedupe_key                   | String               | Optional, max 256 characters, default empty |
| keepalive_timeout_in_seconds | Integer              | Optional, default 0                         |
| retry_strategy               | RetryStrategy        | Optional                                    |
| overwrite_on_duplicate       | OverwriteOnDuplicate |                                             |
