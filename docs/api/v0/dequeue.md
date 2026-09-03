# Dequeue

Picks up tasks from the queue. Only `ENQUEUED` tasks that have `scheduled_at <= now` are ready. If no tasks are
ready then an empty successful response will be returned. `batch_size` defines a maximum number of tasks that will
be picked up at once if that many is ready. Default is `1`.

__Note:__ This method is eventually consistent. Queues definitions are cached to reduce the load on control plane. So 
any change made by `UpdateQueue`, such as changing dequeuing settings, will be propagated here in about 10 seconds.    

## Request

```json
{
  "queue_name": "SquirrelQueue",
  "batch_size": 10
}
```

## Response

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
  ]
}
```

__DequeueRequest__

| Parameter        | Type        |                                             |
|------------------|-------------|---------------------------------------------|
| queue_name       | String      | Required, max 128 chars, `/[-_0-9a-zA-Z]*/` |
| batch_size       | Integer     | Optional, between [1; 10], default 1        |
