# PurgeQueue

Removes all the tasks from a queue. __TODO only enqueued?__

## Request

```json
{
  "queue_name": "SquirrelQueue"
}
```

## Response

```json
{}
```

__PurgeQueueRequest__

| Parameter       | Type                |                                                 |
|-----------------|---------------------|-------------------------------------------------|
| queue_name      | String              | Required, max 128 chars, `/[-_0-9a-zA-Z]*/`     |
