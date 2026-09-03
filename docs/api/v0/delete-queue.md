# DeleteQueue

__Irreversibly__ removes a queue with all its tasks and schedules.

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

__DeleteQueueRequest__

| Parameter       | Type                |                                                 |
|-----------------|---------------------|-------------------------------------------------|
| queue_name      | String              | Required, max 128 chars, `/[-_0-9a-zA-Z]*/`     |
