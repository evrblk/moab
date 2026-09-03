# CreateQueue

Creates a queue.

Queue names must be unique within a given account and cannot be changed later. Valid names can contain alphanumeric 
characters, hyphens and underscores.

There is a maximum limit on the number of queues an account can have, this method will return an error if the limit is
reached. See [Service Limits](/docs/moab/limits).

## Request

```json
{
  "name": "SquirrelQueue",
  "description": "Async squirrel feeder",
  "keepalive_timeout_in_seconds": 15,
  "expires_in_seconds": 0,
  "retry_strategy": {
  },
  "dequeuing_settings": {
    "max_inflight_tasks": 0,
    "rate_limiting": { // 1000 tasks per second
      "max_tokens": 1000,
      "interval": 1,
      "interval_unit": "SECONDS"
    },
    "dequeuing_paused": false
  },
  "dead_letter_queue_config": {
    "enable": true,
    "max_size": 0, // unlimited size
    "retention_period_in_seconds": 86400 // 1 day (in seconds)
  }
}
```

## Response

```json
{
  "queue": {
    "name": "SquirrelQueue",
    "description": "Async squirrel feeder",
    "created_at": 1695826539671432000,
    "updated_at": 1695826539671432000,
    "version": 1,
    "keepalive_timeout_in_seconds": 15,
    "expires_in_seconds": 1209600, // default 14 days
    "retry_strategy": {
    },
    "dequeuing_settings": {
      "max_inflight_tasks": 0,
      "rate_limiting": {
        "max_tokens": 1000,
        "interval": 1,
        "interval_unit": "SECONDS"
      },
      "dequeuing_paused": false
    },
    "dead_letter_queue_config": {
      "enable": true,
      "max_size": 0,
      "retention_period_in_seconds": 86400
    }
  }
}
```

__CreateQueueRequest__

| Parameter                    | Type                  |                                                 |
|------------------------------|-----------------------|-------------------------------------------------|
| name                         | String                | Required, max 128 chars, `/[-_0-9a-zA-Z]*/`     |
| description                  | String                | Optional, max 1024 chars, default empty         |
| keepalive_timeout_in_seconds | Integer               | Required, between [5; 60]                       |
| expires_in_seconds           | Integer               | Optional, between [5; 1209600], default 1209600 |
| retry_strategy               | RetryStrategy         | Optional, default no retries                    |
| dequeuing_settings           | DequeuingSettings     | Optional                                        |
| dead_letter_queue_config     | DeadLetterQueueConfig | Optional                                        |

__RetryStrategy__

| Parameter                    | Type               |                                         |
|------------------------------|--------------------|-----------------------------------------|
| name                         | String             | Required, max 128 chars                 |
| description                  | String             | Optional, max 1024 chars, default empty |

__DequeuingSettings__

| Parameter            | Type                     |                             |
|----------------------|--------------------------|-----------------------------|
| max_inflight_tasks   | Integer                  | Optional, default unlimited |
| rate_limiting        | TokenBucketRateLimiting  | Optional, default empty     |
| dequeuing_paused     | Boolean                  | Optional, default false     |

__TokenBucketRateLimiting__

| Parameter            | Type      |                                           |
|----------------------|-----------|-------------------------------------------|
| max_tokens           | Integer   | Required                                  |
| interval             | Integer   | Required                                  |
| interval_unit        | Enum      | Required, [`SECONDS`, `MINUTES`, `HOURS`] |


__DeadLetterQueueConfig__

| Parameter                    | Type               |                                                   |
|------------------------------|--------------------|---------------------------------------------------|
| enable                       | Boolean            | Optional, default false                           |
| max_size                     | Integer            | Optional, default 0 (unlimited)                   |
| retention_period_in_seconds  | Integer            | Optional, between [5; 1209600], default 1209600   |
