# UpdateQueue

Updates the queue. Parameters are pretty much the same as for `CreateQueue` request. The queue will be updated with all
the fields provided in this request as-is, i.e. if some optional fields are omitted from `UpdateQueue` they
will be removed from the queue object. Name cannot be changed.

Returns an updated queue. On each update `updated_at` timestamp gets updated and `version` number is incremented, even 
if there were no effective changes.

## Request

```json
{
  "queue_name": "MyQueue1",
  "description": "Some new description",
  "keepalive_timeout_in_seconds": 30,
  "expires_in_seconds": 86400,
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
  "dead_tasks_set_config": {
    "enable": true,
    "max_size": 0,
    "retention_period_in_seconds": 86400
  }
}
```

## Response

```json
{
  "queue": {
    "name": "MyQueue1",
    "description": "Some new description",
    "created_at": 1695826539671432000,
    "updated_at": 1695826554288946000,
    "version": 2,
    "keepalive_timeout_in_seconds": 30,
    "expires_in_seconds": 86400,
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
    "dead_tasks_set_config": {
      "enable": true,
      "max_size": 0,
      "retention_period_in_seconds": 86400
    }
  }
}
```


__UpdateQueueRequest__

| Parameter                    | Type                  |                                                 |
|------------------------------|-----------------------|-------------------------------------------------|
| queue_name                   | String                | Required, max 128 chars, `/[-_0-9a-zA-Z]*/`     |
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
