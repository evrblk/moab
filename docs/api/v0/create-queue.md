# CreateQueue

Creates a queue.

Queue names must be unique within a given account and cannot be changed later. Valid names can contain alphanumeric 
characters, hyphens and underscores.

There is a maximum limit on the number of queues an account can have, this method will return an error if the limit is
reached.

## Request

* `keepalive_timeout_in_seconds` is required, between [5; 60]
* `expires_in_seconds` is optional, between [5; 1209600], default 1209600 (14 days)
* `dead_letter_queue_config.max_size` is optional, default 0 (unlimited)
* `dead_letter_queue_config.retention_period_in_seconds` is optional, between [5; 1209600], default 1209600 (14 days)

```json
{
  "name": "SquirrelQueue",
  "description": "Async squirrel feeder",
  "keepalive_timeout_in_seconds": 15,
  "expires_in_seconds": 0,
  "retry_strategy": {
  },
  "dequeuing_settings": {
    "max_in_progress_tasks": 0,
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

* Returns `AlreadyExists` if a queue with the same name exists in the account.
* Returns `ResourceExhausted` if the account has reached its queue quota.

```json
{
  "queue": {
    "name": "SquirrelQueue",
    "description": "Async squirrel feeder",
    "created_at": 1695826539671432000,
    "updated_at": 1695826539671432000,
    "version": 1,
    "keepalive_timeout_in_seconds": 15,
    "expires_in_seconds": 1209600,
    "retry_strategy": {
    },
    "dequeuing_settings": {
      "max_in_progress_tasks": 0,
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
