# GetQueue

Returns a queue by name along with its current statistics.

Read-only and safe to retry.

## Request

```json
{
  "queue_name": "SquirrelQueue"
}
```

## Response

* Returns `NotFound` if the queue does not exist.
* `now` is the server clock (Unix nanoseconds) at the moment `stats` was collected — a
  snapshot, not a stored attribute of the queue.

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
  },
  "stats": {
    "enqueued_tasks_count": 15230, // number of tasks waiting to be picked up
    "in_progress_tasks_count": 10, // number of tasks currenly in-progress
    "dead_tasks_count": 15, // number of tasks ever died during the lifetime of this queue
    "processed_tasks_count": 1832043, // number of tasks ever processed through this queue
    "expired_tasks_count": 5, // number of tasks that were removed as expired
    // The oldest task out of this 15230 has been ready to be picked up for 16.5 seconds (in nanoseconds),
    // but it is still in the queue.
    "age_of_oldest_enqueued_task": 16498185433
  },
  "now": 1695826839671432000
}
```
