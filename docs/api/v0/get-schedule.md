# GetSchedule

Returns a schedule by name for a given queue.

Read-only and safe to retry.

## Request

```json
{
  "queue_name": "SquirrelQueue",
  "schedule_name": "DailyFeeder"
}
```

## Response

* Returns `NotFound` if the queue does not exist.

```json
{
  "schedule": {
    "name": "DailyFeeder",
    "description": "",
    "queue_name": "SquirrelQueue",
    "created_at": 1695826539671432000,
    "updated_at": 1695826539671432000,
    "version": 1,
    "cron": "0 0,30 * * * *",
    "payload": "",
    "dedupe_key": "",
    "expires_in_seconds": 0,
    "keepalive_timeout_in_seconds": 15,
    "retry_strategy": {},
    "timezone": "America/Los_Angeles"
  }
}
```
