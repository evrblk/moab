# CreateSchedule

Creates a schedule for a specified queue.

There is a maximum limit on the number of schedules an account can have, and on the number of schedules per queue, this 
method will return an error if the limit is reached.

## Request

* For `cron` and `timezone` see [Schedules](/docs/schedules.md)

```json
{
  "queue_name": "SquirrelQueue",
  "name": "DailyFeeder",
  "description": "",
  "cron": "0 0,30 * * * *",
  "payload": "",
  "dedupe_key": "",
  "expires_in_seconds": 0,
  "keepalive_timeout_in_seconds": 15,
  "retry_strategy": {},
  "timezone": "America/Los_Angeles"
}
```

## Response

* Returns `NotFound` if the queue does not exist.
* Returns `AlreadyExists` if a schedule with the same name exists in the queue.
* Returns `ResourceExhausted` if the queue has reached its schedule quota.

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
