# CreateSchedule

Creates a schedule for a specified queue.

There is a maximum limit on the number of schedules an account can have, and on the number of schedules per queue, this 
method will return an error if the limit is reached. See [Service Limits](/docs/moab/limits).

## Request

```json
{
  "queue_name": "SquirrelQueue",
  "name": "DailyFeeder",
  "description": "",
  "cron": "",
  "payload": "",
  "dedupe_key": "",
  "expires_in_seconds": 0,
  "keepalive_timeout_in_seconds": 15,
  "retry_strategy": {},
  "timezone": "America/Los_Angeles"
}
```

## Response

```json
{
  "schedule": {
    "name": "DailyFeeder",
    "description": "",
    "queue_name": "SquirrelQueue",
    "created_at": 1695826539671432000,
    "updated_at": 1695826539671432000,
    "version": 1,
    "cron": "",
    "payload": "",
    "dedupe_key": "",
    "expires_in_seconds": 0,
    "keepalive_timeout_in_seconds": 15,
    "retry_strategy": {},
    "timezone": "America/Los_Angeles"
  }
}
```

__CreateScheduleRequest__

| Parameter                    | Type            |                                                                         |
|------------------------------|-----------------|-------------------------------------------------------------------------|
| queue_name                   | String          | Required, max 128 chars, `/[-_0-9a-zA-Z]*/`                             |
| name                         | String          | Required, max 128 chars, `/[-_0-9a-zA-Z]*/`                             |
| description                  | String          | Optional, max 1024 chars, default empty                                 |
| keepalive_timeout_in_seconds | Integer         | Optional, default 0                                                     |
| expires_in_seconds           | Integer         | Optional, default 0                                                     |
| retry_strategy               | RetryStrategy   | Optional, default empty                                                 |
| cron                         | String          | Required, valid cron expression, see [Schedules](/docs/moab/schedules/) |
| timezone                     | String          | Required, valid timezone                                                |
| dedupe_key                   | String          | Optional, max 256 characters, default empty                             |
