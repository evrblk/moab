# UpdateSchedule

Updates the schedule. Parameters are pretty much the same as for `CreateSchedule` request. The schedule will be
updated with all the fields provided in this request as-is, i.e. if some optional fields are omitted from
`UpdateSchedule` they will be reset to their defaults. Queue name and schedule name cannot be changed.

Returns an updated schedule. On each update `updated_at` timestamp gets updated and `version` number is incremented,
even if there were no effective changes.

## Request

* `expected_version` is required, must match the schedule's current `version` (optimistic concurrency check).
* For `cron` and `timezone` see [Schedules](/docs/schedules.md)

```json
{
  "queue_name": "SquirrelQueue",
  "schedule_name": "DailyFeeder",
  "description": "Generous feeder, twice a day",
  "cron": "0 8,20 * * *",
  "payload": "{\"amount_in_grams\": 50}",
  "dedupe_key": "",
  "expires_in_seconds": 0,
  "keepalive_timeout_in_seconds": 15,
  "retry_strategy": {
    "retry_intervals_in_seconds": [5, 30, 120]
  },
  "timezone": "America/Los_Angeles",
  "expected_version": 1
}
```

## Response

* Returns `NotFound` if the queue does not exist.
* Returns `NotFound` if the schedule does not exist.
* Returns `InvalidRequest` if `expected_version` does not match the schedule's current version.

```json
{
  "schedule": {
    "name": "DailyFeeder",
    "description": "Generous feeder, twice a day",
    "queue_name": "SquirrelQueue",
    "created_at": 1695826539671432000,
    "updated_at": 1695826554288946000,
    "version": 2,
    "cron": "0 8,20 * * *",
    "payload": "{\"amount_in_grams\": 50}",
    "dedupe_key": "",
    "expires_in_seconds": 0,
    "keepalive_timeout_in_seconds": 15,
    "retry_strategy": {
      "retry_intervals_in_seconds": [5, 30, 120]
    },
    "timezone": "America/Los_Angeles"
  }
}
```
