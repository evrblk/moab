# ListSchedules

Lists the schedules for a given queue, paginated.

Read-only and safe to retry.

## Request

* Leave `pagination_token` empty for the first page.
* `limit` sets the number of entries per page.

```json
{
  "queue_name": "SquirrelQueue",
  "pagination_token": "",
  "limit": 100
}
```

## Response

* Returns `NotFound` if the queue does not exist.

```json
{
  "schedules": [
    {
      "name": "MySchedule1",
      "description": "Generous feeder",
      "queue_name": "SquirrelQueue",
      "created_at": 1695826549671432000,
      "updated_at": 1695826549671432000,
      "version": 1,
      "cron": "*/5 * * * *",
      "payload": "",
      "dedupe_key": "",
      "expires_in_seconds": 0,
      "keepalive_timeout_in_seconds": 0,
      "retry_strategy": {
      },
      "timezone": "America/Los_Angeles"
    }
  ],
  "next_pagination_token": "",
  "previous_pagination_token": ""
}
```
