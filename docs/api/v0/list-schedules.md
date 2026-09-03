# ListSchedules

Lists the schedules for a given queue, paginated.

## Request

```json
{
  "queue_name": "SquirrelQueue"
}
```

## Response

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

__ListSchedulesRequest__

| Parameter        | Type                |                                                 |
|------------------|---------------------|-------------------------------------------------|
| queue_name       | String              | Required, max 128 chars, `/[-_0-9a-zA-Z]*/`     |
| pagination_token | String              | Optional, base64-encoded token from a previous response |
| limit            | Int32               | Optional, max page size, defaults to 100, capped at 250 |
