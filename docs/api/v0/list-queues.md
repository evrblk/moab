# ListQueues

Lists all the queues in the account. Unlike `GetQueue`, it does not return additional statistics.

Read-only and safe to retry.

## Request

* Leave `pagination_token` empty for the first page.
* `limit` sets the number of entries per page.

```json
{
  "pagination_token": "",
  "limit": 100
}
```

## Response

```json
{
  "queues": [
    {
      "name": "SquirrelQueue",
      "description": "Async squirrel feeder",
      "created_at": 1695826539671432000,
      "updated_at": 1695826539723719100,
      "version": 5,
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
  ],
  "next_pagination_token": "",
  "previous_pagination_token": ""
}
```
