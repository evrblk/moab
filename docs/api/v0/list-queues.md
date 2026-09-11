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
      "name": "MyQueue1",
      "description": "",
      "created_at": 1695826539671432000,
      "updated_at": 1695826539723719100,
      "version": 5,
      "keepalive_timeout_in_seconds": 15,
      "retry_strategy": {
      },
      "dequeuing_strategy": {},
      "dead_letter_queue_config": {
        "enable": true,
        "max_size": 0,
        "retention_period_in_seconds": 86400
      }
    }
  ]
}
```
