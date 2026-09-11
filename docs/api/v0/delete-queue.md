# DeleteQueue

__Irreversibly__ removes a queue with all its tasks and schedules.

The queue itself is removed immediately, freeing its name for reuse right away. Its tasks and
schedules are cleaned up asynchronously in the background afterward, so the call returns quickly
regardless of how many either it had.

## Request

```json
{
  "queue_name": "SquirrelQueue"
}
```

## Response

```json
{}
```
