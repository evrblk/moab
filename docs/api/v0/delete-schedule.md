# DeleteSchedule

Removes a schedule.

__Note:__ Due to the way Moab scheduler is implemented there might be a few scheduled
tasks already enqueued, and they will not be deleted from the queue when corresponding schedule is deleted.

## Request

```json
{
  "queue_name": "SquirrelQueue",
  "schedule_name": "DailyFeeder"
}
```

## Response

```json
{}
```
