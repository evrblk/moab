# DeleteSchedule

Removes a schedule. Please keep in mind that due to the way Moab scheduler is implemented there might be a few scheduled
tasks already enqueued, and they will not be deleted from the queue when corresponding schedule is deleted. This usually
happens for

## Request

```json
{
  "schedule_id": "schdl_nNreHT35hyTfFJqxqrouYHQipj82dJuqB"
}
```

## Response

```json
{}
```
