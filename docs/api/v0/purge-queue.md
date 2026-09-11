# PurgeQueue

Removes all the tasks from a queue regardless of their state. The action is quick since it only marks the queue for
garbage collection and the async GC worker actually cleans it up. There is no conflict between newly added tasks right
after purging and old tasks that being cleaned by GC.

## Request

```json
{
  "queue_name": "SquirrelQueue"
}
```

## Response

* Returns `NotFound` if the queue does not exist.

```json
{}
```
