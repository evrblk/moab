# GetTask

Returns a task by id. There is no `ListTasks` RPC because scrolling through a giant queue which is constantly changing
by enqueuing and dequeuing does not provide any good user experience. Therefore, the only way to have an id for a
task to feed into `GetTask` is to memorize the id after enqueuing.

## Request

```json
{
}
```

## Response

```json
{
}
```
