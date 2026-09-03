# Dequeuing Settings

Normally, workers would dequeue a batch of tasks from the queue, process them, report as completed (or failed), then repeat 
on the next batch. The more workers you have, the more tasks you can process at once. There are no technical limits on the 
maximum number of inflight tasks. Also, workers dequeue tasks as fast as they can process them. However, this is not always
desirable. Moab allows you to limit the rate of dequeuing and the number of inflight tasks if needed. 

## Concurrency Limiting (number of inflight tasks)

Sometimes it is necessary to limit the number of concurrently processing tasks because of, for example, technical
limitations on a number of connections to an underlying database or contractual limitations on a 3rd party integration.
This can be achieved by setting `dequeuing_settings.max_inflight_tasks` parameter at the queue level. Default `0`
means unlimited. Moab will ensure that at most `max_inflight_tasks` tasks are in `INFLIGHT` state at any given moment in
time. Please keep in mind that it does not mean that all those tasks are actively being processed by workers, since any
worker can die and some tasks will remain in `INFLIGHT` state until `keepalive_timeout_in_seconds` expires. When the limit
is reached, `Dequeue` returns a successful empty response.

The mechanism for limiting the number of inflight tasks is implemented inside the Moab queue on the server side and the
number is maintained regardless of how many workers are pulling tasks from the queue simultaneously.

## Rate Limiting

Rate of dequeuing can be limited with Token Bucket algorithm. `dequeuing_settings.rate_limiting.max_tokens` is the size
of the bucket. `dequeuing_settings.rate_limiting.interval` and `dequeuing_settings.rate_limiting.interval_unit` specify 
the interval at which the bucket is fully refilled. Units can be `SECONDS`, `MINUTES` or `HOURS`. Here are some examples:

```json
// 1k tasks per second
{
  "max_tokens": 1000,
  "interval": 1,
  "interval_unit": "SECONDS"
}

// 2k tasks per 1 minute
{
  "max_tokens": 2000,
  "interval": 1,
  "interval_unit": "MINUTES"
}

// 15 tasks per hour
{
  "max_tokens": 15,
  "interval": 1,
  "interval_unit": "HOURS"
}
```

It is possible to spend all tokens at once. Consider the difference between _1 task per second_ and _10 tasks per 10 
seconds_. Both buckets are refilled at the same rate (1 token per second), but when buckets are full the latter can 
release more tokens at once.

If both are configured, rate limiting works together with `max_inflight_tasks` as expected: `Dequeue` returns a
successful empty response if either of limits is reached.

The mechanism for rate limiting is also implemented inside the Moab queue on the server side and the rate is spread
between all the workers which are pulling tasks from the queue. The distribution of the rate between workers is
non-deterministic, only the total rate is guaranteed to be limited.  

## Pause

Entire dequeuing can be paused simply by setting `dequeuing_settings.dequeuing_paused` to true. Any worker who pulls a 
paused queue will receive a successful empty response. This can be used by an operator in case of emergency when a queue 
has tasks in it, but there is a known issue that prevents those tasks from being completed successfully (for example, a 
dependency is temporarily unavailable). Returning a successful empty response mitigates any panic behavior from workers. 
It looks just like an empty queue for them, no errors, no need to retry requests.  

Please note that `Dequeue` method is eventually consistent. Queues definitions are cached to reduce the load on control 
plane. So any change made by `UpdateQueue`, such as changing dequeuing settings, will be propagated here in about 10 seconds.
