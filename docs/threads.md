# Threads

A **Thread** is a mutual-exclusion group: at most one of its member tasks is ever `IN_PROGRESS` at a time, and
the member that becomes visible next is always the one with the earliest `scheduled_at` among members not currently
being worked. Tasks are groupped into a thread per `thread_id`. A thread exists only while it has at least one member.
It is deleted as soon as its last member leaves. Threaded tasks can be enqueued into same queue as non-threaded tasks.





### Threads: does the thread advance or stop when the head fails?

Neither, cleanly — a thread has exactly one member being worked at a time, and it stays that way
whether the head succeeds, retries, or dies:

- **Head fails and has retries left**: it re-enters `ENQUEUED` at `now + backoff[Attempts-1]` and
  re-competes for headship under Law 1, exactly like any other member. If a sibling now has an
  earlier `ScheduledAt`, that sibling becomes head and gets dequeued next — the failed task is
  *not* evicted, it's just temporarily out-prioritized, and it remains eligible to become head
  again (repeatedly, across further retries) until it resolves. At every instant, at most one
  member is `IN_PROGRESS`, so the "single-flight" guarantee never breaks; what breaks is any
  assumption that the thread is strict FIFO-by-arrival — it's priority-by-`ScheduledAt` (Law 1),
  which is the same rule that already lets a fresh `Enqueue` displace a not-yet-dequeued head today.
  If a caller wants pure FIFO-by-arrival, they simply never set `ScheduledAt` explicitly — ties
  break by `TaskId`, which is arrival order.
- **Head fails and is retry-exhausted**: it dies (subject to DLQ routing, below) and is detached
  from the thread *permanently* — `threadedTasksIndex`'s entry for it is removed, and the thread
  advances to the next-earliest remaining member (or is deleted if none remain). A dead task never
  blocks its thread again unless explicitly restarted, at which point it's a brand-new arrival
  competing under Law 1/5 like anything else.
- **Head is deleted explicitly** (`DeleteTasks`) while `IN_PROGRESS`: treated identically to
  death — detach and promote immediately, synchronously, without waiting for the keepalive timeout
  to lapse. Deletion is authoritative; the thread doesn't wait around for a worker that's about to
  be told its work no longer exists.

So: **"does a retried threaded task go to the end of the thread?"** — not by rule, only by
consequence. It's rescheduled to `now + backoff`, and in the common case where siblings were
enqueued with default (immediate) `ScheduledAt`, that backoff will usually push it behind
everything currently waiting — which *looks* like "goes to the end," but is really just Law 1
applied uniformly. If a sibling was itself scheduled far in the future, a short backoff can still
land the retried task ahead of it. There is no special "retry" case in the ordering function.
