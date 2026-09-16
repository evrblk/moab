# Threads

A **Thread** is a mutual-exclusion group: at most one of its member tasks is ever `IN_PROGRESS` at a time, and
the member that becomes visible next (the **head**) is always the one with the earliest `scheduled_at` among 
members not currently being worked. Tasks are groupped into a thread per `thread_id`. A thread exists only while 
it has at least one member. It is deleted as soon as its last member leaves. Threaded tasks can be enqueued into
same queue as non-threaded tasks.

### What happens when the head fails?

Neither, cleanly — a thread has exactly one member being worked at a time, and it stays that way
whether the head succeeds, retries, or dies:

* **Head fails and has retries left**: it re-enters `ENQUEUED` at `now + backoff` and
  re-competes for headship, exactly like any other member. If a sibling now has an
  earlier `ScheduledAt`, that sibling becomes head and gets dequeued next — the failed task is
  *not* evicted, it's just temporarily out-prioritized, and it remains eligible to become head
  again (repeatedly, across further retries) until it resolves. At every instant, at most one
  member is `IN_PROGRESS`, so the "single-flight" guarantee never breaks; what breaks is any
  assumption that the thread is strict FIFO-by-arrival — it's priority-by-`ScheduledAt`,
  which is the same rule that already lets a fresh `Enqueue` displace a not-yet-dequeued head today.
  If a caller wants pure FIFO-by-arrival, they simply never set `ScheduledAt` explicitly — ties
  break by `TaskId`, which is arrival order.
* **Head fails and is retry-exhausted**: it dies (subject to DLQ routing) and is detached
  from the thread *permanently*, and the thread
  advances to the next-earliest remaining member (or is deleted if none remain). A dead task never
  blocks its thread again unless explicitly restarted, at which point it's a brand-new arrival
  competing like anything else.
* **Head is deleted explicitly** (`DeleteTasks`) while `IN_PROGRESS`: treated identically to
  death — detach and promote immediately, synchronously, without waiting for the keepalive timeout
  to lapse. Deletion is authoritative; the thread doesn't wait around for a worker that's about to
  be told its work no longer exists.
