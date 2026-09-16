# Unique tasks

Tasks can be deduplicated by an optional string `dedupe_key`. If a key is set Moab will ensure that at most one task
in `ENQUEUED` or `IN_PROGRESS` states with such a key can be present in the queue at any given time. Unique (with 
non-empty `dedupe_key`) and regular (with empty `dedupe_key`) tasks can be placed into the same queue.

On __Fig. A__ below, task `C` will be successfully enqueued because such a key does not exist yet. On __Fig. B__, task
`C` will be skipped, because such a key exists. __TODO__ update response and return flags for skipped tasks.

![Unique tasks](/docs/images/unique-1.png)

However, it is possible to overwrite one or more properties when a duplicate is being enqueued. Moab can overwrite
`payload`, `scheduled_at`, and `expires_at`. Figure below shows how a payload overwrite looks like.

![Unique tasks](/docs/images/unique-2.png)

Next figure shows how `scheduled_at` can be overwritten. This works for both situations when a task is enqueued at `now`
or into the future. This is handy when something can trigger a delayed job several times, but instead of enqueuing
the same job several times it can be rescheduled instead.

![Unique tasks](/docs/images/unique-3.png)

Dedupe keys guard live uniqueness, not historical identity. A `DedupeKey` is claimed by
at most one live (`ENQUEUED` or `IN_PROGRESS`) task at a time. It is released the instant a task
stops being live — on success, on delete, and on death — so a fresh task can reuse the key
immediately after a failure. Because of that, any *out-of-band* resurrection of an old task under
that key (i.e. `RestartTasks`) must re-validate the key against current state rather than assume
it's still free — restart is not exempt from the live-uniqueness guarantee just because it's an
explicit operator action.
