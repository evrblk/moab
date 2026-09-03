# Moab Architecture Notes

> Internal reference for working in this repo. Optimized for fast re-orientation, not external docs.

## What it is

A distributed task-queue service, built on the **Monstera** framework (sharded, Raft-replicated
state machines over embedded BadgerDB) — closer in spirit to SQS + EventBridge Scheduler / Cloud
Tasks than to a workflow engine: it moves and paces individual tasks, it does not orchestrate
multi-step business processes.

Core concepts (see `docs/concepts.md`, `docs/retries.md`, `docs/threads.md`, `docs/dequeuing-settings.md`):

- **Queue** — durable, named, per-account. Owns defaults: keepalive timeout, retry strategy,
  dequeuing settings (`max_in_progress_tasks` concurrency cap + token-bucket `rate_limiting` +
  `dequeuing_paused`), dead-letter-queue config, `expires_in_seconds` default.
- **Task** — a unit of work with an opaque `payload` (deliberately unstructured; e2e-encryptable).
  States: `ENQUEUED` → (dequeued) `IN_PROGRESS`, leased for `keepalive_timeout_in_seconds` →
  `SUCCEEDED` (deleted) or `FAILED` → retried back to `ENQUEUED` (per `RetryStrategy`) or `DEAD`
  once retries are exhausted. Every task has an absolute `ExpiresAt`; a GC worker sweeps it
  regardless of state once that passes, independent of the retry/DLQ path.
- **Schedule** — a cron expression (7-segment, via `gronx`) attached to a queue; a background
  worker enqueues one task per due tick, reusing the queue's retry/keepalive/expiration settings
  by default.
- **Thread** (`thread_id`) — FIFO sub-ordering within a queue: only the earliest task of a thread
  (its "head") is ever dequeued at a time; the rest sit invisible (`ENQUEUED` but not in
  the dequeue index) until the head succeeds or dies, at which point the next-earliest is promoted.
- **Dedupe key** (`dedupe_key`) — on re-`Enqueue`, if the key already points at a live (`ENQUEUED`)
  task, `overwrite_on_duplicate` selectively updates that task's payload/scheduled_at/expires_at
  instead of creating a new one. Queue-lifetime dedup (bounded by the task still being `ENQUEUED`),
  not a fixed time window like SQS FIFO.

## Two cores, two shard spaces — the thing to keep in your head

Unlike a single-cluster-app design, Moab is **two independently-sharded Monstera applications**:
`MoabQueues` (queues + schedules, sharded by `sharding.ByAccount`) and `MoabTasks` (tasks +
threads + rate limiters, sharded by `sharding.ByAccountAndQueue`). A queue and its own tasks are
looked up via different shard keys and can live on different nodes/shards. **There is no
cross-core transaction.** Anything that touches both — `Enqueue` resolving a queue's settings
before writing tasks, the cron worker enqueuing a due schedule's tasks — is two separate RPCs
issued from a layer above both cores (the gRPC handler, or a worker); a core never calls another
core directly.

## Layered request flow

```
gRPC client (evrblk-go moabpb v0)
   │
   ▼
pkg/server/v0/server.go       MoabApiServer — implements moabpb.MoabApiServer
   │  Validate<Method>(req) → InvalidArgument on failure; every call hardcodes accountId=0 and
   │  moab.DefaultServiceLimits (see Single-tenant note); a few methods bump prometheus counters
   │  (tasksEnqueuedTotal/tasksDequeuedTotal, hand-instrumented, unrelated to MonitoringMiddleware)
   ▼
pkg/server/v0/handler.go      MoabApiServerHandler
   │  - resolves QueueName → *Queue via s.getQueue (10s TTL cache, keyed accountId/queueName)
   │  - generates a random QueueId/ScheduleId at this layer, retries on IDCollision (up to
   │    maxIDGenerationAttempts=5)
   │  - fills in per-entry defaults from the queue (keepalive timeout, retry strategy, expires_at
   │    capped at the queue's own default)
   │  - decodes public TaskId strings (pkg/ids), converts front pb ⇄ core pb (pbconv.go),
   │    pagination tokens (base64 ⇄ core)
   ▼
pkg/coreapis  MoabClientApi (interface)     ← the seam between front-end/workers and the two cores
   │  Two implementations (both generated):
   │   • MoabMonsteraStub        — cluster mode: marshals → monsteraClient.Read/Update(appName,
   │                                shardKey, bytes); appName is "MoabQueues" or "MoabTasks"
   │   • MoabNonclusteredStub    — single-node/tests: routes by shardKey to an in-process core
   │                                slice (shard count must be a power of two — panics if not)
   ▼
pkg/queues.Core  /  pkg/tasks.Core          the two state machines, one per shard, implementing
   │                                        MoabQueuesCoreApi / MoabTasksCoreApi
   │  pure functions over a BadgerDB txn; no time/network of their own — `Now` is passed in on
   │  every request, never read from the clock inside a core
   ▼
honey.BinaryTable / OneToManySortedIndex / SortedIndex / Uint64Table (yellowstone-common/honey)
   ▼
BadgerDB
```

Same determinism rule as any Monstera core: never call `time.Now()` inside one. Errors are either
a Go `error` (infrastructure failure, bubbles up, `mrpc.ErrorToGRPC` at the gRPC boundary) or an
`ApplicationError` (`*mrpc.Error`, e.g. `NotFound`/`AlreadyExists`/`ResourceExhausted`/
`IDCollision`, carried in the response payload rather than as a Go error).

## Package map (`pkg/`)

| Package | Role |
|---|---|
| `corepb/` | Core protobuf types + vtproto (`*.proto` → `*.pb.go` + `*_vtproto.pb.go`). Hand-written `sharding.go` (a `ShardKey()` method per request type, delegating to `pkg/sharding`) and `marshal_gen.go` (genmarshal-generated `MarshalBinary`/`UnmarshalBinary`). |
| `coreapis/` | **Generated** by `monstera code generate` (`make generate`). `api.go` (typed request/response aliases + `Moab{Queues,Tasks}CoreApi` interfaces + `MoabClientApi`), `adapters.go` (Monstera `ApplicationCore` adapters), `stubs.go` (`MoabMonsteraStub` + `MoabNonclusteredStub` + core factories). DO NOT EDIT. |
| `queues/` | `MoabQueues` core. `core.go` (`Core` + all RPC impls: Create/Get/List/Update/DeleteQueue, Create/Get/List/Update/DeleteSchedule, DequeSchedules, ReportSchedulesStatus, RunQueuesGarbageCollection), `queues.go` (`queuesTable`: primary + names index), `schedules.go` (`schedulesTable`: primary + names index + due-time index), `counters.go` (per-account `NumberOfQueues`), `gc_records.go` (async schedule cleanup after `DeleteQueue`), `table_prefixes.go`. |
| `tasks/` | `MoabTasks` core — the largest, most load-bearing file in the repo is `core.go`. `tasks.go` (`tasksTable`: primary + dedupeKeysIndex + queueIndex + inProgressIndex + deadTasksIndex + enqueuedIndex), `threads.go` (`threadsTable`: head pointer + threadedTasksIndex), `expiration.go` (shard-local `expirationIndex`, the GC sweep target — every task lives here regardless of state), `rate_limiters.go` (token-bucket state per queue), `queue_state.go` (task id sequence + dequeue scan cursors `LastVisitedEnqueued`/`LastVisitedInProgress`), `counters.go` (per-`(account,queue)` `EnqueuedTasksCount`/`InProgressTasksCount`/`DeadTasksCount`), `table_prefixes.go`. |
| `sharding/` | `ByAccount(accountId)`, `ByAccountAndQueue(accountId, queueId)` → truncated-hash `cluster.ShardKey`. `MoabQueues` cores use the former, `MoabTasks` cores the latter — this is the split described above. |
| `ids/` | Public string IDs ⇄ core pb IDs. base62-encoded, type-prefixed: `que_` (accountId+queueId), `schdl_` (accountId+queueId+scheduleId), `tsk_` (**just** the raw task id — no account/queue baked in; that context comes from the queue name already present in the surrounding request). |
| `pagination/` | `GetLimitWithDefaults` (default 100, max 250), `CoreToMonstera`/`MonsteraToCore` (honey `PaginationToken` ⇄ corepb `PaginationToken`). |
| `moab/` | `limits.go` — `ServiceLimits` struct + `DefaultServiceLimits`. Only `MaxNumberOfQueues`, `MaxNumberOfSchedulesPerQueue`, `MaxEnqueueBatchSize`, `MaxDequeueBatchSize` are actually enforced anywhere; the other six fields (all rate-limit fields, plus the account-wide `MaxNumberOfSchedules`) are declared but never read outside this file. |
| `payloads/` | Empty — reserved, currently unused. |
| `server/v0/` | gRPC front-end. `server.go` (validate+dispatch, hardcodes `accountId=0`), `handler.go` (orchestration, `getQueue` cache), `validators.go` (`Validate*Request`), `pbconv.go` (`*ToFront`/`*ToCore`), `middleware.go` (`AuthenticationMiddleware` — verifies `evrblk-go/authn` request signatures, key type dispatched on `key_alfa_`/`key_bravo_` id prefix; `MonitoringMiddleware` — defined but never registered on any server, see Known gaps), `metrics.go`. |
| `workers/` | Three `yellowstone-common/workers.IntervalWorker`s, 5s poll each, each fanning out over `ListShards(appName)`: `MoabQueuesCronWorker` (`DequeSchedules` → expand due ticks via `gronx`-based `nextTick` → `Enqueue` into `MoabTasks` → `ReportSchedulesStatus` to advance `NextScheduledAt`), `MoabQueuesGCWorker` (`RunQueuesGarbageCollection` — drains `DeleteQueue`'s async schedule-cleanup records), `MoabTasksGCWorker` (`RunTasksGarbageCollection` — sweeps the tasks expiration index). |

## Single-tenant (OSS) vs multi-tenant (cloud)

Every RPC in `server.go` hardcodes `accountId = 0` and `moab.DefaultServiceLimits`.
`AuthenticationMiddleware` verifies a signed request but never resolves the key to an account id
or a per-account `ServiceLimits` and threads it through. Treat this as deliberate, intentional OSS
behavior, not a bug to flag — a closed-source build is expected to reuse `handler.go` and the two 
cores as-is and swap in its own front server that derives `accountId`/limits from the authenticated
key.

## cmd / run modes (`cmd/moab`)

`moab run <mode>` (cobra; `root.go` → `run.go` → subcommands):

- **single-node** (`single_node.go`): one shared `BadgerStore`; `honey.ReplicaPrefixRegistry`
  assigns each of `--shards` (default 64, **must be a power of two — panics otherwise**) internal
  shards a node-local replica prefix; `MoabNonclusteredStub` over both cores' factories; gRPC
  server + all 3 workers in one process. Dev/test shape.
- **node** (`node.go`): a stateful Monstera node. Registers `MoabQueues` and `MoabTasks` as
  `CoreTypePersistedExclusive` application descriptors, each core wrapped in its generated adapter.
  Raft-replicated, sharded per the cluster's config. Binds via `--listen host:port`.
- **gateway** (`gateway.go`): stateless. Discovers cluster config via exactly one of
  `--monstera-nodes`/`-file`/`-srv` (`discovery.go`), `MoabMonsteraStub` → gRPC server. Binds via
  `--port` (all interfaces) — a different convention from `node`'s `--listen`.
- **worker** (`worker.go`): stateless. Same discovery/client wiring as `gateway`, runs the 3
  workers only, no gRPC server of its own.

## Core conventions & invariants

- **Constructor**: `NewCore(badgerStore, replicaPrefix, shardLowerBound, shardUpperBound)` — same
  shape in both `queues.Core` and `tasks.Core`. `replicaPrefix` (2 bytes, handed out by
  `honey.ReplicaPrefixRegistry`, one per shard/replica, persisted+idempotent) namespaces this
  core's entire keyspace in the shared store; `shardLowerBound`/`shardUpperBound` matter only for
  `Restore`'s ownership check, not for key layout.
- `var _ coreapis.Moab<X>CoreApi = &Core{}` compile-time assertion at the top of each `core.go`.
- **Snapshot/Restore** go through `honey.Section`/`PortableTable` (`Clear`/`EachEntity`/
  `RestoreEntity` implemented per table) — a shared "portable snapshot" abstraction
  (`yellowstone-common/honey`): any core of an application can restore any other core's snapshot,
  keeping only the entities whose shard key falls inside its own bounds. Only primary entities
  are streamed; every secondary index is rebuilt from scratch by re-inserting through the table's
  normal write path.
- **Transactions**: `badgerStore.View()`/`Update()`, always `defer txn.Discard()`, one
  `txn.Commit()` at the end. One Badger transaction per RPC — no cross-RPC transactions.
- **Thread invariant**: only a thread's current head is ever in `queueIndex` (i.e. ever
  dequeued/in-progress). Non-head tasks live only in `threadedTasksIndex`. `promoteNextThreadHead`
  (called from `deleteTask`/`failTaskToDead` whenever the head departs) finds the earliest
  remaining task and is the *only* path that adds a task to `queueIndex` on a thread's behalf
  outside of `createTask`. **This is the invariant the `overwriteDuplicate` TODO bug breaks** (see
  Known gaps #1) — re-check it first in any change that touches threads + dedupe + overwrite
  together. Because `queueIndex` deliberately excludes non-head members, it cannot answer "what's
  the oldest `ENQUEUED` task overall" — that's what `enqueuedIndex` is for: every `ENQUEUED` task,
  head or not, is added to it exactly where it's added to (or would be added to, if non-threaded)
  `queueIndex`, and removed exactly where it leaves `ENQUEUED` state. `getAgeOfOldestEnqueuedTask`
  reads only from `enqueuedIndex`.
- **Dequeue** is two passes per call inside one transaction, each bounded by
  `maxVisitedTasksForDequeuing` (100): `dequeueInProgressTasksBeforeTime` first (re-dequeue tasks
  whose keepalive lapsed, retry or kill them), then `dequeueTasksBeforeTime` (pull fresh `ENQUEUED`
  tasks) if budget remains — sharing one `dequeueLimit`, reduced up front by the token-bucket rate
  limiter when `DequeuingSettings.RateLimiting` is set. Both use a `queueState` cursor
  (`LastVisitedInProgress`/`LastVisitedEnqueued`) so a call resumes scanning where the last one left
  off instead of always rescanning from the front of the index.
- **Expiration is GC-swept, not lazy-filtered on read**: every task, in every state, sits in one
  shard-local `expirationIndex`; only `RunTasksGarbageCollection` actually deletes past-`ExpiresAt`
  tasks, in bounded pages. The one lazy-filter exception is `GetTask`: a task whose `ExpiresAt` has
  passed but hasn't been swept yet is reported as `NotFound`.
- **Counters** (`tasks/counters.go`, `queues/counters.go`) are hand-maintained on every state
  transition, never recomputed from a scan. Easy to drift when adding a new transition — most of
  the Critical/High findings in `notes/moab/` are exactly this class of bug (an index or counter
  update missing on one specific path).

## Tables / keyspace

- Table prefixes are 1 byte, scoped **within one core's `replicaPrefix`** — every Moab core (queues
  or tasks, on any shard) reuses the same small prefix set, because the 2-byte `replicaPrefix`
  already makes different cores'/shards' data collision-free in the shared store.
- Every sort-key/index item is plain big-endian `utils.ConcatBytes(timestamp, ...ids)`, which only
  sorts correctly for **non-negative** timestamps (two's-complement makes negatives sort last).
  `queueIndexItem`/`inProgressIndexItem`/`deadTasksIndexItem`/`threadedTasksIndexItem`/the
  expiration index's item all document this inline. In practice it's always safe:
  `ScheduledAt`/`VisibleAt`/`ExpiresAt` are always `max(requested, now)` or `now + positive
  duration`, never a zero/negative sentinel — but it's a landmine for any future field that isn't.

## Known live bugs & gaps — skim before touching `pkg/tasks`, `pkg/server/v0`, or `pkg/workers`

1. **`overwriteDuplicate`'s `OVERWRITE_ON_DUPLICATE_SCHEDULED_AT` case (`tasks/core.go`, marked
   `// TODO overwrite scheduled at on threaded tasks`) corrupts `queueIndex`/`threadedTasksIndex`
   for a non-head threaded duplicate** — makes it independently dequeueable (breaking the
   thread-head invariant above) and permanently orphans an index entry. Not yet fixed.
2. **`ReportStatus` with an unset/invalid `Status` silently no-ops**: `validators.go` never checks
   it, `pbconv.go` maps it to `0`/no-error, `reportStatus`'s switch has no `default` case. Returns
   200 OK while the task's real state is never touched — the most dangerous silent-failure mode in
   the API, since it's the primary way a worker reports outcomes.
3. **Dequeue's rate-limiter and `MaxInProgressTasks` cap are checked *after* the first candidate is
   already processed**: both `dequeueInProgressTasksBeforeTime` and `dequeueTasksBeforeTime` can
   hand out one extra task per call, indefinitely, once already sitting at the limit/zero tokens.
4. `handler.go`'s `getQueue` 10s TTL cache is never invalidated by
   CreateQueue/UpdateQueue/DeleteQueue — a fast delete+recreate of the same queue name can route
   `Enqueue`/`Dequeue` traffic at the stale, now-deleted `QueueId` for up to 10 more seconds.
5. All three `pkg/workers` use `context.TODO()` for every RPC, and `IntervalWorker.Stop()` neither
   cancels an in-flight tick nor waits for it — SIGTERM mid-tick can exit before a schedule's
   `NextScheduledAt` advances, so the cron worker re-derives and re-enqueues the same due ticks on
   restart.
6. No panic-recovery interceptor exists anywhere in the gRPC stack. `MonitoringMiddleware` is
   defined (`metrics.go`'s `totalRequestsCounter`/`failedRequestsCounter`/`requestsDuration`) but
   never added to `unaryInterceptors` in `gateway.go`/`single_node.go` — those three metrics are
   permanently zero.

## Testing conventions

- `testify/require`, table-driven where it fits. `pkg/queues`/`pkg/tasks` core tests build a `Core`
  over `store.NewBadgerInMemoryStore()` directly (no gRPC, no Monstera client).
- `pkg/sharding`/`pkg/ids` are solidly tested (a golden fixture, and a 10,000-iteration round-trip
  test respectively). `pkg/queues`'s table tests were recently filled in (`queues_test.go`/
  `schedules_test.go` were one-line placeholders before). `pkg/workers`, `pkg/moab`, and
  `pkg/pagination` (`helpers_test.go` is still a placeholder) have little to no coverage.
- The systematic blind spot in `pkg/tasks/core_test.go`: every dequeue-limit test starts from an
  empty/fresh state and dequeues once; nothing exercises "already at the cap / bucket already at
  zero tokens before this call" — exactly the shape that hides Known-gaps bug #3.

## Conventions cheat-sheet

- Errors: infra failure → Go `error` (bubbles up, `mrpc.ErrorToGRPC` at the gRPC boundary). Domain
  failure → `*mrpc.Error`/`ApplicationError` (`mrpc.NewErrorWithContext(code, msg, ctxMap)`) in the
  response payload. `nilifyIfEmpty` (generated stub code) turns an empty/OK error into `nil`.
- Validation lives only in `server/v0/validators.go` (the front edge); cores assume validated input
  but still enforce limits/existence/state-transition rules themselves.
- `Now` is always `time.Now().UnixNano()` (int64 nanos); durations in the wire API are seconds,
  converted in cores as `now + seconds*int64(time.Second)`.
- IDs: queue/schedule IDs are randomly generated **at the handler layer** with collision-retry
  (`maxIDGenerationAttempts = 5`, detected via `mrpc.IDCollision`); task IDs are a **deterministic
  per-queue sequence** (`queueState.TaskIdSequence`) generated **inside the Tasks core itself**,
  since task creation always happens within one single-shard `Enqueue` call and doesn't need
  collision avoidance.
- `make build` / `go test ./... -race` / `make lint` (fmt, vet, staticcheck, govulncheck) /
  `make moab` (binary) / `make moab-image` (docker) / `make generate` (protoc + `monstera code
  generate` + genmarshal) / `make format` (gofmt -s + goimports).

## Docs & tooling

- `moab/docs/` is product-facing (`concepts.md`, `retries.md`, `threads.md`, `expiration.md`,
  `dequeuing-settings.md`, `unique-tasks.md`, `schedules.md`, `docs/api/v0/<method>.md` per RPC).
  Several are still literal `__TODO__` stubs (`retries.md`, `threads.md`, `report-status.md`,
  `get-task.md`'s examples) — per `notes/moab/industry-comparison.md`, the two most operationally
  important docs (retry/failure-path semantics) are the least finished part of the product surface.
- `notes/moab/` is **not** product docs — it's a standing, periodically-revalidated code-review
  record (bugs/gaps per subsystem, plus a product-level comparison against SQS/Cloud
  Tasks/Sidekiq/Celery/BullMQ/RabbitMQ/Temporal in `industry-comparison.md`). Check it before
  deep-diving into `pkg/tasks`, `pkg/server/v0`, or `pkg/workers` — it's very likely faster than
  re-deriving the same findings from scratch.
- `moab-enterprise/tasksinmem/` is a sibling module's alternate, ephemeral (no persistence, pure Go
  maps/btrees) implementation of `coreapis.MoabTasksCoreApi` — a second engine choice pluggable at
  the `cmd/moab` factory-wiring layer alongside the Badger-backed `pkg/tasks.Core`. It does not
  support threads (`thread_id` is accepted but ignored). Explicitly out of scope for the
  `notes/moab` reviews.
