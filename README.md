# Everblack Moab

[![Go](https://github.com/evrblk/moab/actions/workflows/go.yml/badge.svg)](https://github.com/evrblk/moab/actions/workflows/go.yml)

**Everblack Moab** — a durable, distributed feature-rich task queue, served over a clean API, in 
a single self-contained binary.

## Why Moab

* **Durability**. All enqueued tasks are persisted and replicated. A task can be removed only by an explicit
  confirmation from a worker that a task was successfully completed. It scales horizontally as your workload grows.
* **Pull-based model**. Workers periodically pull (dequeue) tasks from queues, allowing simple scaling of the
  workers fleet. Workers are stateless. No poison pills ever.
* **Automatic retries**. Failed tasks are automatically retried. Even in the event of worker crash. Retry
  strategy can be configured for a queue or for each individual task. That guarantees at-least-once execution.
* **Scheduled Tasks**. Moab allows you to schedule the time (delay) when a task will be executed.
* **Tasks Expiration**. Tasks that are set to expire can run as long as they want, but an expiring task must be 
  picked up before the expiration time. Expiration time can be set on each task individually.
* **Unique Tasks**. Moab makes sure there is only a single copy of a task with a given key in the queue. You can 
  choose to leave the original task or rewrite it (if payload or scheduled time is different).
* **Long running tasks**. Tasks can run for hours or days as long as workers keep reporting them in-progress.
* **Built-in Cron**. Schedule periodic tasks with built-in Cron.
* **Pause a queue**. You can pause dequeuing from a queue with a single command. For example, in an emergency scenario.
* **Rate Limiter**. Moab has a built-in rate limiter that works across all workers. Rate limiter is implemented with
  Token Bucket algorithm.
* **Concurrency Limiter**. Regardless of how many workers are pulling tasks from a queue Moab can also limit the number
  of in-progress tasks.
* **Zero dependencies.** One binary, embedded storage — no database, Kafka, Redis, or ZooKeeper to
  run alongside it. Start single-node, grow into a replicated, sharded cluster without changing your code.

Go to [documentation](/docs/concepts.md) to learn more.

## Installing

Build Moab from sources:

```shell
# Checkout source code
$ git clone git@github.com:evrblk/moab.git
$ cd moab

# Build
$ make build

# Produce ./cmd/moab/moab executable
$ make moab
```

## Running

### Single-node mode

```shell
$ ./cmd/moab/moab run single-node --gateway-listen-addr=:8000 --data-dir=./data
```

### Clustered mode

TODO

## Using

Use with `evrblk` CLI tool from [github.com/evrblk/evrblk-cli](https://github.com/evrblk/evrblk-cli).

Example:

```shell
$ evrblk moab-v0 list-queues --endpoint=localhost:8000
{}

$ echo '{"name": "MyQueue1", "keepalive_timeout_in_seconds": 15, "expires_in_seconds": 86400}' | evrblk moab-v0 create-queue --endpoint=localhost:8000
{
  "queue": {
    "name": "MyQueue1",
    "createdAt": "1789157059707484000",
    "updatedAt": "1789157059707484000",
    "version": "1",
    "keepaliveTimeoutInSeconds": "15",
    "expiresInSeconds": "86400"
  }
}

$ echo '{"queue_name": "MyQueue1", "entries": [{"payload": "dGVzdCBwYXlsYW9k"}]}' | evrblk moab-v0 enqueue --endpoint=localhost:8000
{
  "tasks": [
    {
      "id": "tsk_BAAAAAAAAAA",
      "payload": "dGVzdCBwYXlsYW9k",
      "createdAt": "1789157183849019000",
      "scheduledAt": "1789157183849019000",
      "expiresAt": "1789243583848992000",
      "state": "TASK_STATE_ENQUEUED"
    }
  ]
}
```

Or use with official Everblack SDKs:
* [github.com/evrblk/evrblk-go](https://github.com/evrblk/evrblk-go) for Go
* [github.com/evrblk/evrblk-ruby](https://github.com/evrblk/evrblk-ruby) for Ruby

Example in Go:

```go
import (
    evrblk "github.com/evrblk/evrblk-go"
    moab "github.com/evrblk/evrblk-go/moab/v0"
)

moabClient, err := moab.NewMoabGrpcClient("localhost:8000", evrblk.NewNoOpSigner())

createQueueResp, err = moabClient.CreateQueue(context.Background(), &moab.CreateQueueRequest{
	Name:                      "MyQueue1",
	KeepaliveTimeoutInSeconds: 15,
	ExpiresInSeconds:          86400,
})

enqueueResp, err := moabClient.Enqueue(context.Background(), &moab.EnqueueRequest{
	QueueName: "MyQueue1",
	Entries: []*moab.EnqueueRequestEntry{
		{
			Payload: []byte(`{"user_id": 123, "event_type": "updated"}`),
		},
	},
})

dequeueResp, err := moabClient.Dequeue(context.Background(), &moab.DequeueRequest{
	QueueName: "MyQueue1",
	BatchSize: 10,
})
```

See [docs/api/v0](/docs/api/v0) for the full request/response reference of every RPC.

## Authentication

By default, API calls are unauthenticated. To use request signing add `--auth-keys-path=` argument to
`./moab run gateway` or `./moab run single-node`. It should point to a directory with API keys where
each file name is an API key ID, and corresponding file content is an API secret key.

Generate keys with `evrblk` [CLI tool](https://github.com/evrblk/evrblk-cli):

```shell
$ evrblk authn generate-alfa-key
```

Read [Authentication](https://everblack.dev/docs/api/authentication/) documentation to learn more
about how it works and how to generate keys.

## Project Status

Moab is being actively developed. Public API version is `v0` - expect breaking changes before it
stabilizes. Disk-level compatibility can also be broken at this stage.

## Contributing

Ways to contribute:

- Bug reports: Use the [GitHub issue tracker](https://github.com/evrblk/moab/issues/new).
- Feature requests: Use the [GitHub issue tracker](https://github.com/evrblk/moab/issues/new).
- Code changes: Submit a pull request.
- Documentation: Submit a pull request.

Before you contribute:

- Check [open issues](https://github.com/evrblk/moab/issues) and
  [pull requests](https://github.com/evrblk/moab/pulls) to avoid duplicating work.
- See [AGENTS.md](/AGENTS.md) for build/test commands and code style guidelines.

## License

Everblack Moab is released under [AGPL-3 License](https://opensource.org/license/agpl-v3).
