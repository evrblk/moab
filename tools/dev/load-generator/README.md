# Moab Load Generator

A configurable load testing tool for Moab. It spreads producer and consumer traffic across many
queues, using the real `MoabConsumer` from `evrblk-go/moab/v0` on the consuming side, so it
exercises the same code path applications do.

## Overview

The generator:
- Creates (or reuses) a configurable number of queues
- Runs a configurable number of producer goroutines, each enqueueing batches of tasks against
  randomly chosen queues, with a configurable payload size range and dedupe_key / thread_id
  injection rates
- Runs a configurable number of `MoabConsumer` instances (from `evrblk-go/moab/v0`), spread
  round-robin over the queues, each processing tasks with a configurable simulated handler
  latency
- Prints stats to stdout on a configurable interval, and exposes Prometheus metrics
- Runs for a configurable duration (or indefinitely) and shuts down cleanly, optionally deleting
  the queues it created

## Prerequisites

A running Moab server — single-node or gateway.

## Quick Start

### 1. Start a Moab server

```bash
cd cmd/moab
go run . run single-node --gateway-listen-addr=:8000 --data-dir=./data
```

### 2. Run the load generator

```bash
cd tools/dev/load-generator
go run . --endpoint=localhost:8000 --producers=100 --consumers=100 --queues=200 --duration=30s
```

You should see output like:

```
[00:00:05] Enqueued: 353,701 tasks (64,998 reqs, 0 errors, 70,739 tasks/sec) | Handled: 273,680 tasks (0 errors, 54,735 tasks/sec) | Success: enqueue=100.0% handler=100.0%
```

### 3. View metrics

While the load generator is running, visit:
- Load generator metrics: http://localhost:2114/metrics
- Moab server metrics: http://localhost:2112/metrics

## Configuration

### Connection

- `--endpoint` - Moab server (gateway or single-node) address (default: `localhost:8000`)

### Load Shape

- `--producers` - Number of concurrent producer goroutines (default: `100`)
- `--consumers` - Number of concurrent `MoabConsumer` instances (default: `100`)
- `--queues` - Number of queues to create and spread load across (default: `200`)
- `--duration` - Load test duration (default: `60s`, `0` = infinite)
- `--enqueue-rate` - Target Enqueue RPCs per second across all producers combined (default: `0` =
  unlimited)

Consumers are assigned to queues round-robin (`consumer[i]` → `queues[i % len(queues)]`), so with
more consumers than queues, multiple consumers share a queue; with fewer, some queues go
unconsumed for that run.

### Payload / Batching

- `--payload-size-min` / `--payload-size-max` - Task payload size range in bytes (default: `64` /
  `1024`; capped at Moab's 64KB max payload size)
- `--batch-size-min` / `--batch-size-max` - Number of entries per `Enqueue` call (default: `1` /
  `10`; capped at Moab's max of 50 entries per call)

### Task Shape

- `--dedupe-key-pct` - Percentage of tasks enqueued with a `dedupe_key` (default: `10`)
- `--dedupe-keys-per-queue` - Size of the reused `dedupe_key` pool per queue (default: `1000`).
  Keys are drawn from this bounded pool per queue rather than generated fresh each time, so
  duplicates actually occur and exercise Moab's deduplication path.
- `--thread-id-pct` - Percentage of tasks enqueued with a `thread_id` (default: `10`)
- `--threads-per-queue` - Size of the reused `thread_id` pool per queue (default: `20`), same
  reasoning as `dedupe-keys-per-queue`.

### Consumer Behavior

- `--handler-latency-ms-min` / `--handler-latency-ms-max` - Simulated task handler processing time
  in milliseconds (default: `0` / `50`)

### Queue Creation

- `--queue-name-prefix` - Prefix for created queue names (default: `load-test-queue`)
- `--queue-keepalive-timeout` - Queue `keepalive_timeout_in_seconds` (default: `15`)
- `--queue-expires-in-seconds` - Queue `expires_in_seconds` (default: `86400`)
- `--queue-setup-concurrency` - Number of concurrent `GetOrCreateQueue` / `DeleteQueue` calls
  during setup and cleanup (default: `32`)

### General

- `--prometheus-listen-addr` - Prometheus metrics bind address (default: `:2114`)
- `--log-interval` - Stats logging interval (default: `5s`)
- `--cleanup` - Delete created queues on shutdown (default: `true`)

## Workload Scenarios

### 1. Small local smoke test

```bash
go run . --endpoint=localhost:8000 --producers=10 --consumers=10 --queues=5 --duration=15s
```

### 2. Hundreds of queues and producers/consumers

```bash
go run . \
  --endpoint=localhost:8000 \
  --producers=300 --consumers=300 --queues=300 \
  --duration=5m
```

### 3. Heavy deduplication and ordering

```bash
go run . \
  --producers=100 --consumers=100 --queues=50 \
  --dedupe-key-pct=80 --dedupe-keys-per-queue=200 \
  --thread-id-pct=80 --threads-per-queue=10 \
  --duration=2m
```

### 4. Rate-limited soak test

```bash
go run . \
  --producers=50 --consumers=50 --queues=100 \
  --enqueue-rate=5000 \
  --duration=1h --log-interval=30s
```

### 5. Large payloads

```bash
go run . \
  --payload-size-min=32768 --payload-size-max=65536 \
  --batch-size-min=1 --batch-size-max=3 \
  --duration=1m
```

## Metrics

### Prometheus Metrics

Available at `http://localhost:2114/metrics` (or `--prometheus-listen-addr`):

- `load_generator_enqueue_requests_total{status}` - Enqueue RPCs by status
- `load_generator_enqueue_duration_seconds` - Enqueue RPC latency histogram
- `load_generator_tasks_enqueued_total` - Tasks actually enqueued (excludes entries skipped by
  server-side deduplication)
- `load_generator_enqueue_errors_total{error_type}` - Enqueue errors by type
- `load_generator_tasks_handled_total{status}` - Tasks handled by consumers
- `load_generator_handler_duration_seconds` - Simulated handler processing time histogram
- `load_generator_active_producers` / `load_generator_active_consumers` - Active worker counts
- `load_generator_queues` - Number of queues in use for this run

### Console Output

Real-time stats are printed every `--log-interval`:

```
[00:05:00] Enqueued: 939,602 tasks (176,135 reqs, 4 errors, 57,887 tasks/sec) | Handled: 769,870 tasks (0 errors, 50,187 tasks/sec) | Success: enqueue=100.0% handler=100.0%
```

## Cleanup

By default the load generator deletes all queues it created on shutdown (`--cleanup=true`). To
preserve them for inspection:

```bash
go run . --cleanup=false
```

Queues created: `{queue-name-prefix}-{index}` for `index` in `[0, queues)`. Queue setup is
idempotent — an existing queue with the same name is reused rather than recreated, so re-running
against the same server with `--cleanup=false` is safe.

## Architecture

- **main.go** - Entry point, orchestration, signal handling
- **config.go** - Configuration and validation
- **pacer.go** - Simple fixed-interval rate limiter used for `--enqueue-rate`
- **producer.go** - Producer goroutine: builds and sends `Enqueue` batches
- **consumer.go** - Wires a `MoabConsumer` (from `evrblk-go/moab/v0`) to a simulated task handler
- **queues.go** - Queue setup (get-or-create) and cleanup, fanned out concurrently
- **stats.go** - Statistics collection and console reporting
- **metrics.go** - Prometheus metrics
