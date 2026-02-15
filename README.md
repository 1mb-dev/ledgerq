# LedgerQ

[![CI](https://github.com/1mb-dev/ledgerq/actions/workflows/ci.yml/badge.svg)](https://github.com/1mb-dev/ledgerq/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Go Reference](https://pkg.go.dev/badge/github.com/1mb-dev/ledgerq.svg)](https://pkg.go.dev/github.com/1mb-dev/ledgerq/pkg/ledgerq)
[![Feature Complete](https://img.shields.io/badge/status-feature--complete-success.svg)](https://github.com/1mb-dev/ledgerq/releases/tag/v1.4.0)
[![Go Version](https://img.shields.io/badge/go-%3E%3D1.23-blue.svg)](https://golang.org/dl/)

A production-ready, disk-backed message queue for single-node applications - written in pure Go with zero dependencies.

**Feature-complete as of v1.4.0** - LedgerQ focuses on being excellent at local message queuing rather than trying to replace distributed systems like Kafka.

## Features

- Crash-safe durability with append-only log design
- Zero dependencies beyond the Go standard library
- High throughput with batch operations
- **Priority queue support (v1.1.0+)** with starvation prevention
- **Dead Letter Queue (DLQ) support (v1.2.0+)** for failed message handling
- **Payload compression (v1.3.0+)** with GZIP to reduce disk usage
- **Message deduplication (v1.4.0+)** for exactly-once semantics
- Replay from message ID or timestamp
- Message TTL and headers
- Metrics and pluggable logging
- Automatic compaction with retention policies
- Thread-safe concurrent access
- CLI tool for management

## Why LedgerQ?

**Single-node message queue with disk durability.** No network, no clustering, no external dependencies. If you're building CLI tools, desktop apps, or edge devices that need crash-safe task queues, LedgerQ stores messages on disk using an append-only log.

### When to Use LedgerQ

**Perfect for:**
- **Offline-first applications** - Mobile sync, desktop apps that work without network
- **Local task processing** - CI/CD pipelines, backup jobs, batch processing
- **Edge/IoT devices** - Limited connectivity, single-node operation
- **Event sourcing** - Local audit trails and event logs
- **Development/testing** - Simpler than running Kafka/RabbitMQ locally
- **Embedded systems** - Zero external dependencies, small footprint

### When NOT to Use LedgerQ

**Use these instead:**
- **Need multiple consumers on different machines?** → Use **Kafka** (distributed streaming)
- **Need pub/sub fan-out patterns?** → Use **NATS** or **RabbitMQ** (message broker)
- **Need distributed consensus?** → Use **Kafka** or **Pulsar** (distributed log)
- **Need cross-language support via HTTP?** → Use **RabbitMQ** or **ActiveMQ** (AMQP/STOMP)
- **Already using a database?** → Use **PostgreSQL LISTEN/NOTIFY** or **Redis Streams**

### Comparison

| Feature | LedgerQ | Kafka | NATS | RabbitMQ |
|---------|---------|-------|------|----------|
| **Deployment** | Single binary | Cluster (ZooKeeper/KRaft) | Single/Cluster | Single/Cluster |
| **Dependencies** | None | Java, ZooKeeper | None | Erlang |
| **Consumer Model** | Single reader | Consumer groups | Subjects/Queues | Queues/Exchanges |
| **Use Case** | Local queuing | Distributed streaming | Lightweight messaging | Enterprise messaging |
| **Setup Time** | Instant | Hours | Minutes | Minutes |

**LedgerQ's sweet spot**: You need reliability without the operational overhead of distributed systems.

### Trade-offs

- ✅ Zero config, just a directory path
- ✅ Survives crashes and reboots
- ✅ Fast sequential I/O (append-only)
- ✅ No network protocols or ports
- ✅ Embeddable in any Go application
- ❌ Single process only (file locking)
- ❌ Not distributed (single node)
- ❌ Single consumer (FIFO order)

## Quick Start

### Installation

```bash
go get github.com/1mb-dev/ledgerq/pkg/ledgerq@latest
```

### Basic Usage

```go
package main

import (
    "fmt"
    "log"

    "github.com/1mb-dev/ledgerq/pkg/ledgerq"
)

func main() {
    q, err := ledgerq.Open("/tmp/myqueue", nil)
    if err != nil {
        log.Fatal(err)
    }
    defer q.Close()

    offset, _ := q.Enqueue([]byte("Hello, World!"))
    fmt.Printf("Enqueued at offset: %d\n", offset)

    msg, _ := q.Dequeue()
    fmt.Printf("Dequeued [ID:%d]: %s\n", msg.ID, msg.Payload)
}
```

### Configuration

```go
opts := ledgerq.DefaultOptions("/tmp/myqueue")
opts.AutoSync = true
opts.MaxSegmentSize = 100 * 1024 * 1024
opts.CompactionInterval = 5 * time.Minute

q, _ := ledgerq.Open("/tmp/myqueue", opts)
```

## Core Operations

**Batch operations** (10-100x faster):

```go
payloads := [][]byte{[]byte("msg1"), []byte("msg2"), []byte("msg3")}
offsets, _ := q.EnqueueBatch(payloads)

messages, _ := q.DequeueBatch(10)
```

**Message TTL:**

```go
q.EnqueueWithTTL([]byte("temporary"), 5*time.Second)
```

**Message headers:**

```go
headers := map[string]string{"content-type": "application/json"}
q.EnqueueWithHeaders(payload, headers)
```

**Payload compression (v1.3.0+):**

```go
// Enable compression by default
opts := ledgerq.DefaultOptions("/tmp/myqueue")
opts.DefaultCompression = ledgerq.CompressionGzip
opts.CompressionLevel = 6  // 1 (fastest) to 9 (best compression)
opts.MinCompressionSize = 1024  // Only compress >= 1KB
q, _ := ledgerq.Open("/tmp/myqueue", opts)

// Messages are automatically compressed/decompressed
q.Enqueue(largePayload)  // Compressed if >= 1KB

// Or control compression per-message
q.EnqueueWithCompression(payload, ledgerq.CompressionGzip)
```

**Message deduplication (v1.4.0+):**

```go
// Enable deduplication with a 5-minute window
opts := ledgerq.DefaultOptions("/tmp/myqueue")
opts.DefaultDeduplicationWindow = 5 * time.Minute
opts.MaxDeduplicationEntries = 100000  // Max 100K unique messages tracked
q, _ := ledgerq.Open("/tmp/myqueue", opts)

// Enqueue with deduplication
offset, isDup, _ := q.EnqueueWithDedup(payload, "order-12345", 0)
if isDup {
    fmt.Printf("Duplicate detected! Original at offset %d\n", offset)
} else {
    fmt.Printf("New message enqueued at offset %d\n", offset)
}

// Custom per-message window
q.EnqueueWithDedup(payload, "request-789", 10*time.Minute)

// View deduplication statistics
stats := q.Stats()
fmt.Printf("Tracked entries: %d\n", stats.DedupTrackedEntries)
```

**Priority queue (v1.1.0+):**

```go
// Enable priority mode (set at queue creation, cannot be changed later)
opts := ledgerq.DefaultOptions("/tmp/myqueue")
opts.EnablePriorities = true
q, _ := ledgerq.Open("/tmp/myqueue", opts)

// Enqueue with priority
q.EnqueueWithPriority([]byte("urgent"), ledgerq.PriorityHigh)
q.EnqueueWithPriority([]byte("normal"), ledgerq.PriorityMedium)
q.EnqueueWithPriority([]byte("background"), ledgerq.PriorityLow)

// Dequeue in priority order (High → Medium → Low)
msg, _ := q.Dequeue() // Returns "urgent" first
```

**Batch operations with priorities (v1.1.0+):**

```go
// Batch enqueue with per-message options
messages := []ledgerq.BatchEnqueueOptions{
    {Payload: []byte("urgent task"), Priority: ledgerq.PriorityHigh},
    {Payload: []byte("normal task"), Priority: ledgerq.PriorityMedium},
    {Payload: []byte("background task"), Priority: ledgerq.PriorityLow, TTL: time.Hour},
}
offsets, _ := q.EnqueueBatchWithOptions(messages)
```

**Dead Letter Queue (v1.2.0+):**

```go
// Enable DLQ with max retries
opts := ledgerq.DefaultOptions("/tmp/myqueue")
opts.DLQPath = "/tmp/myqueue/dlq"
opts.MaxRetries = 3
q, _ := ledgerq.Open("/tmp/myqueue", opts)

// Process messages with Ack/Nack
msg, _ := q.Dequeue()

// Check retry info for custom backoff logic
if info := q.GetRetryInfo(msg.ID); info != nil {
    backoff := time.Duration(1<<uint(info.RetryCount)) * time.Second
    time.Sleep(backoff) // Exponential backoff
}

if processSuccessfully(msg.Payload) {
    q.Ack(msg.ID) // Mark as successfully processed
} else {
    q.Nack(msg.ID, "processing failed") // Record failure (moves to DLQ after MaxRetries)
}

// Inspect DLQ messages
dlq := q.GetDLQ()
dlqMsg, _ := dlq.Dequeue()
fmt.Printf("Failed: %s, Reason: %s\n",
    string(dlqMsg.Payload),
    dlqMsg.Headers["dlq.failure_reason"])

// Requeue from DLQ after fixing issues
q.RequeueFromDLQ(dlqMsg.ID)
```

**Note**: `Nack()` tracks failures but does not auto-retry messages. After `MaxRetries` failures, messages move to DLQ. For retry patterns with backoff, see [USAGE.md](docs/USAGE.md#dead-letter-queue-dlq---v120).

**Streaming:**

```go
ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
defer cancel()

q.Stream(ctx, func(msg *ledgerq.Message) error {
    fmt.Printf("Received: %s\n", msg.Payload)
    return nil
})
```

**Replay:**

```go
q.SeekToMessageID(100)
q.SeekToTimestamp(time.Now().Add(-1 * time.Hour).UnixNano())
```

## Performance

Benchmarks (Go 1.21, macOS, Intel i5-8257U @ 2.3GHz):

| Operation | Throughput | Latency |
|-----------|------------|---------|
| Enqueue (buffered) | 3M ops/sec | 300-400 ns |
| Enqueue (fsync) | 50 ops/sec | 19 ms |
| EnqueueBatch (100 msgs) | 50M msgs/sec | 200 ns/msg |
| Dequeue | 1.4K ops/sec | 700 μs |

Use batch operations for high throughput.

## Examples

See [examples/](examples/) for runnable code with comprehensive READMEs:

**Getting Started:**
- [simple](examples/simple) - Basic operations ⭐ **Start here!**
- [producer-consumer](examples/producer-consumer) - Concurrent access with multiple goroutines

**Core Features:**
- [ttl](examples/ttl) - Message expiration (time-to-live)
- [headers](examples/headers) - Message metadata and routing
- [replay](examples/replay) - Seeking and time-travel through message history
- [streaming](examples/streaming) - Real-time event streaming
- [metrics](examples/metrics) - Monitoring and observability

**Advanced Features (v1.1.0+):**
- **[priority](examples/priority)** - Priority ordering (v1.1.0+)
- **[dlq](examples/dlq)** - Dead Letter Queue for failed messages (v1.2.0+)
- **[compression](examples/compression)** - GZIP payload compression (v1.3.0+)
- **[deduplication](examples/deduplication)** - Exactly-once semantics (v1.4.0+)

## CLI Tool

```bash
go install github.com/1mb-dev/ledgerq/cmd/ledgerq@latest
```

**View queue statistics:**
```bash
$ ledgerq stats /path/to/queue

Queue Statistics
================
Directory:         /path/to/queue
Total Messages:    1500
Pending Messages:  342
Next Message ID:   1501
Read Message ID:   1159
Segment Count:     3
```

**Inspect queue metadata:**
```bash
$ ledgerq inspect /path/to/queue
```

**Compact queue (remove processed messages):**
```bash
$ ledgerq compact /path/to/queue
```

**Peek at messages (without dequeuing):**
```bash
$ ledgerq peek /path/to/queue 5
```

## Documentation

- [Usage Guide](docs/USAGE.md) - Complete reference
- [Architecture](docs/ARCHITECTURE.md) - Internal design
- [API Reference](https://pkg.go.dev/github.com/1mb-dev/ledgerq/pkg/ledgerq) - GoDoc
- [Contributing](CONTRIBUTING.md) - Development guide

## Limitations

### Batch Operations and Priority Queues

**Important**: `DequeueBatch()` always returns messages in FIFO order (by message ID), even when `EnablePriorities=true`. This is a performance trade-off: batch dequeue optimizes for sequential I/O rather than priority ordering.

For priority-aware consumption, use `Dequeue()` in a loop or the `Stream()` API.

```go
// FIFO batch dequeue (fast, sequential I/O)
messages, _ := q.DequeueBatch(100)

// Priority-aware dequeue (respects priority order)
for i := 0; i < 100; i++ {
    msg, err := q.Dequeue()
    // ...
}
```

### Priority Queue Memory Usage

When `EnablePriorities=true`, the queue maintains an in-memory index of all unprocessed message IDs, organized by priority level. Memory usage scales linearly with queue depth (measured with runtime.MemStats):

- 1M pending messages: ~24 MB
- 10M pending messages: ~240 MB
- 100M pending messages: ~2.4 GB

For very deep queues (>10M messages), consider:
- Using FIFO mode (`EnablePriorities=false`)
- Implementing application-level batching to keep queue depth low
- Running compaction regularly to remove processed messages

## Testing

```bash
go test ./...
go test -race ./...
go test -bench=. ./internal/queue
go test -fuzz=FuzzEnqueueDequeue -fuzztime=30s ./internal/queue
```

## License

Apache License 2.0 - see [LICENSE](LICENSE)

## Credits

Inspired by Apache Kafka, NATS Streaming, and Python's persist-queue.

Built as a learning project for message queue internals and Go systems programming.
