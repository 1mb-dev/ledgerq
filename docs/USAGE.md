# LedgerQ Usage Guide

**Last Updated:** 2025-10-21
**Version:** 1.4.1
**Maintainer:** @1mb-dev
**Review Cadence:** After each minor version release

Complete reference for using LedgerQ.

## Table of Contents

- [Installation](#installation)
- [Basic Operations](#basic-operations)
- [Configuration](#configuration)
- [Advanced Features](#advanced-features)
  - [Message TTL](#message-ttl-time-to-live)
  - [Message Headers](#message-headers)
  - [Streaming API](#streaming-api)
  - [Replay Operations](#replay-operations)
  - [Queue Statistics](#queue-statistics)
  - [Metrics Collection](#metrics-collection)
  - [Logging](#logging)
  - [Manual Compaction](#manual-compaction)
  - [Priority Queue (v1.1.0+)](#priority-queue---v110)
  - [Dead Letter Queue (v1.2.0+)](#dead-letter-queue-dlq---v120)
  - [Payload Compression (v1.3.0+)](#payload-compression---v130)
  - [Message Deduplication (v1.4.0+)](#message-deduplication---v140)
- [CLI Tool](#cli-tool)
- [Best Practices](#best-practices)
  - [Performance](#performance)
  - [Reliability](#reliability)
  - [Development](#development)
  - [Security](#security)
- [Troubleshooting](#troubleshooting)

## Installation

```bash
go get github.com/1mb-dev/ledgerq/pkg/ledgerq@latest
```

Import in your code:

```go
import "github.com/1mb-dev/ledgerq/pkg/ledgerq"
```

## Basic Operations

### Opening a Queue

**Default configuration**:
```go
q, err := ledgerq.Open("/path/to/queue", nil)
if err != nil {
    log.Fatal(err)
}
defer q.Close()
```

**Custom configuration**:
```go
opts := ledgerq.DefaultOptions("/path/to/queue")
opts.AutoSync = true
opts.SyncInterval = 1 * time.Second
opts.MaxSegmentSize = 100 * 1024 * 1024 // 100MB

q, err := ledgerq.Open("/path/to/queue", opts)
```

### Enqueuing Messages

**Single message**:
```go
offset, err := q.Enqueue([]byte("Hello, World!"))
if err != nil {
    return err
}
fmt.Printf("Enqueued at offset: %d\n", offset)
```

**Batch enqueue** (recommended for high throughput):
```go
payloads := [][]byte{
    []byte("message1"),
    []byte("message2"),
    []byte("message3"),
}
offsets, err := q.EnqueueBatch(payloads)
if err != nil {
    return err
}
```

Batching provides 10-100x performance improvement with a single fsync.

### Dequeuing Messages

**Single message**:
```go
msg, err := q.Dequeue()
if err != nil {
    // Queue might be empty or closed
    return err
}
fmt.Printf("Message ID: %d, Payload: %s\n", msg.ID, msg.Payload)
```

**Batch dequeue**:
```go
messages, err := q.DequeueBatch(10) // Up to 10 messages
if err != nil {
    return err
}

for _, msg := range messages {
    fmt.Printf("Processing: %s\n", msg.Payload)
}
```

### Closing the Queue

```go
if err := q.Close(); err != nil {
    log.Printf("Error closing queue: %v", err)
}
```

## Configuration

### Options Reference

```go
type Options struct {
    // Sync behavior
    AutoSync       bool          // fsync after every write (default: false)
    SyncInterval   time.Duration // Periodic sync interval (default: 1s)

    // Compaction
    CompactionInterval time.Duration // Auto-compaction interval (0 = disabled)

    // Segment rotation
    MaxSegmentSize     uint64         // Max bytes per segment (default: 1GB)
    MaxSegmentMessages uint64         // Max messages per segment (0 = unlimited)
    RotationPolicy     RotationPolicy // When to rotate segments

    // Retention
    RetentionPolicy *RetentionPolicy  // Segment cleanup policy (nil = no cleanup)

    // Observability
    Logger           Logger            // Structured logging (nil = no logging)
    MetricsCollector MetricsCollector // Metrics collection (nil = no metrics)
}
```

### Rotation Policies

**By size** (default):
```go
opts.RotationPolicy = ledgerq.RotateBySize
opts.MaxSegmentSize = 100 * 1024 * 1024 // 100MB
```

**By message count**:
```go
opts.RotationPolicy = ledgerq.RotateByCount
opts.MaxSegmentMessages = 1000000 // 1M messages
```

**By both** (whichever comes first):
```go
opts.RotationPolicy = ledgerq.RotateByBoth
opts.MaxSegmentSize = 100 * 1024 * 1024
opts.MaxSegmentMessages = 1000000
```

### Retention Policies

Configure automatic cleanup of old segments:

```go
opts.RetentionPolicy = &ledgerq.RetentionPolicy{
    MaxAge:      7 * 24 * time.Hour,          // Delete segments older than 7 days
    MaxSize:     10 * 1024 * 1024 * 1024,     // Keep total size under 10GB
    MaxSegments: 100,                          // Keep max 100 segments
    MinSegments: 1,                            // Always keep at least 1
}
```

## Advanced Features

### Message TTL (Time-To-Live)

Enqueue messages that expire automatically:

```go
// Expire after 5 seconds
offset, err := q.EnqueueWithTTL([]byte("temporary"), 5*time.Second)
```

**How it works**:
- Expired messages are skipped during dequeue
- No background cleanup (lazy expiration)
- TTL persists across restarts
- Zero overhead for non-TTL messages

**Use cases**:
- Temporary task queues
- Time-sensitive notifications
- Cache invalidation messages
- Session management

### Message Headers

Attach metadata to messages:

```go
headers := map[string]string{
    "content-type":   "application/json",
    "correlation-id": "req-12345",
    "priority":       "high",
}
offset, err := q.EnqueueWithHeaders(payload, headers)
```

**Combine TTL and headers**:
```go
offset, err := q.EnqueueWithOptions(payload, 10*time.Second, headers)
```

**Use cases**:
- Message routing and classification
- Distributed tracing (trace-id, span-id)
- Event sourcing metadata
- Content-type indication

### Streaming API

Process messages in real-time with context support:

```go
ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
defer cancel()

handler := func(msg *ledgerq.Message) error {
    fmt.Printf("Received: %s\n", msg.Payload)
    // Return error to stop streaming
    return nil
}

err := q.Stream(ctx, handler)
if err != nil && err != context.Canceled {
    log.Printf("Stream error: %v", err)
}
```

**Benefits**:
- Push-based (no polling loop needed)
- Graceful shutdown via context
- Cleaner error handling

### Replay Operations

Reprocess messages from a specific point:

**Seek by message ID**:
```go
err := q.SeekToMessageID(100)
if err != nil {
    return err
}
// Next Dequeue() starts from message ID 100
```

**Seek by timestamp**:
```go
oneHourAgo := time.Now().Add(-1 * time.Hour).UnixNano()
err := q.SeekToTimestamp(oneHourAgo)
if err != nil {
    return err
}
// Next Dequeue() starts from first message after timestamp
```

**Use cases**:
- Replay events for testing
- Reprocess failed batches
- Audit log analysis
- Time-based recovery

### Queue Statistics

Monitor queue health:

```go
stats := q.Stats()
fmt.Printf("Total messages:   %d\n", stats.TotalMessages)
fmt.Printf("Pending messages: %d\n", stats.PendingMessages)
fmt.Printf("Next ID:          %d\n", stats.NextMessageID)
fmt.Printf("Read position:    %d\n", stats.ReadMessageID)
fmt.Printf("Segments:         %d\n", stats.SegmentCount)
```

### Metrics Collection

Collect performance metrics:

```go
// Create collector
collector := ledgerq.NewMetricsCollector("my_queue")

opts := ledgerq.DefaultOptions("/path/to/queue")
opts.MetricsCollector = collector

q, _ := ledgerq.Open("/path/to/queue", opts)

// Perform operations...
q.Enqueue([]byte("test"))
q.Dequeue()

// Get snapshot
snapshot := ledgerq.GetMetricsSnapshot(collector)
fmt.Printf("Enqueue Total:    %d\n", snapshot.EnqueueTotal)
fmt.Printf("Dequeue Total:    %d\n", snapshot.DequeueTotal)
fmt.Printf("Enqueue P95:      %v\n", snapshot.EnqueueDurationP95)
fmt.Printf("Pending Messages: %d\n", snapshot.PendingMessages)
```

**Available metrics**:
- Operation counters (enqueue, dequeue, batch, errors)
- Payload bytes (enqueued, dequeued)
- Duration percentiles (P50, P95, P99)
- Queue state (pending, segments, IDs)
- Compaction metrics

### Logging

**Implement custom logger**:

LedgerQ does not export a default logger implementation. To enable logging, implement the `Logger` interface:
```go
// Logger interface that must be implemented
type Logger interface {
    Debug(msg string, fields ...LogField)
    Info(msg string, fields ...LogField)
    Warn(msg string, fields ...LogField)
    Error(msg string, fields ...LogField)
}

// Example implementation using standard log package
type SimpleLogger struct{}

func (l *SimpleLogger) Debug(msg string, fields ...ledgerq.LogField) {
    log.Printf("[DEBUG] %s %v", msg, fields)
}
func (l *SimpleLogger) Info(msg string, fields ...ledgerq.LogField) {
    log.Printf("[INFO] %s %v", msg, fields)
}
func (l *SimpleLogger) Warn(msg string, fields ...ledgerq.LogField) {
    log.Printf("[WARN] %s %v", msg, fields)
}
func (l *SimpleLogger) Error(msg string, fields ...ledgerq.LogField) {
    log.Printf("[ERROR] %s %v", msg, fields)
}

// Use custom logger
opts.Logger = &SimpleLogger{}
```

### Manual Compaction

Trigger compaction on demand:

```go
result, err := q.Compact()
if err != nil {
    return err
}
fmt.Printf("Removed %d segments, freed %d bytes\n",
    result.SegmentsRemoved, result.BytesFreed)
```

**Automatic compaction**:
```go
opts.CompactionInterval = 5 * time.Minute
// Compaction runs in background
```

### Priority Queue - v1.1.0+

Process urgent messages first with three priority levels.

**Enable priority mode**:
```go
opts := ledgerq.DefaultOptions("/path/to/queue")
opts.EnablePriorities = true
q, err := ledgerq.Open("/path/to/queue", opts)
```

**Enqueue with priority**:
```go
// Priority constants: PriorityHigh (2), PriorityMedium (1), PriorityLow (0)
q.EnqueueWithPriority([]byte("urgent task"), ledgerq.PriorityHigh)
q.EnqueueWithPriority([]byte("normal task"), ledgerq.PriorityMedium)
q.EnqueueWithPriority([]byte("background"), ledgerq.PriorityLow)

// Dequeue returns highest priority first
msg, _ := q.Dequeue()  // Returns "urgent task"
```

**Batch operations with priorities**:
```go
messages := []ledgerq.BatchEnqueueOptions{
    {Payload: []byte("urgent"), Priority: ledgerq.PriorityHigh, TTL: time.Hour},
    {Payload: []byte("normal"), Priority: ledgerq.PriorityMedium},
}
offsets, _ := q.EnqueueBatchWithOptions(messages)
```

**Important**: `DequeueBatch()` returns messages in FIFO order (ignores priorities). Use `Dequeue()` in a loop for priority-aware processing.

**Memory usage**: Priority mode maintains an in-memory index. ~24 MB per 1M pending messages.

### Dead Letter Queue (DLQ) - v1.2.0+

Handle failed messages with automatic retry tracking and dead letter queue support.

**Enable DLQ**:
```go
opts := ledgerq.DefaultOptions("/path/to/queue")
opts.DLQPath = "/path/to/queue/dlq"   // Enable DLQ
opts.MaxRetries = 3                    // Max 3 retry attempts (default)

q, err := ledgerq.Open("/path/to/queue", opts)
```

**Message Acknowledgment**:
```go
msg, err := q.Dequeue()
if err != nil {
    return err
}

// Process message
if err := processMessage(msg.Payload); err != nil {
    // Report failure - increments retry counter
    q.Nack(msg.ID, fmt.Sprintf("processing failed: %v", err))
} else {
    // Report success - clears retry tracking
    q.Ack(msg.ID)
}
```

**How it works**:
- Call `Ack(msgID)` after successful processing
- Call `Nack(msgID, reason)` when processing fails
- After `MaxRetries` failures, message automatically moves to DLQ
- Retry state persists across restarts (crash-safe)
- DLQ messages include failure metadata headers

**Inspect DLQ messages**:
```go
dlq := q.GetDLQ()
if dlq == nil {
    // DLQ not configured
    return
}

// Check DLQ stats
dlqStats := dlq.Stats()
fmt.Printf("DLQ has %d pending messages\n", dlqStats.PendingMessages)

// Read DLQ message
dlqMsg, err := dlq.Dequeue()
if err != nil {
    return err
}

// DLQ messages have metadata headers
fmt.Printf("Original message ID: %s\n", dlqMsg.Headers["dlq.original_msg_id"])
fmt.Printf("Retry count: %s\n", dlqMsg.Headers["dlq.retry_count"])
fmt.Printf("Failure reason: %s\n", dlqMsg.Headers["dlq.failure_reason"])
fmt.Printf("Last failure: %s\n", dlqMsg.Headers["dlq.last_failure"])
fmt.Printf("Payload: %s\n", string(dlqMsg.Payload))
```

**Requeue from DLQ**:
```go
// After fixing the issue, requeue message back to main queue
dlqMsg, err := dlq.Dequeue()
if err != nil {
    return err
}

err = q.RequeueFromDLQ(dlqMsg.ID)
if err != nil {
    return err
}

// Message is now back in main queue with fresh retry state
```

**DLQ metadata headers**:
- `dlq.original_msg_id`: Original message ID from main queue
- `dlq.retry_count`: Number of failed attempts
- `dlq.failure_reason`: Last failure reason from Nack()
- `dlq.last_failure`: Timestamp of last failure (RFC3339)

**Use cases**:
- Handling transient failures (network timeouts, temporary service outages)
- Poison message isolation (malformed data, processing bugs)
- Manual investigation of failed messages
- Automated retry with backoff strategies
- Message reprocessing after bug fixes

**Important notes**:
- DLQ is disabled by default (set `DLQPath` to enable)
- When disabled, `Ack()` and `Nack()` are no-ops (zero overhead)
- DLQ queue itself doesn't have a DLQ (prevents infinite recursion)
- Retry state stored in `.retry_state.json` file
- Use `Ack()` even if DLQ disabled - enables future DLQ without code changes

**Understanding Ack/Nack:**

`Dequeue()` consumes messages permanently (read position advances). `Nack(msgID, reason)` tracks the failure count but does NOT requeue the message. After `MaxRetries` Nack() calls, the message moves to DLQ for manual inspection.

For retries with exponential backoff, implement application-level re-enqueue logic (pattern below).

**Retry Pattern:**

Store failed messages in memory and re-enqueue with exponential backoff:

```go
type FailedMessage struct {
    Payload  []byte
    Headers  map[string]string
    RetryAt  time.Time
    Attempts int
}

func worker(q *ledgerq.Queue) {
    failedMsgs := make(map[uint64]*FailedMessage)
    const maxRetries = 3

    // Background retry worker
    go func() {
        for range time.Tick(time.Second) {
            now := time.Now()
            for id, msg := range failedMsgs {
                if now.After(msg.RetryAt) && msg.Attempts < maxRetries {
                    msg.Headers["retry_count"] = fmt.Sprintf("%d", msg.Attempts)
                    q.EnqueueWithHeaders(msg.Payload, msg.Headers)
                    delete(failedMsgs, id)
                } else if msg.Attempts >= maxRetries {
                    log.Printf("Max retries exceeded: %d", id)
                    delete(failedMsgs, id)
                }
            }
        }
    }()

    // Main worker loop
    for {
        msg, _ := q.Dequeue()

        if err := processTask(msg.Payload); err != nil {
            attempts := 1
            if rc, ok := msg.Headers["retry_count"]; ok {
                fmt.Sscanf(rc, "%d", &attempts)
                attempts++
            }

            backoff := time.Duration(1<<uint(attempts-1)) * time.Second
            failedMsgs[msg.ID] = &FailedMessage{
                Payload:  msg.Payload,
                Headers:  msg.Headers,
                RetryAt:  time.Now().Add(backoff),
                Attempts: attempts,
            }
            log.Printf("Retry scheduled [ID:%d, Attempt:%d] in %v", msg.ID, attempts, backoff)
        }
    }
}
```

**Alternative patterns:** See `examples/dlq/` for:
- Separate retry queue (persistent state across restarts)
- DLQ-only mode (manual inspection without auto-retry)

**Best practices:**
- Use exponential backoff: `2^(attempt-1) * baseDelay`
- Add jitter to prevent thundering herd: `backoff * (0.9 + rand.Float64()*0.2)`
- Set reasonable max retries (3-5 attempts)
- Track retry count in message headers

**Example: Worker pattern with DLQ**:
```go
func worker(q *ledgerq.Queue) {
    for {
        msg, err := q.Dequeue()
        if err != nil {
            if err == ledgerq.ErrQueueEmpty {
                time.Sleep(100 * time.Millisecond)
                continue
            }
            log.Printf("Dequeue error: %v", err)
            return
        }

        // Process message
        if err := processTask(msg.Payload); err != nil {
            log.Printf("Task failed [ID:%d]: %v", msg.ID, err)
            q.Nack(msg.ID, err.Error())

            // ⚠️ WARNING: Without backoff, this will retry immediately!
            // See retry strategies above for recommended patterns.
            continue
        }

        // Success
        q.Ack(msg.ID)
    }
}
```

### Payload Compression - v1.3.0+

Reduce disk usage with optional GZIP compression. Useful for large text payloads (JSON, XML, logs).

**Enable compression by default**:
```go
opts := ledgerq.DefaultOptions("/path/to/queue")
opts.DefaultCompression = ledgerq.CompressionGzip
opts.CompressionLevel = 6             // 1 (fastest) to 9 (best compression)
opts.MinCompressionSize = 1024        // Only compress payloads >= 1KB
q, err := ledgerq.Open("/path/to/queue", opts)
```

**Per-message compression**:
```go
// Compress specific message
q.EnqueueWithCompression(largePayload, ledgerq.CompressionGzip)

// Disable compression for small message
q.EnqueueWithCompression(smallPayload, ledgerq.CompressionNone)
```

**How it works**:
- Automatic compression/decompression (transparent to application)
- Skips compression if savings < 5% (efficiency check)
- Decompression bomb protection (100MB limit)
- Uses stdlib `compress/gzip` (zero dependencies)

**When to use**:
- Large text payloads (JSON, XML, logs): 60-80% compression
- Small payloads (<1KB): Skip compression (overhead not worth it)
- Binary data (images, protobuf): Usually pre-compressed, skip

### Message Deduplication - v1.4.0+

Prevent duplicate processing with idempotency keys. Perfect for at-least-once delivery semantics.

**Enable deduplication**:
```go
opts := ledgerq.DefaultOptions("/path/to/queue")
opts.DefaultDeduplicationWindow = 5 * time.Minute
opts.MaxDeduplicationEntries = 100000   // Max 100K tracked (~6.4 MB memory)
q, err := ledgerq.Open("/path/to/queue", opts)
```

**Enqueue with deduplication**:
```go
// Use business ID as dedup key (e.g., order ID, request ID)
offset, isDup, err := q.EnqueueWithDedup(payload, "order-12345", 0)
if isDup {
    fmt.Printf("Duplicate! Original at offset %d\n", offset)
    // Message NOT enqueued, returned offset of original
} else {
    fmt.Printf("New message at offset %d\n", offset)
}
```

**Custom time windows**:
```go
// Override default window for this message
q.EnqueueWithDedup(payload, "payment-789", 10*time.Minute)

// Use 0 to apply queue's default window
q.EnqueueWithDedup(payload, "event-456", 0)
```

**How it works**:
- SHA-256 hash of dedup ID (not payload)
- O(1) duplicate detection with in-memory hash table
- Entries expire after time window
- State persists across restarts (`.dedup_state.json`)
- Background cleanup every 10 seconds

**Memory usage**: ~64 bytes per tracked entry. 100K entries ≈ 6.4 MB.

**Use cases**:
- Idempotent message processing (prevent duplicate orders)
- DLQ requeue safety (prevent re-processing)
- Webhook deduplication (same event sent multiple times)

**View statistics**:
```go
stats := q.Stats()
fmt.Printf("Tracking %d unique messages\n", stats.DedupTrackedEntries)
```

## CLI Tool

### Installation

```bash
go install github.com/1mb-dev/ledgerq/cmd/ledgerq@latest
```

### Commands

**Show statistics**:
```bash
ledgerq stats /path/to/queue
```

**Inspect queue (JSON)**:
```bash
ledgerq inspect /path/to/queue
```

**Compact manually**:
```bash
ledgerq compact /path/to/queue
```

**Peek messages** (without consuming):
```bash
ledgerq peek /path/to/queue 5
```

**Show version**:
```bash
ledgerq version
```

## Best Practices

### Performance

1. **Use batch operations** for high throughput
   - Single fsync for entire batch
   - 10-100x faster than individual operations

2. **Disable AutoSync** for better performance
   - Use periodic sync instead (SyncInterval)
   - Risk: potential data loss on crash
   - Mitigation: Use batch operations

3. **Right-size segments**
   - Too small: Frequent rotation overhead
   - Too large: Slower compaction, recovery
   - Recommended: 100MB - 1GB

4. **Monitor metrics**
   - Watch pending message count
   - Track dequeue latency percentiles
   - Alert on compaction failures

### Reliability

1. **Always close queues properly**
   - Use `defer q.Close()` pattern
   - Ensures data is flushed to disk

2. **Handle errors appropriately**
   - Queue might be closed
   - Disk might be full
   - Data might be corrupted

3. **Use AutoSync for critical data**
   - Guarantees durability at cost of performance
   - Or use batch operations with periodic sync

4. **Configure retention policies**
   - Prevent disk from filling up
   - Keep MinSegments > 0 for safety

### Development

1. **Use examples as templates**
   - See `examples/` directory
   - Cover common patterns

2. **Run tests before deploying**
   - `go test -race ./...`
   - Catches concurrency issues

3. **Enable logging during development**
   - Debug issues more easily
   - Disable in production for performance

### Security

**File Permissions**: Queue data files are created with 0644 permissions (world-readable). For sensitive data, restrict access using parent directory permissions:

```go
// Create queue in user-only directory
queueDir := "/var/app/queues/sensitive"
if err := os.MkdirAll(queueDir, 0700); err != nil {
    log.Fatal(err)
}
q, err := ledgerq.Open(queueDir, nil)
```

**Additional Security Considerations**:
- Validate and sanitize data before enqueueing
- Consider application-level encryption for sensitive payloads
- Avoid placing queue directories in web-accessible locations
- Use dedicated service accounts on multi-user systems
- Monitor queue statistics for unusual activity

For complete security policy and audit results, see:
- [SECURITY.md](../SECURITY.md) - Vulnerability reporting and security policy
- [SECURITY_AUDIT.md](SECURITY_AUDIT.md) - Detailed security audit findings

## Troubleshooting

### Queue won't open

**Error**: "failed to create directory"
- **Cause**: Permission issues
- **Fix**: Ensure directory is writable

**Error**: "invalid segment sequence"
- **Cause**: Corrupted or missing segment files
- **Fix**: Restore from backup or delete corrupted segments

### Messages not persisting

**Cause**: AutoSync disabled, process crashed before sync
- **Fix**: Enable AutoSync or use shorter SyncInterval

### Performance degradation

**Cause**: Too many small segments
- **Fix**: Increase MaxSegmentSize or enable compaction

**Cause**: Disk full
- **Fix**: Configure retention policy or free up space

### High memory usage

**Cause**: Large batch operations
- **Fix**: Reduce batch size

**Cause**: Metrics collecting too much history
- **Fix**: Reset metrics periodically or disable

### Compaction not running

**Cause**: CompactionInterval = 0
- **Fix**: Set non-zero interval or use manual compaction

**Cause**: No retention policy configured
- **Fix**: Configure RetentionPolicy

## See Also

- [Architecture](ARCHITECTURE.md) - Internal design details
- [Examples](../examples/) - Runnable code examples
- [API Documentation](https://pkg.go.dev/github.com/1mb-dev/ledgerq/pkg/ledgerq) - GoDoc reference
