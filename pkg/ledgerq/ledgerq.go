// Package ledgerq provides a persistent, disk-backed message queue.
//
// LedgerQ is designed for local message persistence with FIFO guarantees,
// automatic segment rotation, and efficient replay capabilities.
//
// Example usage:
//
//	q, err := ledgerq.Open("/path/to/queue", nil)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer q.Close()
//
//	// Enqueue a message
//	offset, err := q.Enqueue([]byte("Hello, World!"))
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	// Dequeue a message
//	msg, err := q.Dequeue()
//	if err != nil {
//	    log.Fatal(err)
//	}
//	fmt.Printf("Message: %s\n", msg.Payload)
package ledgerq

import (
	"context"
	"time"

	"github.com/1mb-dev/ledgerq/internal/format"
	"github.com/1mb-dev/ledgerq/internal/logging"
	"github.com/1mb-dev/ledgerq/internal/metrics"
	"github.com/1mb-dev/ledgerq/internal/queue"
	"github.com/1mb-dev/ledgerq/internal/segment"
)

// Version is the current version of LedgerQ.
// This is the single source of truth for the application version.
const Version = "1.4.1"

// Priority represents the priority level of a message.
type Priority uint8

const (
	// PriorityLow is the default priority level (FIFO behavior).
	PriorityLow Priority = 0

	// PriorityMedium gives messages higher precedence than low priority.
	PriorityMedium Priority = 1

	// PriorityHigh gives messages the highest precedence.
	PriorityHigh Priority = 2
)

// CompressionType represents the compression algorithm used for message payloads.
type CompressionType uint8

const (
	// CompressionNone indicates no compression (default).
	CompressionNone CompressionType = 0

	// CompressionGzip indicates GZIP compression using stdlib compress/gzip.
	CompressionGzip CompressionType = 1
)

// Queue represents a persistent message queue.
type Queue struct {
	q *queue.Queue
}

// Message represents a dequeued message.
type Message struct {
	// ID is the unique message identifier
	ID uint64

	// Offset is the file offset where the message is stored
	Offset uint64

	// Payload is the message data
	Payload []byte

	// Timestamp is when the message was enqueued (Unix nanoseconds)
	Timestamp int64

	// ExpiresAt is when the message expires (Unix nanoseconds), 0 if no TTL
	ExpiresAt int64

	// Priority is the message priority level (v1.1.0+)
	Priority Priority

	// Headers contains key-value metadata for the message
	Headers map[string]string
}

// Options configures queue behavior.
type Options struct {
	// AutoSync enables automatic syncing after each write
	// Default: false
	AutoSync bool

	// SyncInterval for periodic syncing (if AutoSync is false)
	// Default: 1 second
	SyncInterval time.Duration

	// CompactionInterval for automatic background compaction
	// Set to 0 to disable automatic compaction (default)
	CompactionInterval time.Duration

	// MaxSegmentSize is the maximum size of a segment file in bytes
	// Default: 1GB
	MaxSegmentSize uint64

	// MaxSegmentMessages is the maximum number of messages per segment
	// Default: unlimited (0)
	MaxSegmentMessages uint64

	// RotationPolicy determines when to rotate segments
	// Options: RotateBySize, RotateByCount, RotateByBoth
	// Default: RotateBySize
	RotationPolicy RotationPolicy

	// RetentionPolicy configures segment retention and cleanup
	// Default: nil (no automatic cleanup)
	RetentionPolicy *RetentionPolicy

	// EnablePriorities enables priority queue mode (v1.1.0+)
	// When disabled, all messages are treated as PriorityLow (FIFO behavior)
	// Default: false
	EnablePriorities bool

	// PriorityStarvationWindow prevents low-priority message starvation (v1.1.0+)
	// Low-priority messages waiting longer than this duration will be promoted
	// Set to 0 to disable starvation prevention
	// Default: 30 seconds
	PriorityStarvationWindow time.Duration

	// DLQPath is the path to the dead letter queue directory (v1.2.0+)
	// If empty, DLQ is disabled. Messages that fail processing after MaxRetries
	// will be moved to this separate queue for inspection and potential reprocessing.
	// Default: "" (disabled)
	DLQPath string

	// MaxRetries is the maximum number of delivery attempts before moving to DLQ (v1.2.0+)
	// Set to 0 for unlimited retries (messages never move to DLQ).
	// Only effective when DLQPath is configured.
	// Default: 3
	MaxRetries int

	// MaxMessageSize is the maximum size in bytes for a single message payload (v1.2.0+)
	// Messages larger than this will be rejected during enqueue.
	// Set to 0 for unlimited message size (not recommended for production).
	// Default: 10 MB
	MaxMessageSize int64

	// MinFreeDiskSpace is the minimum required free disk space in bytes (v1.2.0+)
	// Enqueue operations will fail if available disk space falls below this threshold.
	// Set to 0 to disable disk space checking (not recommended for production).
	// Default: 100 MB
	MinFreeDiskSpace int64

	// DLQMaxAge is the maximum age for messages in the DLQ (v1.2.0+)
	// Messages older than this duration will be removed during compaction.
	// Set to 0 to keep DLQ messages indefinitely.
	// Default: 0 (no age-based cleanup)
	DLQMaxAge time.Duration

	// DLQMaxSize is the maximum total size in bytes for the DLQ (v1.2.0+)
	// When DLQ exceeds this size, oldest messages will be removed during compaction.
	// Set to 0 for unlimited DLQ size.
	// Default: 0 (no size limit)
	DLQMaxSize int64

	// DefaultCompression is the compression type used when not explicitly specified (v1.3.0+)
	// Set to CompressionNone to disable compression by default
	// Default: CompressionNone (no compression)
	DefaultCompression CompressionType

	// CompressionLevel is the compression level for algorithms that support it (v1.3.0+)
	// For GZIP: 1 (fastest) to 9 (best compression), 0 = default (6)
	// Higher values = better compression but slower
	// Default: 0 (use algorithm default)
	CompressionLevel int

	// MinCompressionSize is the minimum payload size to compress (v1.3.0+)
	// Messages smaller than this are not compressed even if compression is requested
	// This avoids the CPU overhead when compression doesn't help much
	// Default: 1024 bytes (1KB)
	MinCompressionSize int

	// DefaultDeduplicationWindow is the default time window for dedup tracking (v1.4.0+)
	// Set to 0 to disable deduplication by default
	// Default: 0 (disabled)
	DefaultDeduplicationWindow time.Duration

	// MaxDeduplicationEntries is the maximum number of dedup entries to track (v1.4.0+)
	// Prevents unbounded memory growth
	// Default: 100,000 entries (~6.4 MB)
	MaxDeduplicationEntries int

	// Logger for structured logging (nil = no logging)
	// Default: no logging
	Logger Logger

	// MetricsCollector for collecting queue metrics (nil = no metrics)
	// Default: no metrics
	MetricsCollector MetricsCollector
}

// EnqueueOptions contains options for enqueuing a message with all features.
type EnqueueOptions struct {
	// Priority is the message priority level (v1.1.0+)
	// Default: PriorityLow
	Priority Priority

	// TTL is the time-to-live duration for the message
	// Set to 0 for no expiration
	// Default: 0 (no expiration)
	TTL time.Duration

	// Headers contains key-value metadata for the message
	// Default: nil
	Headers map[string]string

	// Compression is the compression type for the message (v1.3.0+)
	// Set to CompressionNone to use queue's DefaultCompression
	// Default: CompressionNone (uses queue default)
	Compression CompressionType
}

// MetricsCollector defines the interface for recording queue metrics.
type MetricsCollector interface {
	RecordEnqueue(payloadSize int, duration time.Duration)
	RecordDequeue(payloadSize int, duration time.Duration)
	RecordEnqueueBatch(count, totalPayloadSize int, duration time.Duration)
	RecordDequeueBatch(count, totalPayloadSize int, duration time.Duration)
	RecordEnqueueError()
	RecordDequeueError()
	RecordSeek()
	RecordCompaction(segmentsRemoved int, bytesFreed int64, duration time.Duration)
	RecordCompactionError()
	UpdateQueueState(pending, segments, nextMsgID, readMsgID uint64)
}

// RotationPolicy determines when segment rotation occurs.
type RotationPolicy int

const (
	// RotateBySize rotates when segment size exceeds MaxSegmentSize
	RotateBySize RotationPolicy = iota

	// RotateByCount rotates when message count exceeds MaxSegmentMessages
	RotateByCount

	// RotateByBoth rotates when either size or count limit is reached
	RotateByBoth
)

// RetentionPolicy configures segment retention.
type RetentionPolicy struct {
	// MaxAge is the maximum age of segments to keep
	MaxAge time.Duration

	// MaxSize is the maximum total size of all segments
	MaxSize uint64

	// MaxSegments is the maximum number of segments to keep
	MaxSegments int

	// MinSegments is the minimum number of segments to always keep
	MinSegments int
}

// Logger interface for pluggable logging.
type Logger interface {
	Debug(msg string, fields ...LogField)
	Info(msg string, fields ...LogField)
	Warn(msg string, fields ...LogField)
	Error(msg string, fields ...LogField)
}

// LogField represents a structured log field.
type LogField struct {
	Key   string
	Value interface{}
}

// Stats contains queue statistics.
type Stats struct {
	// TotalMessages is the total number of messages ever enqueued
	TotalMessages uint64

	// PendingMessages is the number of unread messages
	PendingMessages uint64

	// NextMessageID is the ID that will be assigned to the next enqueued message
	NextMessageID uint64

	// ReadMessageID is the ID of the next message to be dequeued
	ReadMessageID uint64

	// SegmentCount is the number of segments
	SegmentCount int

	// DLQ statistics (v1.2.0+)
	// These fields are populated only when DLQ is enabled

	// DLQMessages is the total number of messages in the DLQ
	DLQMessages uint64

	// DLQPendingMessages is the number of unprocessed messages in the DLQ
	DLQPendingMessages uint64

	// RetryTrackedMessages is the number of messages currently being tracked for retries
	RetryTrackedMessages int

	// DedupTrackedEntries is the number of active dedup entries (v1.4.0+)
	DedupTrackedEntries int
}

// CompactionResult contains the result of a compaction operation.
type CompactionResult struct {
	// SegmentsRemoved is the number of segments removed
	SegmentsRemoved int

	// BytesFreed is the total bytes freed
	BytesFreed int64
}

// MetricsSnapshot is a point-in-time view of queue metrics.
type MetricsSnapshot = metrics.Snapshot

// NewMetricsCollector creates a new metrics collector for a queue.
// The queue name is used to identify metrics from this specific queue.
func NewMetricsCollector(queueName string) *metrics.Collector {
	return metrics.NewCollector(queueName)
}

// GetMetricsSnapshot returns a snapshot of current metrics from a collector.
func GetMetricsSnapshot(collector MetricsCollector) *MetricsSnapshot {
	if c, ok := collector.(*metrics.Collector); ok {
		return c.GetSnapshot()
	}
	return nil
}

// DefaultOptions returns sensible defaults for queue configuration.
func DefaultOptions(dir string) *Options {
	return &Options{
		AutoSync:                 false,
		SyncInterval:             1 * time.Second,
		CompactionInterval:       0,                  // Disabled by default
		MaxSegmentSize:           1024 * 1024 * 1024, // 1GB
		MaxSegmentMessages:       0,                  // Unlimited
		RotationPolicy:           RotateBySize,
		RetentionPolicy:          nil,               // No retention
		EnablePriorities:         false,             // FIFO mode by default
		PriorityStarvationWindow: 30 * time.Second,  // 30 seconds
		DLQPath:                  "",                // DLQ disabled by default
		MaxRetries:               3,                 // 3 retries before moving to DLQ
		MaxMessageSize:           10 * 1024 * 1024,  // 10 MB max message size
		MinFreeDiskSpace:         100 * 1024 * 1024, // 100 MB minimum free space
		DLQMaxAge:                0,                 // No age-based cleanup by default
		DLQMaxSize:               0,                 // No size limit by default
		Logger:                   nil,               // No logging
		MetricsCollector:         nil,               // No metrics
	}
}

// Open opens or creates a queue at the specified directory.
// If opts is nil, default options are used.
func Open(dir string, opts *Options) (*Queue, error) {
	var qopts *queue.Options
	if opts == nil {
		qopts = queue.DefaultOptions(dir)
	} else {
		metricsCollector := opts.MetricsCollector
		if metricsCollector == nil {
			metricsCollector = metrics.NoopCollector{}
		}

		qopts = &queue.Options{
			SegmentOptions:             convertSegmentOptions(dir, opts),
			AutoSync:                   opts.AutoSync,
			SyncInterval:               opts.SyncInterval,
			CompactionInterval:         opts.CompactionInterval,
			EnablePriorities:           opts.EnablePriorities,
			PriorityStarvationWindow:   opts.PriorityStarvationWindow,
			DLQPath:                    opts.DLQPath,
			MaxRetries:                 opts.MaxRetries,
			MaxMessageSize:             opts.MaxMessageSize,
			MinFreeDiskSpace:           opts.MinFreeDiskSpace,
			DLQMaxAge:                  opts.DLQMaxAge,
			DLQMaxSize:                 opts.DLQMaxSize,
			DefaultCompression:         format.CompressionType(opts.DefaultCompression),
			CompressionLevel:           opts.CompressionLevel,
			MinCompressionSize:         opts.MinCompressionSize,
			DefaultDeduplicationWindow: opts.DefaultDeduplicationWindow,
			MaxDeduplicationEntries:    opts.MaxDeduplicationEntries,
			Logger:                     convertLogger(opts.Logger),
			MetricsCollector:           metricsCollector,
		}
	}

	q, err := queue.Open(dir, qopts)
	if err != nil {
		return nil, err
	}

	return &Queue{q: q}, nil
}

// Enqueue appends a message to the queue.
// Returns the offset where the message was written.
func (q *Queue) Enqueue(payload []byte) (uint64, error) {
	return q.q.Enqueue(payload)
}

// EnqueueWithTTL appends a message to the queue with a time-to-live duration.
// The message will expire after the specified TTL and will be skipped during dequeue.
// Returns the offset where the message was written.
func (q *Queue) EnqueueWithTTL(payload []byte, ttl time.Duration) (uint64, error) {
	return q.q.EnqueueWithTTL(payload, ttl)
}

// EnqueueWithHeaders appends a message to the queue with key-value metadata headers.
// Headers can be used for routing, tracing, content-type indication, or message classification.
// Returns the offset where the message was written.
func (q *Queue) EnqueueWithHeaders(payload []byte, headers map[string]string) (uint64, error) {
	return q.q.EnqueueWithHeaders(payload, headers)
}

// EnqueueWithOptions appends a message with both TTL and headers.
// This allows combining multiple features in a single enqueue operation.
// Returns the offset where the message was written.
func (q *Queue) EnqueueWithOptions(payload []byte, ttl time.Duration, headers map[string]string) (uint64, error) {
	return q.q.EnqueueWithOptions(payload, ttl, headers)
}

// EnqueueWithPriority appends a message with a specific priority level (v1.1.0+).
// Priority determines the order in which messages are dequeued when EnablePriorities is true.
// When EnablePriorities is false, priority is ignored and FIFO order is maintained.
// Returns the offset where the message was written.
func (q *Queue) EnqueueWithPriority(payload []byte, priority Priority) (uint64, error) {
	return q.q.EnqueueWithPriority(payload, uint8(priority))
}

// EnqueueWithCompression appends a message with explicit compression (v1.3.0+).
// This allows overriding the queue's DefaultCompression setting for individual messages.
// Returns the offset where the message was written.
func (q *Queue) EnqueueWithCompression(payload []byte, compression CompressionType) (uint64, error) {
	return q.q.EnqueueWithCompression(payload, format.CompressionType(compression))
}

// EnqueueWithDedup appends a message with deduplication tracking (v1.4.0+).
// If a message with the same dedupID was enqueued within the deduplication window,
// this returns the original message ID and offset without writing a duplicate.
// Returns (offset, isDuplicate, error).
//
// dedupID: A unique identifier for this message (e.g., order ID, request ID)
// window: How long to track this message for deduplication (0 = use queue default)
//
// Example:
//
//	offset, isDup, err := q.EnqueueWithDedup(payload, "order-12345", 5*time.Minute)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	if isDup {
//	    log.Printf("Duplicate message detected, original at offset %d", offset)
//	} else {
//	    log.Printf("New message enqueued at offset %d", offset)
//	}
func (q *Queue) EnqueueWithDedup(payload []byte, dedupID string, window time.Duration) (uint64, bool, error) {
	return q.q.EnqueueWithDedup(payload, dedupID, window)
}

// EnqueueWithAllOptions appends a message with priority, TTL, headers, and compression (v1.1.0+, v1.3.0+).
// This is the most flexible enqueue method, combining all available features.
// Returns the offset where the message was written.
func (q *Queue) EnqueueWithAllOptions(payload []byte, opts EnqueueOptions) (uint64, error) {
	// Map public options to internal batch options
	batchOpts := []queue.BatchEnqueueOptions{
		{
			Payload:     payload,
			Priority:    uint8(opts.Priority),
			TTL:         opts.TTL,
			Headers:     opts.Headers,
			Compression: format.CompressionType(opts.Compression),
		},
	}
	offsets, err := q.q.EnqueueBatchWithOptions(batchOpts)
	if err != nil {
		return 0, err
	}
	return offsets[0], nil
}

// EnqueueBatch appends multiple messages to the queue in a single operation.
// This is more efficient than calling Enqueue() multiple times.
// Returns the offsets where the messages were written.
// Note: All messages in the batch have default priority (PriorityLow).
// Use EnqueueBatchWithOptions for priority support.
func (q *Queue) EnqueueBatch(payloads [][]byte) ([]uint64, error) {
	return q.q.EnqueueBatch(payloads)
}

// BatchEnqueueOptions contains options for enqueueing a single message in a batch operation (v1.1.0+, v1.3.0+).
type BatchEnqueueOptions struct {
	// Payload is the message data
	Payload []byte

	// Priority is the message priority level
	// Default: PriorityLow (0)
	Priority Priority

	// TTL is the time-to-live duration for the message
	// Set to 0 for no expiration
	// Default: 0 (no expiration)
	TTL time.Duration

	// Headers contains key-value metadata for the message
	// Default: nil
	Headers map[string]string

	// Compression is the compression type for the message (v1.3.0+)
	// Set to CompressionNone to use queue's DefaultCompression
	// Default: CompressionNone (uses queue default)
	Compression CompressionType
}

// EnqueueBatchWithOptions appends multiple messages with individual options to the queue (v1.1.0+).
// This is more efficient than calling EnqueueWithAllOptions() multiple times as it performs
// a single fsync for all messages. Each message can have different priority, TTL, and headers.
// Returns the offsets where the messages were written.
func (q *Queue) EnqueueBatchWithOptions(messages []BatchEnqueueOptions) ([]uint64, error) {
	// Convert public options to internal options
	internalMessages := make([]queue.BatchEnqueueOptions, len(messages))
	for i, msg := range messages {
		internalMessages[i] = queue.BatchEnqueueOptions{
			Payload:     msg.Payload,
			Priority:    uint8(msg.Priority),
			TTL:         msg.TTL,
			Headers:     msg.Headers,
			Compression: format.CompressionType(msg.Compression),
		}
	}

	return q.q.EnqueueBatchWithOptions(internalMessages)
}

// Dequeue retrieves the next message from the queue.
// Returns an error if no messages are available.
// Automatically skips expired messages with TTL.
// When EnablePriorities is true, returns the highest priority message first.
func (q *Queue) Dequeue() (*Message, error) {
	msg, err := q.q.Dequeue()
	if err != nil {
		return nil, err
	}

	return &Message{
		ID:        msg.ID,
		Offset:    msg.Offset,
		Payload:   msg.Payload,
		Timestamp: msg.Timestamp,
		ExpiresAt: msg.ExpiresAt,
		Priority:  Priority(msg.Priority),
		Headers:   msg.Headers,
	}, nil
}

// DequeueBatch retrieves up to maxMessages from the queue in a single operation.
// Returns fewer messages if the queue has fewer than maxMessages available.
// Automatically skips expired messages with TTL.
//
// Note: DequeueBatch always returns messages in FIFO order (by message ID), even when
// EnablePriorities is true. For priority-aware consumption, use Dequeue() in a loop or
// the Stream() API. This is a performance trade-off: batch dequeue optimizes for
// sequential I/O rather than priority ordering.
func (q *Queue) DequeueBatch(maxMessages int) ([]*Message, error) {
	msgs, err := q.q.DequeueBatch(maxMessages)
	if err != nil {
		return nil, err
	}

	result := make([]*Message, len(msgs))
	for i, msg := range msgs {
		result[i] = &Message{
			ID:        msg.ID,
			Offset:    msg.Offset,
			Payload:   msg.Payload,
			Timestamp: msg.Timestamp,
			ExpiresAt: msg.ExpiresAt,
			Priority:  Priority(msg.Priority),
			Headers:   msg.Headers,
		}
	}

	return result, nil
}

// SeekToMessageID sets the read position to a specific message ID.
// Subsequent Dequeue() calls will start reading from this message.
func (q *Queue) SeekToMessageID(msgID uint64) error {
	return q.q.SeekToMessageID(msgID)
}

// SeekToTimestamp sets the read position to the first message at or after the given timestamp.
// The timestamp should be in Unix nanoseconds.
func (q *Queue) SeekToTimestamp(timestamp int64) error {
	return q.q.SeekToTimestamp(timestamp)
}

// Sync forces a sync of pending writes to disk.
func (q *Queue) Sync() error {
	return q.q.Sync()
}

// Close closes the queue and releases all resources.
func (q *Queue) Close() error {
	return q.q.Close()
}

// Stats returns current queue statistics.
func (q *Queue) Stats() *Stats {
	stats := q.q.Stats()
	return &Stats{
		TotalMessages:        stats.TotalMessages,
		PendingMessages:      stats.PendingMessages,
		NextMessageID:        stats.NextMessageID,
		ReadMessageID:        stats.ReadMessageID,
		SegmentCount:         stats.SegmentCount,
		DLQMessages:          stats.DLQMessages,
		DLQPendingMessages:   stats.DLQPendingMessages,
		RetryTrackedMessages: stats.RetryTrackedMessages,
		DedupTrackedEntries:  stats.DedupTrackedEntries,
	}
}

// Compact manually triggers compaction of old segments based on retention policy.
// Returns the number of segments removed and bytes freed.
func (q *Queue) Compact() (*CompactionResult, error) {
	result, err := q.q.Compact()
	if err != nil {
		return nil, err
	}

	return &CompactionResult{
		SegmentsRemoved: result.SegmentsRemoved,
		BytesFreed:      result.BytesFreed,
	}, nil
}

// StreamHandler is called for each message in the stream.
// Return an error to stop streaming.
type StreamHandler func(*Message) error

// Stream continuously reads messages from the queue and calls the handler for each message.
// Streaming continues until the context is cancelled, an error occurs, or no more messages are available.
//
// The Stream method polls for new messages with a configurable interval (100ms by default).
// When a message is available, it's immediately passed to the handler.
// If no messages are available, Stream waits briefly before checking again.
//
// Context cancellation will gracefully stop streaming and return context.Canceled.
// Handler errors will stop streaming and return the handler error.
//
// Example usage:
//
//	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
//	defer cancel()
//
//	err := q.Stream(ctx, func(msg *Message) error {
//	    fmt.Printf("Received: %s\n", msg.Payload)
//	    return nil
//	})
func (q *Queue) Stream(ctx context.Context, handler StreamHandler) error {
	// Convert public handler to internal handler
	internalHandler := func(msg *queue.Message) error {
		return handler(&Message{
			ID:        msg.ID,
			Offset:    msg.Offset,
			Payload:   msg.Payload,
			Timestamp: msg.Timestamp,
			ExpiresAt: msg.ExpiresAt,
			Priority:  Priority(msg.Priority),
			Headers:   msg.Headers,
		})
	}

	return q.q.Stream(ctx, internalHandler)
}

// Ack acknowledges successful processing of a message (v1.2.0+).
// When DLQ is enabled, this removes the message from retry tracking.
// If DLQ is not configured, this method is a no-op.
//
// This should be called after successfully processing a dequeued message
// to indicate that the message does not need to be retried.
//
// Example usage:
//
//	msg, err := q.Dequeue()
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	// Process the message
//	if err := processMessage(msg); err != nil {
//	    // Processing failed - report failure
//	    q.Nack(msg.ID, err.Error())
//	} else {
//	    // Processing succeeded - acknowledge
//	    q.Ack(msg.ID)
//	}
func (q *Queue) Ack(msgID uint64) error {
	return q.q.Ack(msgID)
}

// Nack reports a message processing failure (v1.2.0+).
// When DLQ is enabled, this increments the retry count and potentially moves
// the message to the dead letter queue if max retries are exceeded.
// If DLQ is not configured, this method is a no-op.
//
// The reason parameter should describe why the message processing failed.
// This reason is stored with the retry metadata for debugging purposes.
//
// When a message exceeds the configured MaxRetries, it is automatically moved
// to the DLQ with metadata headers containing:
//   - dlq.original_msg_id: Original message ID from the main queue
//   - dlq.retry_count: Number of failed attempts
//   - dlq.failure_reason: Last failure reason provided to Nack()
//   - dlq.last_failure: Timestamp of the last failure
func (q *Queue) Nack(msgID uint64, reason string) error {
	return q.q.Nack(msgID, reason)
}

// GetDLQ returns the dead letter queue for inspection (v1.2.0+).
// Returns nil if DLQ is not configured.
// The returned queue can be used to inspect or dequeue messages from the DLQ.
//
// Example usage:
//
//	dlq := q.GetDLQ()
//	if dlq != nil {
//	    stats := dlq.Stats()
//	    fmt.Printf("DLQ has %d pending messages\n", stats.PendingMessages)
//
//	    // Inspect DLQ messages
//	    msg, err := dlq.Dequeue()
//	    if err == nil {
//	        fmt.Printf("Failed message: %s\n", msg.Payload)
//	        fmt.Printf("Failure reason: %s\n", msg.Headers["dlq.failure_reason"])
//	    }
//	}
func (q *Queue) GetDLQ() *Queue {
	dlq := q.q.GetDLQ()
	if dlq == nil {
		return nil
	}
	return &Queue{q: dlq}
}

// RequeueFromDLQ moves a message from the DLQ back to the main queue (v1.2.0+).
// The message ID should be from the DLQ (not the original message ID).
// Returns an error if DLQ is not configured or the message is not found.
//
// The message is enqueued to the main queue with its original payload and headers,
// but DLQ-specific metadata headers are removed.
//
// Example usage:
//
//	dlq := q.GetDLQ()
//	if dlq != nil {
//	    msg, err := dlq.Dequeue()
//	    if err == nil {
//	        // Requeue the message back to main queue
//	        if err := q.RequeueFromDLQ(msg.ID); err != nil {
//	            log.Printf("Failed to requeue: %v", err)
//	        }
//	    }
//	}
func (q *Queue) RequeueFromDLQ(dlqMsgID uint64) error {
	return q.q.RequeueFromDLQ(dlqMsgID)
}

// RetryInfo contains retry metadata for a message (v1.2.0+).
type RetryInfo struct {
	// MessageID is the message ID being tracked
	MessageID uint64

	// RetryCount is the number of times Nack() has been called for this message
	RetryCount int

	// LastFailure is the timestamp of the most recent Nack() call
	LastFailure time.Time

	// FailureReason is the reason string from the most recent Nack() call
	FailureReason string
}

// GetRetryInfo returns retry information for a message (v1.2.0+).
// Returns nil if DLQ is not configured or if the message has no retry tracking.
// This is useful for implementing custom retry logic and backoff strategies.
//
// The returned RetryInfo contains:
//   - MessageID: The message ID being tracked
//   - RetryCount: Number of times Nack() has been called for this message
//   - LastFailure: Timestamp of the most recent Nack() call
//   - FailureReason: Reason string from the most recent Nack() call
//
// Example usage:
//
//	msg, _ := q.Dequeue()
//	info := q.GetRetryInfo(msg.ID)
//	if info != nil && info.RetryCount > 0 {
//	    // Calculate exponential backoff based on retry count
//	    backoff := CalculateBackoff(info.RetryCount, time.Second, 5*time.Minute)
//	    log.Printf("Message has failed %d times, waiting %v before retry",
//	        info.RetryCount, backoff)
//	    time.Sleep(backoff)
//	}
//
//	// Process the message
//	if err := processMessage(msg.Payload); err != nil {
//	    q.Nack(msg.ID, err.Error())
//	} else {
//	    q.Ack(msg.ID)
//	}
func (q *Queue) GetRetryInfo(msgID uint64) *RetryInfo {
	info := q.q.GetRetryInfo(msgID)
	if info == nil {
		return nil
	}

	return &RetryInfo{
		MessageID:     info.MessageID,
		RetryCount:    info.RetryCount,
		LastFailure:   info.LastFailure,
		FailureReason: info.FailureReason,
	}
}

// CalculateBackoff calculates exponential backoff duration based on retry count (v1.2.0+).
// This is a helper function for implementing retry logic with the DLQ system.
// The backoff duration increases exponentially: baseDelay * 2^retryCount, capped at maxBackoff.
//
// Parameters:
//   - retryCount: Number of previous retry attempts (typically from RetryInfo.RetryCount)
//   - baseDelay: Base delay for the first retry (e.g., 1 second)
//   - maxBackoff: Maximum backoff duration to prevent excessively long waits
//
// Example usage:
//
//	msg, _ := q.Dequeue()
//	if info := q.GetRetryInfo(msg.ID); info != nil && info.RetryCount > 0 {
//	    backoff := CalculateBackoff(info.RetryCount, time.Second, 5*time.Minute)
//	    time.Sleep(backoff)
//	}
//
// Returns the calculated backoff duration, always between baseDelay and maxBackoff.
func CalculateBackoff(retryCount int, baseDelay, maxBackoff time.Duration) time.Duration {
	return queue.CalculateBackoff(retryCount, baseDelay, maxBackoff)
}

// Helper functions to convert between public and internal types

func convertSegmentOptions(dir string, opts *Options) *segment.ManagerOptions {
	segOpts := segment.DefaultManagerOptions(dir)

	if opts.MaxSegmentSize > 0 {
		segOpts.MaxSegmentSize = opts.MaxSegmentSize
	}

	if opts.MaxSegmentMessages > 0 {
		segOpts.MaxSegmentMessages = opts.MaxSegmentMessages
	}

	switch opts.RotationPolicy {
	case RotateBySize:
		segOpts.RotationPolicy = segment.RotateBySize
	case RotateByCount:
		segOpts.RotationPolicy = segment.RotateByCount
	case RotateByBoth:
		segOpts.RotationPolicy = segment.RotateByBoth
	}

	if opts.RetentionPolicy != nil {
		segOpts.RetentionPolicy = &segment.RetentionPolicy{
			MaxAge:      opts.RetentionPolicy.MaxAge,
			MaxSize:     opts.RetentionPolicy.MaxSize,
			MaxSegments: opts.RetentionPolicy.MaxSegments,
			MinSegments: opts.RetentionPolicy.MinSegments,
		}
	}

	return segOpts
}

func convertLogger(l Logger) logging.Logger {
	if l == nil {
		return logging.NoopLogger{}
	}
	return &loggerAdapter{l: l}
}

// loggerAdapter adapts public Logger to internal logging.Logger
type loggerAdapter struct {
	l Logger
}

func (a *loggerAdapter) Debug(msg string, fields ...logging.Field) {
	a.l.Debug(msg, convertFields(fields)...)
}

func (a *loggerAdapter) Info(msg string, fields ...logging.Field) {
	a.l.Info(msg, convertFields(fields)...)
}

func (a *loggerAdapter) Warn(msg string, fields ...logging.Field) {
	a.l.Warn(msg, convertFields(fields)...)
}

func (a *loggerAdapter) Error(msg string, fields ...logging.Field) {
	a.l.Error(msg, convertFields(fields)...)
}

func convertFields(fields []logging.Field) []LogField {
	result := make([]LogField, len(fields))
	for i, f := range fields {
		result[i] = LogField{Key: f.Key, Value: f.Value}
	}
	return result
}
