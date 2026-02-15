// Package queue provides message enqueue operations.
// This file contains all variants of enqueue functionality including batch operations.
package queue

import (
	"fmt"
	"time"

	"github.com/1mb-dev/ledgerq/internal/format"
	"github.com/1mb-dev/ledgerq/internal/logging"
)

// Enqueue appends a message to the queue.
// Returns the offset where the message was written.
func (q *Queue) Enqueue(payload []byte) (uint64, error) {
	start := time.Now()

	// Validate message size before acquiring lock
	if err := validateMessageSize(payload, q.opts.MaxMessageSize); err != nil {
		return 0, err
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		q.opts.MetricsCollector.RecordEnqueueError()
		return 0, fmt.Errorf("queue is closed")
	}

	msgID := q.nextMsgID

	// Apply compression if default compression is enabled
	compressedPayload, compressionType, err := compressPayloadIfNeeded(
		payload,
		q.opts.DefaultCompression,
		q.opts.CompressionLevel,
		q.opts.MinCompressionSize,
		q.opts.Logger,
	)
	if err != nil {
		q.opts.MetricsCollector.RecordEnqueueError()
		q.opts.Logger.Error("compression failed",
			logging.F("msg_id", msgID),
			logging.F("error", err.Error()),
		)
		return 0, fmt.Errorf("failed to compress payload: %w", err)
	}

	// Create entry
	entry := &format.Entry{
		Type:        format.EntryTypeData,
		Flags:       format.EntryFlagNone,
		MsgID:       msgID,
		Timestamp:   time.Now().UnixNano(),
		Compression: compressionType,
		Payload:     compressedPayload,
	}

	// Write to segment
	offset, err := q.segments.Write(entry)
	if err != nil {
		q.opts.MetricsCollector.RecordEnqueueError()
		q.opts.Logger.Error("enqueue failed",
			logging.F("msg_id", msgID),
			logging.F("error", err.Error()),
		)
		return 0, fmt.Errorf("failed to write entry: %w", err)
	}

	// Add to priority index if priority mode is enabled
	if q.opts.EnablePriorities && q.priorityIndex != nil {
		position := uint32(offset) //nolint:gosec // G115: Offset won't exceed uint32 in practice
		q.priorityIndex.Insert(offset, position, entry.Priority, entry.Timestamp)
	}

	// Increment message ID
	q.nextMsgID++

	// Update metadata
	if err := q.metadata.SetNextMsgID(q.nextMsgID); err != nil {
		return offset, fmt.Errorf("failed to update metadata: %w", err)
	}

	// Sync if auto-sync is enabled
	if q.opts.AutoSync {
		if err := q.segments.Sync(); err != nil {
			return offset, fmt.Errorf("failed to sync: %w", err)
		}
	}

	q.opts.Logger.Debug("message enqueued",
		logging.F("msg_id", msgID),
		logging.F("offset", offset),
		logging.F("payload_size", len(payload)),
	)

	// Record metrics
	q.opts.MetricsCollector.RecordEnqueue(len(payload), time.Since(start))

	return offset, nil
}

// EnqueueWithTTL appends a message to the queue with a time-to-live duration.
// The message will expire after the specified TTL and will be skipped during dequeue.
// Returns the offset where the message was written.
func (q *Queue) EnqueueWithTTL(payload []byte, ttl time.Duration) (uint64, error) {
	start := time.Now()

	// Validate message size before acquiring lock
	if err := validateMessageSize(payload, q.opts.MaxMessageSize); err != nil {
		return 0, err
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		q.opts.MetricsCollector.RecordEnqueueError()
		return 0, fmt.Errorf("queue is closed")
	}

	if ttl <= 0 {
		return 0, fmt.Errorf("TTL must be positive")
	}

	msgID := q.nextMsgID
	now := time.Now().UnixNano()

	// Create entry with TTL
	entry := &format.Entry{
		Type:      format.EntryTypeData,
		Flags:     format.EntryFlagTTL,
		MsgID:     msgID,
		Timestamp: now,
		ExpiresAt: now + ttl.Nanoseconds(),
		Payload:   payload,
	}

	// Write to segment
	offset, err := q.segments.Write(entry)
	if err != nil {
		q.opts.MetricsCollector.RecordEnqueueError()
		q.opts.Logger.Error("enqueue with TTL failed",
			logging.F("msg_id", msgID),
			logging.F("ttl", ttl.String()),
			logging.F("error", err.Error()),
		)
		return 0, fmt.Errorf("failed to write entry: %w", err)
	}

	// Add to priority index if priority mode is enabled
	if q.opts.EnablePriorities && q.priorityIndex != nil {
		position := uint32(offset) //nolint:gosec // G115: Offset won't exceed uint32 in practice
		q.priorityIndex.Insert(offset, position, entry.Priority, entry.Timestamp)
	}

	// Increment message ID
	q.nextMsgID++

	// Update metadata
	if err := q.metadata.SetNextMsgID(q.nextMsgID); err != nil {
		return offset, fmt.Errorf("failed to update metadata: %w", err)
	}

	// Sync if auto-sync is enabled
	if q.opts.AutoSync {
		if err := q.segments.Sync(); err != nil {
			return offset, fmt.Errorf("failed to sync: %w", err)
		}
	}

	q.opts.Logger.Debug("message enqueued with TTL",
		logging.F("msg_id", msgID),
		logging.F("offset", offset),
		logging.F("payload_size", len(payload)),
		logging.F("ttl", ttl.String()),
		logging.F("expires_at", entry.ExpiresAt),
	)

	// Record metrics
	q.opts.MetricsCollector.RecordEnqueue(len(payload), time.Since(start))

	return offset, nil
}

// EnqueueWithHeaders appends a message to the queue with key-value metadata headers.
// Headers can be used for routing, tracing, content-type indication, or message classification.
// Returns the offset where the message was written.
func (q *Queue) EnqueueWithHeaders(payload []byte, headers map[string]string) (uint64, error) {
	start := time.Now()

	// Validate message size before acquiring lock
	if err := validateMessageSize(payload, q.opts.MaxMessageSize); err != nil {
		return 0, err
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		q.opts.MetricsCollector.RecordEnqueueError()
		return 0, fmt.Errorf("queue is closed")
	}

	msgID := q.nextMsgID

	// Create entry with headers (flag will be set by Marshal if headers present)
	entry := &format.Entry{
		Type:      format.EntryTypeData,
		Flags:     format.EntryFlagNone,
		MsgID:     msgID,
		Timestamp: time.Now().UnixNano(),
		Headers:   headers,
		Payload:   payload,
	}

	// Write to segment
	offset, err := q.segments.Write(entry)
	if err != nil {
		q.opts.MetricsCollector.RecordEnqueueError()
		q.opts.Logger.Error("enqueue with headers failed",
			logging.F("msg_id", msgID),
			logging.F("headers_count", len(headers)),
			logging.F("error", err.Error()),
		)
		return 0, fmt.Errorf("failed to write entry: %w", err)
	}

	// Add to priority index if priority mode is enabled
	if q.opts.EnablePriorities && q.priorityIndex != nil {
		position := uint32(offset) //nolint:gosec // G115: Offset won't exceed uint32 in practice
		q.priorityIndex.Insert(offset, position, entry.Priority, entry.Timestamp)
	}

	// Increment message ID
	q.nextMsgID++

	// Update metadata
	if err := q.metadata.SetNextMsgID(q.nextMsgID); err != nil {
		return offset, fmt.Errorf("failed to update metadata: %w", err)
	}

	// Sync if auto-sync is enabled
	if q.opts.AutoSync {
		if err := q.segments.Sync(); err != nil {
			return offset, fmt.Errorf("failed to sync: %w", err)
		}
	}

	q.opts.Logger.Debug("message enqueued with headers",
		logging.F("msg_id", msgID),
		logging.F("offset", offset),
		logging.F("payload_size", len(payload)),
		logging.F("headers_count", len(headers)),
	)

	// Record metrics
	q.opts.MetricsCollector.RecordEnqueue(len(payload), time.Since(start))

	return offset, nil
}

// EnqueueWithOptions appends a message with both TTL and headers.
// This allows combining multiple features in a single enqueue operation.
// Returns the offset where the message was written.
func (q *Queue) EnqueueWithOptions(payload []byte, ttl time.Duration, headers map[string]string) (uint64, error) {
	start := time.Now()

	// Validate message size before acquiring lock
	if err := validateMessageSize(payload, q.opts.MaxMessageSize); err != nil {
		return 0, err
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		q.opts.MetricsCollector.RecordEnqueueError()
		return 0, fmt.Errorf("queue is closed")
	}

	if ttl <= 0 {
		return 0, fmt.Errorf("TTL must be positive")
	}

	msgID := q.nextMsgID
	now := time.Now().UnixNano()

	// Create entry with both TTL and headers (flags will be set by Marshal)
	entry := &format.Entry{
		Type:      format.EntryTypeData,
		Flags:     format.EntryFlagNone,
		MsgID:     msgID,
		Timestamp: now,
		ExpiresAt: now + ttl.Nanoseconds(),
		Headers:   headers,
		Payload:   payload,
	}

	// Write to segment
	offset, err := q.segments.Write(entry)
	if err != nil {
		q.opts.MetricsCollector.RecordEnqueueError()
		q.opts.Logger.Error("enqueue with options failed",
			logging.F("msg_id", msgID),
			logging.F("ttl", ttl.String()),
			logging.F("headers_count", len(headers)),
			logging.F("error", err.Error()),
		)
		return 0, fmt.Errorf("failed to write entry: %w", err)
	}

	// Add to priority index if priority mode is enabled
	if q.opts.EnablePriorities && q.priorityIndex != nil {
		position := uint32(offset) //nolint:gosec // G115: Offset won't exceed uint32 in practice
		q.priorityIndex.Insert(offset, position, entry.Priority, entry.Timestamp)
	}

	// Increment message ID
	q.nextMsgID++

	// Update metadata
	if err := q.metadata.SetNextMsgID(q.nextMsgID); err != nil {
		return offset, fmt.Errorf("failed to update metadata: %w", err)
	}

	// Sync if auto-sync is enabled
	if q.opts.AutoSync {
		if err := q.segments.Sync(); err != nil {
			return offset, fmt.Errorf("failed to sync: %w", err)
		}
	}

	q.opts.Logger.Debug("message enqueued with TTL and headers",
		logging.F("msg_id", msgID),
		logging.F("offset", offset),
		logging.F("payload_size", len(payload)),
		logging.F("ttl", ttl.String()),
		logging.F("headers_count", len(headers)),
		logging.F("expires_at", entry.ExpiresAt),
	)

	// Record metrics
	q.opts.MetricsCollector.RecordEnqueue(len(payload), time.Since(start))

	return offset, nil
}

// EnqueueWithPriority appends a message with a specific priority level (v1.1.0+).
// Returns the offset where the message was written.
func (q *Queue) EnqueueWithPriority(payload []byte, priority uint8) (uint64, error) {
	start := time.Now()

	// Validate message size before acquiring lock
	if err := validateMessageSize(payload, q.opts.MaxMessageSize); err != nil {
		return 0, err
	}

	// Validate priority before acquiring lock
	if priority > 2 {
		return 0, fmt.Errorf("invalid priority %d (must be 0-2)", priority)
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		q.opts.MetricsCollector.RecordEnqueueError()
		return 0, fmt.Errorf("queue is closed")
	}

	msgID := q.nextMsgID

	// Create entry with priority (flag will be set by Marshal if not PriorityLow)
	entry := &format.Entry{
		Type:      format.EntryTypeData,
		Flags:     format.EntryFlagNone,
		MsgID:     msgID,
		Timestamp: time.Now().UnixNano(),
		Priority:  priority,
		Payload:   payload,
	}

	// Write to segment
	offset, err := q.segments.Write(entry)
	if err != nil {
		q.opts.MetricsCollector.RecordEnqueueError()
		q.opts.Logger.Error("enqueue with priority failed",
			logging.F("msg_id", msgID),
			logging.F("priority", priority),
			logging.F("error", err.Error()),
		)
		return 0, fmt.Errorf("failed to write entry: %w", err)
	}

	// Add to priority index if priority mode is enabled
	if q.opts.EnablePriorities && q.priorityIndex != nil {
		position := uint32(offset) //nolint:gosec // G115: Offset won't exceed uint32 in practice
		q.priorityIndex.Insert(offset, position, entry.Priority, entry.Timestamp)
	}

	// Increment message ID
	q.nextMsgID++

	// Update metadata
	if err := q.metadata.SetNextMsgID(q.nextMsgID); err != nil {
		return offset, fmt.Errorf("failed to update metadata: %w", err)
	}

	// Sync if auto-sync is enabled
	if q.opts.AutoSync {
		if err := q.segments.Sync(); err != nil {
			return offset, fmt.Errorf("failed to sync: %w", err)
		}
	}

	q.opts.Logger.Debug("message enqueued with priority",
		logging.F("msg_id", msgID),
		logging.F("offset", offset),
		logging.F("payload_size", len(payload)),
		logging.F("priority", priority),
	)

	// Record metrics
	q.opts.MetricsCollector.RecordEnqueue(len(payload), time.Since(start))

	return offset, nil
}

// EnqueueWithAllOptions appends a message with priority, TTL, and headers (v1.1.0+).
// This is the most flexible enqueue method, combining all available features.
// Returns the offset where the message was written.
func (q *Queue) EnqueueWithAllOptions(payload []byte, priority uint8, ttl time.Duration, headers map[string]string) (uint64, error) {
	start := time.Now()

	// Validate message size before acquiring lock
	if err := validateMessageSize(payload, q.opts.MaxMessageSize); err != nil {
		return 0, err
	}

	// Validate priority before acquiring lock
	if priority > 2 {
		return 0, fmt.Errorf("invalid priority %d (must be 0-2)", priority)
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		q.opts.MetricsCollector.RecordEnqueueError()
		return 0, fmt.Errorf("queue is closed")
	}

	msgID := q.nextMsgID
	now := time.Now().UnixNano()

	// Create entry with all options (flags will be set by Marshal)
	entry := &format.Entry{
		Type:      format.EntryTypeData,
		Flags:     format.EntryFlagNone,
		MsgID:     msgID,
		Timestamp: now,
		Priority:  priority,
		Payload:   payload,
	}

	// Set TTL if specified
	if ttl > 0 {
		entry.ExpiresAt = now + ttl.Nanoseconds()
	}

	// Set headers if specified
	if len(headers) > 0 {
		entry.Headers = headers
	}

	// Write to segment
	offset, err := q.segments.Write(entry)
	if err != nil {
		q.opts.MetricsCollector.RecordEnqueueError()
		q.opts.Logger.Error("enqueue with all options failed",
			logging.F("msg_id", msgID),
			logging.F("priority", priority),
			logging.F("ttl", ttl.String()),
			logging.F("headers_count", len(headers)),
			logging.F("error", err.Error()),
		)
		return 0, fmt.Errorf("failed to write entry: %w", err)
	}

	// Add to priority index if priority mode is enabled
	if q.opts.EnablePriorities && q.priorityIndex != nil {
		position := uint32(offset) //nolint:gosec // G115: Offset won't exceed uint32 in practice
		q.priorityIndex.Insert(offset, position, entry.Priority, entry.Timestamp)
	}

	// Increment message ID
	q.nextMsgID++

	// Update metadata
	if err := q.metadata.SetNextMsgID(q.nextMsgID); err != nil {
		return offset, fmt.Errorf("failed to update metadata: %w", err)
	}

	// Sync if auto-sync is enabled
	if q.opts.AutoSync {
		if err := q.segments.Sync(); err != nil {
			return offset, fmt.Errorf("failed to sync: %w", err)
		}
	}

	q.opts.Logger.Debug("message enqueued with all options",
		logging.F("msg_id", msgID),
		logging.F("offset", offset),
		logging.F("payload_size", len(payload)),
		logging.F("priority", priority),
		logging.F("ttl", ttl.String()),
		logging.F("headers_count", len(headers)),
		logging.F("expires_at", entry.ExpiresAt),
	)

	// Record metrics
	q.opts.MetricsCollector.RecordEnqueue(len(payload), time.Since(start))

	return offset, nil
}

// EnqueueBatch appends multiple messages to the queue in a single operation.
// This is more efficient than calling Enqueue() multiple times as it performs
// a single fsync for all messages.
// Returns the offsets where the messages were written.
func (q *Queue) EnqueueBatch(payloads [][]byte) ([]uint64, error) {
	start := time.Now()

	if len(payloads) == 0 {
		return nil, fmt.Errorf("empty batch")
	}

	// Validate all message sizes before acquiring lock
	for i, payload := range payloads {
		if err := validateMessageSize(payload, q.opts.MaxMessageSize); err != nil {
			return nil, fmt.Errorf("message %d: %w", i, err)
		}
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		q.opts.MetricsCollector.RecordEnqueueError()
		return nil, fmt.Errorf("queue is closed")
	}

	offsets := make([]uint64, len(payloads))
	timestamp := time.Now().UnixNano()
	totalBytes := 0

	// Write all entries
	for i, payload := range payloads {
		entry := &format.Entry{
			Type:      format.EntryTypeData,
			Flags:     format.EntryFlagNone,
			MsgID:     q.nextMsgID,
			Timestamp: timestamp,
			Payload:   payload,
		}

		offset, err := q.segments.Write(entry)
		if err != nil {
			// On error, sync what we've written so far
			_ = q.segments.Sync()
			q.opts.MetricsCollector.RecordEnqueueError()
			return offsets[:i], fmt.Errorf("failed to write entry %d: %w", i, err)
		}

		// Add to priority index if priority mode is enabled
		if q.opts.EnablePriorities && q.priorityIndex != nil {
			position := uint32(offset) //nolint:gosec // G115: Offset won't exceed uint32 in practice
			q.priorityIndex.Insert(offset, position, entry.Priority, entry.Timestamp)
		}

		offsets[i] = offset
		totalBytes += len(payload)
		q.nextMsgID++
	}

	// Update metadata with final nextMsgID
	if err := q.metadata.SetNextMsgID(q.nextMsgID); err != nil {
		return offsets, fmt.Errorf("failed to update metadata: %w", err)
	}

	// Single sync for the entire batch
	if q.opts.AutoSync {
		if err := q.segments.Sync(); err != nil {
			return offsets, fmt.Errorf("failed to sync batch: %w", err)
		}
	}

	q.opts.Logger.Debug("batch enqueued",
		logging.F("count", len(payloads)),
		logging.F("first_msg_id", q.nextMsgID-uint64(len(payloads))),
		logging.F("last_msg_id", q.nextMsgID-1),
	)

	// Record metrics
	q.opts.MetricsCollector.RecordEnqueueBatch(len(payloads), totalBytes, time.Since(start))

	return offsets, nil
}

// BatchEnqueueOptions contains options for enqueueing a single message in a batch operation.
type BatchEnqueueOptions struct {
	// Payload is the message data
	Payload []byte

	// Priority is the message priority level (v1.1.0+)
	// Default: PriorityLow (0)
	Priority uint8

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
	Compression format.CompressionType
}

// EnqueueBatchWithOptions appends multiple messages with individual options to the queue.
// This is more efficient than calling EnqueueWithAllOptions() multiple times as it performs
// a single fsync for all messages. Each message can have different priority, TTL, and headers.
// Returns the offsets where the messages were written.
func (q *Queue) EnqueueBatchWithOptions(messages []BatchEnqueueOptions) ([]uint64, error) {
	start := time.Now()

	if len(messages) == 0 {
		return nil, fmt.Errorf("empty batch")
	}

	// Validate all messages before acquiring lock
	for i, msg := range messages {
		if msg.Priority > 2 {
			return nil, fmt.Errorf("message %d: invalid priority %d (must be 0-2)", i, msg.Priority)
		}
		if len(msg.Payload) == 0 {
			return nil, fmt.Errorf("message %d: payload cannot be empty", i)
		}
		if err := validateMessageSize(msg.Payload, q.opts.MaxMessageSize); err != nil {
			return nil, fmt.Errorf("message %d: %w", i, err)
		}
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		q.opts.MetricsCollector.RecordEnqueueError()
		return nil, fmt.Errorf("queue is closed")
	}

	offsets := make([]uint64, len(messages))
	timestamp := time.Now().UnixNano()
	totalBytes := 0

	// Write all entries
	for i, msg := range messages {
		// Determine compression: use per-message setting or queue default
		compression := msg.Compression
		if compression == format.CompressionNone {
			compression = q.opts.DefaultCompression
		}

		// Apply compression
		compressedPayload, compressionType, err := compressPayloadIfNeeded(
			msg.Payload,
			compression,
			q.opts.CompressionLevel,
			q.opts.MinCompressionSize,
			q.opts.Logger,
		)
		if err != nil {
			// On error, sync what we've written so far
			_ = q.segments.Sync()
			q.opts.MetricsCollector.RecordEnqueueError()
			return offsets[:i], fmt.Errorf("failed to compress entry %d: %w", i, err)
		}

		entry := &format.Entry{
			Type:        format.EntryTypeData,
			Flags:       format.EntryFlagNone,
			MsgID:       q.nextMsgID,
			Timestamp:   timestamp,
			Priority:    msg.Priority,
			Compression: compressionType,
			Payload:     compressedPayload,
		}

		// Set TTL if specified
		if msg.TTL > 0 {
			entry.ExpiresAt = timestamp + msg.TTL.Nanoseconds()
		}

		// Set headers if specified
		if len(msg.Headers) > 0 {
			entry.Headers = msg.Headers
		}

		offset, err := q.segments.Write(entry)
		if err != nil {
			// On error, sync what we've written so far
			_ = q.segments.Sync()
			q.opts.MetricsCollector.RecordEnqueueError()
			return offsets[:i], fmt.Errorf("failed to write entry %d: %w", i, err)
		}

		// Add to priority index if priority mode is enabled
		if q.opts.EnablePriorities && q.priorityIndex != nil {
			position := uint32(offset) //nolint:gosec // G115: Offset won't exceed uint32 in practice
			q.priorityIndex.Insert(offset, position, entry.Priority, entry.Timestamp)
		}

		offsets[i] = offset
		totalBytes += len(msg.Payload)
		q.nextMsgID++
	}

	// Update metadata with final nextMsgID
	if err := q.metadata.SetNextMsgID(q.nextMsgID); err != nil {
		return offsets, fmt.Errorf("failed to update metadata: %w", err)
	}

	// Single sync for the entire batch
	if q.opts.AutoSync {
		if err := q.segments.Sync(); err != nil {
			return offsets, fmt.Errorf("failed to sync batch: %w", err)
		}
	}

	q.opts.Logger.Debug("batch enqueued with options",
		logging.F("count", len(messages)),
		logging.F("first_msg_id", q.nextMsgID-uint64(len(messages))),
		logging.F("last_msg_id", q.nextMsgID-1),
	)

	// Record metrics
	q.opts.MetricsCollector.RecordEnqueueBatch(len(messages), totalBytes, time.Since(start))

	return offsets, nil
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
//	if err != nil { ... }
//	if isDup {
//	    // Message was already processed, offset is the original message
//	}
func (q *Queue) EnqueueWithDedup(payload []byte, dedupID string, window time.Duration) (uint64, bool, error) {
	start := time.Now()

	// Validate dedup ID
	if dedupID == "" {
		return 0, false, fmt.Errorf("deduplication ID cannot be empty")
	}

	// Validate message size before acquiring lock
	if err := validateMessageSize(payload, q.opts.MaxMessageSize); err != nil {
		return 0, false, err
	}

	// Determine deduplication window
	if window == 0 {
		window = q.opts.DefaultDeduplicationWindow
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		q.opts.MetricsCollector.RecordEnqueueError()
		return 0, false, fmt.Errorf("queue is closed")
	}

	// Check for duplicate (if dedup is enabled)
	if q.dedupTracker != nil {
		if existingOffset, isDup := q.dedupTracker.Check(dedupID, window); isDup {
			q.opts.Logger.Debug("duplicate message detected",
				logging.F("dedup_id", dedupID),
				logging.F("original_offset", existingOffset),
			)
			return existingOffset, true, nil
		}
	}

	msgID := q.nextMsgID

	// Apply compression if default compression is enabled
	compressedPayload, compressionType, err := compressPayloadIfNeeded(
		payload,
		q.opts.DefaultCompression,
		q.opts.CompressionLevel,
		q.opts.MinCompressionSize,
		q.opts.Logger,
	)
	if err != nil {
		q.opts.MetricsCollector.RecordEnqueueError()
		q.opts.Logger.Error("compression failed",
			logging.F("msg_id", msgID),
			logging.F("dedup_id", dedupID),
			logging.F("error", err.Error()),
		)
		return 0, false, fmt.Errorf("failed to compress payload: %w", err)
	}

	// Create entry
	entry := &format.Entry{
		Type:        format.EntryTypeData,
		Flags:       format.EntryFlagNone,
		MsgID:       msgID,
		Timestamp:   time.Now().UnixNano(),
		Compression: compressionType,
		Payload:     compressedPayload,
	}

	// Write to segment
	offset, err := q.segments.Write(entry)
	if err != nil {
		q.opts.MetricsCollector.RecordEnqueueError()
		q.opts.Logger.Error("enqueue with dedup failed",
			logging.F("msg_id", msgID),
			logging.F("dedup_id", dedupID),
			logging.F("error", err.Error()),
		)
		return 0, false, fmt.Errorf("failed to write entry: %w", err)
	}

	// Track for deduplication (if enabled)
	if q.dedupTracker != nil {
		if err := q.dedupTracker.Track(dedupID, msgID, offset, window); err != nil {
			// Log error but don't fail the enqueue since message is already written
			q.opts.Logger.Error("failed to track deduplication",
				logging.F("msg_id", msgID),
				logging.F("offset", offset),
				logging.F("dedup_id", dedupID),
				logging.F("error", err.Error()),
			)
		}
	}

	// Add to priority index if priority mode is enabled
	if q.opts.EnablePriorities && q.priorityIndex != nil {
		position := uint32(offset) //nolint:gosec // G115: Offset won't exceed uint32 in practice
		q.priorityIndex.Insert(offset, position, entry.Priority, entry.Timestamp)
	}

	// Increment message ID
	q.nextMsgID++

	// Update metadata
	if err := q.metadata.SetNextMsgID(q.nextMsgID); err != nil {
		return offset, false, fmt.Errorf("failed to update metadata: %w", err)
	}

	// Sync if auto-sync is enabled
	if q.opts.AutoSync {
		if err := q.segments.Sync(); err != nil {
			return offset, false, fmt.Errorf("failed to sync: %w", err)
		}
	}

	q.opts.Logger.Debug("message enqueued with deduplication",
		logging.F("msg_id", msgID),
		logging.F("offset", offset),
		logging.F("payload_size", len(payload)),
		logging.F("dedup_id", dedupID),
		logging.F("window", window.String()),
	)

	// Record metrics
	q.opts.MetricsCollector.RecordEnqueue(len(payload), time.Since(start))

	return offset, false, nil
}

// EnqueueWithCompression appends a message with explicit compression (v1.3.0+).
// This allows overriding the queue's DefaultCompression setting for individual messages.
// Returns the offset where the message was written.
func (q *Queue) EnqueueWithCompression(payload []byte, compression format.CompressionType) (uint64, error) {
	start := time.Now()

	// Validate message size before acquiring lock
	if err := validateMessageSize(payload, q.opts.MaxMessageSize); err != nil {
		return 0, err
	}

	// Validate compression type
	if compression != format.CompressionNone && compression != format.CompressionGzip {
		return 0, fmt.Errorf("invalid compression type: %d", compression)
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		q.opts.MetricsCollector.RecordEnqueueError()
		return 0, fmt.Errorf("queue is closed")
	}

	msgID := q.nextMsgID

	// Apply compression
	compressedPayload, compressionType, err := compressPayloadIfNeeded(
		payload,
		compression,
		q.opts.CompressionLevel,
		q.opts.MinCompressionSize,
		q.opts.Logger,
	)
	if err != nil {
		q.opts.MetricsCollector.RecordEnqueueError()
		q.opts.Logger.Error("compression failed",
			logging.F("msg_id", msgID),
			logging.F("compression_type", compression.String()),
			logging.F("error", err.Error()),
		)
		return 0, fmt.Errorf("failed to compress payload: %w", err)
	}

	// Create entry
	entry := &format.Entry{
		Type:        format.EntryTypeData,
		Flags:       format.EntryFlagNone,
		MsgID:       msgID,
		Timestamp:   time.Now().UnixNano(),
		Compression: compressionType,
		Payload:     compressedPayload,
	}

	// Write to segment
	offset, err := q.segments.Write(entry)
	if err != nil {
		q.opts.MetricsCollector.RecordEnqueueError()
		q.opts.Logger.Error("enqueue with compression failed",
			logging.F("msg_id", msgID),
			logging.F("compression_type", compressionType.String()),
			logging.F("error", err.Error()),
		)
		return 0, fmt.Errorf("failed to write entry: %w", err)
	}

	// Add to priority index if priority mode is enabled
	if q.opts.EnablePriorities && q.priorityIndex != nil {
		position := uint32(offset) //nolint:gosec // G115: Offset won't exceed uint32 in practice
		q.priorityIndex.Insert(offset, position, entry.Priority, entry.Timestamp)
	}

	// Increment message ID
	q.nextMsgID++

	// Update metadata
	if err := q.metadata.SetNextMsgID(q.nextMsgID); err != nil {
		return offset, fmt.Errorf("failed to update metadata: %w", err)
	}

	// Sync if auto-sync is enabled
	if q.opts.AutoSync {
		if err := q.segments.Sync(); err != nil {
			return offset, fmt.Errorf("failed to sync: %w", err)
		}
	}

	q.opts.Logger.Debug("message enqueued with compression",
		logging.F("msg_id", msgID),
		logging.F("offset", offset),
		logging.F("original_size", len(payload)),
		logging.F("stored_size", len(compressedPayload)),
		logging.F("compression_type", compressionType.String()),
	)

	// Record metrics
	q.opts.MetricsCollector.RecordEnqueue(len(payload), time.Since(start))

	return offset, nil
}
