// Package queue provides dead letter queue (DLQ) operations.
// This file contains retry handling, acknowledgement, and DLQ management functionality.
package queue

import (
	"fmt"

	"github.com/1mb-dev/ledgerq/internal/format"
	"github.com/1mb-dev/ledgerq/internal/logging"
)

// Ack acknowledges a message processing success (v1.2.0+).
// When DLQ is enabled, this clears retry tracking for the message.
// If DLQ is not configured, this method is a no-op.
//
// Ack should be called after successfully processing a message from the queue
// to indicate that the message does not need to be retried.
func (q *Queue) Ack(msgID uint64) error {
	q.mu.RLock()
	retryTracker := q.retryTracker
	q.mu.RUnlock()

	// Check if DLQ is enabled
	if retryTracker == nil {
		return nil // No-op if DLQ not configured
	}

	return retryTracker.Ack(msgID)
}

// Nack reports a message processing failure (v1.2.0+).
// When DLQ is enabled, this increments the retry count and potentially moves
// the message to the dead letter queue if max retries are exceeded.
// If DLQ is not configured, this method is a no-op.
//
// The reason parameter should describe why the message processing failed.
// This reason is stored with the retry metadata for debugging purposes.
//
// Returns an error if the operation fails.
func (q *Queue) Nack(msgID uint64, reason string) error {
	q.mu.RLock()
	retryTracker := q.retryTracker
	q.mu.RUnlock()

	// Check if DLQ is enabled
	if retryTracker == nil {
		return nil // No-op if DLQ not configured
	}

	// Record the failure and check if max retries exceeded
	exceeded, err := retryTracker.Nack(msgID, reason)
	if err != nil {
		return fmt.Errorf("failed to record nack: %w", err)
	}

	// If max retries exceeded, move message to DLQ
	if exceeded {
		q.opts.Logger.Info("message exceeded max retries, moving to DLQ",
			logging.F("msg_id", msgID),
			logging.F("reason", reason),
		)

		// Move message to DLQ
		if err := q.moveToDLQ(msgID, reason); err != nil {
			return fmt.Errorf("failed to move message to DLQ: %w", err)
		}
	}

	return nil
}

// moveToDLQ moves a message from the main queue to the DLQ.
// The message is copied to the DLQ with additional metadata headers
// indicating the failure reason and retry count.
func (q *Queue) moveToDLQ(msgID uint64, reason string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	// Get retry information
	retryInfo := q.retryTracker.GetInfo(msgID)
	if retryInfo == nil {
		return fmt.Errorf("no retry information found for message %d", msgID)
	}

	// Ensure data is synced before searching
	if err := q.segments.Sync(); err != nil {
		return fmt.Errorf("failed to sync before DLQ move: %w", err)
	}

	// Find the message in segments
	allSegments := q.segments.GetSegments()
	activeSeg := q.segments.GetActiveSegment()
	if activeSeg != nil {
		allSegments = append(allSegments, activeSeg)
	}

	var entry *format.Entry
	found := false

	for _, seg := range allSegments {
		reader, err := q.segments.OpenReader(seg.BaseOffset)
		if err != nil {
			continue
		}

		// Search for the message
		e, _, _, err := reader.FindByMessageID(msgID)
		_ = reader.Close()

		if err == nil {
			entry = e
			found = true
			break
		}
	}

	if !found {
		return fmt.Errorf("message %d not found in queue segments", msgID)
	}

	// Prepare DLQ headers with retry metadata
	dlqHeaders := make(map[string]string)

	// Copy existing headers if present
	if entry.Headers != nil {
		for k, v := range entry.Headers {
			dlqHeaders[k] = v
		}
	}

	// Add DLQ-specific metadata (sanitize failure reason to prevent information leakage)
	dlqHeaders["dlq.original_msg_id"] = fmt.Sprintf("%d", msgID)
	dlqHeaders["dlq.retry_count"] = fmt.Sprintf("%d", retryInfo.RetryCount)
	dlqHeaders["dlq.failure_reason"] = sanitizeFailureReason(reason)
	dlqHeaders["dlq.last_failure"] = retryInfo.LastFailure.Format("2006-01-02T15:04:05.000Z07:00")

	// Enqueue to DLQ with original payload and enhanced headers
	_, err := q.dlq.EnqueueWithHeaders(entry.Payload, dlqHeaders)
	if err != nil {
		return fmt.Errorf("failed to enqueue message to DLQ: %w", err)
	}

	q.opts.Logger.Info("message moved to DLQ",
		logging.F("msg_id", msgID),
		logging.F("retry_count", retryInfo.RetryCount),
		logging.F("failure_reason", reason),
	)

	// Clean up retry tracking since the message is now in DLQ and won't be retried from main queue
	// We use Ack() to remove the retry tracking entry, as the message has been "processed" (moved to DLQ)
	if err := q.retryTracker.Ack(msgID); err != nil {
		q.opts.Logger.Error("failed to clean up retry tracking after DLQ move",
			logging.F("msg_id", msgID),
			logging.F("error", err.Error()),
		)
		// Don't return error - message is already safely in DLQ, tracking cleanup is non-critical
	}

	return nil
}

// GetDLQ returns the dead letter queue for inspection (v1.2.0+).
// Returns nil if DLQ is not configured.
// The returned queue can be used to inspect or dequeue messages from the DLQ.
func (q *Queue) GetDLQ() *Queue {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.dlq
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
//	    // Calculate backoff based on retry count
//	    backoff := time.Duration(1<<uint(info.RetryCount)) * time.Second
//	    time.Sleep(backoff)
//	}
func (q *Queue) GetRetryInfo(msgID uint64) *RetryInfo {
	q.mu.RLock()
	retryTracker := q.retryTracker
	q.mu.RUnlock()

	if retryTracker == nil {
		return nil // DLQ not configured
	}

	return retryTracker.GetInfo(msgID)
}

// RequeueFromDLQ moves a message from the DLQ back to the main queue (v1.2.0+).
// The message ID should be from the DLQ (not the original message ID).
// Returns an error if DLQ is not configured or the message is not found.
func (q *Queue) RequeueFromDLQ(dlqMsgID uint64) error {
	q.mu.RLock()
	dlq := q.dlq
	q.mu.RUnlock()

	if dlq == nil {
		return fmt.Errorf("DLQ not configured")
	}

	// Find the message in DLQ
	dlq.mu.Lock()
	allSegments := dlq.segments.GetSegments()
	activeSeg := dlq.segments.GetActiveSegment()
	if activeSeg != nil {
		allSegments = append(allSegments, activeSeg)
	}

	var entry *format.Entry
	found := false

	for _, seg := range allSegments {
		reader, err := dlq.segments.OpenReader(seg.BaseOffset)
		if err != nil {
			continue
		}

		// Search for the message
		e, _, _, err := reader.FindByMessageID(dlqMsgID)
		_ = reader.Close()

		if err == nil {
			entry = e
			found = true
			break
		}
	}
	dlq.mu.Unlock()

	if !found {
		return fmt.Errorf("message %d not found in DLQ", dlqMsgID)
	}

	// Enqueue back to main queue (preserving headers but removing DLQ metadata)
	headers := make(map[string]string)
	if entry.Headers != nil {
		for k, v := range entry.Headers {
			// Skip DLQ-specific headers when re-queuing
			if k != "dlq.original_msg_id" && k != "dlq.retry_count" &&
				k != "dlq.failure_reason" && k != "dlq.last_failure" {
				headers[k] = v
			}
		}
	}

	// Enqueue to main queue
	var err error
	if len(headers) > 0 {
		_, err = q.EnqueueWithHeaders(entry.Payload, headers)
	} else {
		_, err = q.Enqueue(entry.Payload)
	}

	if err != nil {
		return fmt.Errorf("failed to requeue message to main queue: %w", err)
	}

	q.opts.Logger.Info("message requeued from DLQ",
		logging.F("dlq_msg_id", dlqMsgID),
	)

	return nil
}
