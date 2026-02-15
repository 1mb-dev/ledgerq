// Package queue provides lifecycle management for queue operations.
// This file contains sync, stats, compaction, and timer management functionality.
package queue

import (
	"fmt"
	"time"

	"github.com/1mb-dev/ledgerq/internal/logging"
	"github.com/1mb-dev/ledgerq/internal/segment"
)

// Sync forces a sync of pending writes to disk.
func (q *Queue) Sync() error {
	q.mu.RLock()
	segments := q.segments
	q.mu.RUnlock()

	if segments == nil {
		return fmt.Errorf("queue is closed")
	}

	return segments.Sync()
}

// startSyncTimer starts the periodic sync timer.
func (q *Queue) startSyncTimer() {
	q.syncTimer = time.AfterFunc(q.opts.SyncInterval, func() {
		q.mu.RLock()
		if !q.closed {
			_ = q.segments.Sync() // Ignore error in background sync
		}
		closed := q.closed
		q.mu.RUnlock()

		// Reschedule if not closed
		if !closed {
			q.mu.Lock()
			if !q.closed {
				q.syncTimer.Reset(q.opts.SyncInterval)
			}
			q.mu.Unlock()
		}
	})
	q.syncTimerActive = true
}

// startCompactionTimer starts the periodic compaction timer.
func (q *Queue) startCompactionTimer() {
	q.compactionTimer = time.AfterFunc(q.opts.CompactionInterval, func() {
		start := time.Now()

		q.mu.RLock()
		if !q.closed {
			// Run compaction in background
			result, err := q.segments.Compact()
			if err != nil {
				q.opts.MetricsCollector.RecordCompactionError()
				q.opts.Logger.Error("background compaction failed",
					logging.F("error", err.Error()),
				)
			} else if result.SegmentsRemoved > 0 {
				q.opts.MetricsCollector.RecordCompaction(result.SegmentsRemoved, result.BytesFreed, time.Since(start))
				q.opts.Logger.Info("background compaction completed",
					logging.F("segments_removed", result.SegmentsRemoved),
					logging.F("bytes_freed", result.BytesFreed),
				)
			}
		}
		closed := q.closed
		q.mu.RUnlock()

		// Reschedule if not closed
		if !closed {
			q.mu.Lock()
			if !q.closed {
				q.compactionTimer.Reset(q.opts.CompactionInterval)
			}
			q.mu.Unlock()
		}
	})
	q.compactionTimerActive = true
}

// startDedupCleanup starts the periodic deduplication cleanup goroutine (v1.4.0+).
func (q *Queue) startDedupCleanup() {
	q.dedupCleanupTicker = time.NewTicker(10 * time.Second)
	q.dedupCleanupDone = make(chan bool)

	go func() {
		for {
			select {
			case <-q.dedupCleanupTicker.C:
				if q.dedupTracker != nil {
					removed := q.dedupTracker.CleanExpired()
					if removed > 0 {
						q.opts.Logger.Debug("dedup cleanup completed",
							logging.F("entries_removed", removed),
						)
					}
				}
			case <-q.dedupCleanupDone:
				return
			}
		}
	}()
}

// Stats returns queue statistics.
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

// Stats returns current queue statistics.
func (q *Queue) Stats() *Stats {
	q.mu.RLock()
	defer q.mu.RUnlock()

	segments := q.segments.GetSegments()
	if q.segments.GetActiveSegment() != nil {
		segments = append(segments, q.segments.GetActiveSegment())
	}

	stats := &Stats{
		TotalMessages:   q.nextMsgID - 1,
		PendingMessages: q.nextMsgID - q.readMsgID,
		NextMessageID:   q.nextMsgID,
		ReadMessageID:   q.readMsgID,
		SegmentCount:    len(segments),
	}

	// Populate DLQ statistics if DLQ is enabled (v1.2.0+)
	if q.dlq != nil {
		dlqStats := q.dlq.Stats()
		stats.DLQMessages = dlqStats.TotalMessages
		stats.DLQPendingMessages = dlqStats.PendingMessages
	}

	// Populate retry tracking statistics if enabled
	if q.retryTracker != nil {
		stats.RetryTrackedMessages = q.retryTracker.Count()
	}

	// Populate dedup statistics if enabled (v1.4.0+)
	if q.dedupTracker != nil {
		stats.DedupTrackedEntries = q.dedupTracker.ActiveCount()
	}

	return stats
}

// Compact manually triggers compaction of old segments based on retention policy.
// Returns the compaction result with segments removed and bytes freed.
func (q *Queue) Compact() (*segment.CompactionResult, error) {
	start := time.Now()

	q.mu.RLock()
	defer q.mu.RUnlock()

	if q.closed {
		q.opts.MetricsCollector.RecordCompactionError()
		return nil, fmt.Errorf("queue is closed")
	}

	result, err := q.segments.Compact()
	if err != nil {
		q.opts.MetricsCollector.RecordCompactionError()
		return nil, err
	}

	// Record metrics
	q.opts.MetricsCollector.RecordCompaction(result.SegmentsRemoved, result.BytesFreed, time.Since(start))

	return result, nil
}
