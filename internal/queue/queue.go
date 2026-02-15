// Package queue provides a persistent message queue implementation.
//
// Queue provides a durable, disk-backed message queue with:
//   - Persistent storage with automatic segment rotation
//   - Message ordering guarantees (FIFO)
//   - Offset-based message tracking
//   - Efficient batching support
//   - Crash recovery
//
// Basic usage:
//
//	q, err := queue.Open("/path/to/queue", nil)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer q.Close()
//
//	// Enqueue a message
//	offset, err := q.Enqueue([]byte("hello"))
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	// Dequeue a message
//	msg, err := q.Dequeue()
//	if err != nil {
//	    log.Fatal(err)
//	}
package queue

import (
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/1mb-dev/ledgerq/internal/format"
	"github.com/1mb-dev/ledgerq/internal/logging"
	"github.com/1mb-dev/ledgerq/internal/segment"
)

// Queue is a persistent, disk-backed message queue.
type Queue struct {
	opts *Options

	mu sync.RWMutex

	// Segment manager for storage
	segments *segment.Manager

	// Metadata for persistent state
	metadata *Metadata

	// Message ID tracking
	nextMsgID uint64 // Next message ID to assign
	readMsgID uint64 // Next message ID to read

	// Priority index for priority queue mode (v1.1.0+)
	// Only used when EnablePriorities is true
	priorityIndex *format.PriorityIndex

	// DLQ components (v1.2.0+)
	// Only initialized when DLQPath is configured
	dlq          *Queue        // Separate queue instance for DLQ
	retryTracker *RetryTracker // Retry state tracker

	// Deduplication tracker (v1.4.0+)
	// Only initialized when DefaultDeduplicationWindow > 0
	dedupTracker       *DedupTracker
	dedupCleanupTicker *time.Ticker
	dedupCleanupDone   chan bool

	// Periodic sync
	syncTimer       *time.Timer
	syncTimerActive bool

	// Periodic compaction
	compactionTimer       *time.Timer
	compactionTimerActive bool

	closed bool
}

// Open opens or creates a queue at the specified directory.
// Creates the directory if it doesn't exist.
//
//nolint:gocyclo // Initialization requires multiple steps and conditional logic
func Open(dir string, opts *Options) (*Queue, error) {
	if opts == nil {
		opts = DefaultOptions(dir)
	}

	// Validate options for security and correctness
	if err := opts.Validate(); err != nil {
		return nil, fmt.Errorf("invalid options: %w", err)
	}

	// Create directory with restrictive permissions (owner-only)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, fmt.Errorf("failed to create directory: %w", err)
	}

	// Check disk space if configured
	if err := checkDiskSpace(dir, opts.MinFreeDiskSpace); err != nil {
		return nil, fmt.Errorf("disk space check failed: %w", err)
	}

	// Open or create metadata file
	metadata, err := OpenMetadata(dir, opts.AutoSync)
	if err != nil {
		return nil, fmt.Errorf("failed to open metadata: %w", err)
	}

	// Get state from metadata
	nextMsgID, readMsgID := metadata.GetState()

	// Create segment manager
	segments, err := segment.NewManager(opts.SegmentOptions)
	if err != nil {
		_ = metadata.Close()
		return nil, fmt.Errorf("failed to create segment manager: %w", err)
	}

	// Recover state from segments if needed (for nextMsgID)
	recoveredNextMsgID, _, err := recoverState(segments)
	if err != nil {
		_ = segments.Close()
		_ = metadata.Close()
		return nil, fmt.Errorf("failed to recover queue state: %w", err)
	}

	// Use the higher of metadata nextMsgID and recovered nextMsgID
	// This handles the case where segments were written but metadata wasn't synced
	// before a crash. We always want to use the highest ID to prevent collisions.
	if recoveredNextMsgID > nextMsgID {
		nextMsgID = recoveredNextMsgID
		// Update metadata with recovered state to ensure consistency
		if err := metadata.SetNextMsgID(nextMsgID); err != nil {
			_ = segments.Close()
			_ = metadata.Close()
			return nil, fmt.Errorf("failed to update metadata: %w", err)
		}
	}

	q := &Queue{
		opts:      opts,
		segments:  segments,
		metadata:  metadata,
		nextMsgID: nextMsgID,
		readMsgID: readMsgID,
	}

	// Initialize priority index if priority mode is enabled
	if opts.EnablePriorities {
		q.priorityIndex = format.NewPriorityIndex()

		// Rebuild priority index from existing segments
		q.rebuildPriorityIndex()
	}

	// Initialize DLQ if configured (v1.2.0+)
	if opts.DLQPath != "" {
		// Create retry tracker
		retryStatePath := dir + "/.retry_state.json"
		retryTracker, err := NewRetryTracker(retryStatePath, opts.MaxRetries)
		if err != nil {
			_ = metadata.Close()
			_ = segments.Close()
			return nil, fmt.Errorf("failed to create retry tracker: %w", err)
		}
		q.retryTracker = retryTracker

		// Create DLQ queue with modified options to prevent infinite recursion
		// The DLQ itself cannot have a DLQ, and messages in DLQ don't need retry tracking
		dlqOpts := *opts
		dlqOpts.DLQPath = ""                                                 // Disable DLQ for the DLQ itself
		dlqOpts.MaxRetries = 0                                               // No retries for DLQ messages
		dlqOpts.SegmentOptions = segment.DefaultManagerOptions(opts.DLQPath) // Use DLQ path for segment manager

		// Open DLQ queue (Open will create directory if it doesn't exist)
		dlq, err := Open(opts.DLQPath, &dlqOpts)
		if err != nil {
			_ = retryTracker.Close()
			_ = metadata.Close()
			_ = segments.Close()
			return nil, fmt.Errorf("failed to open DLQ: %w", err)
		}
		q.dlq = dlq

		opts.Logger.Info("DLQ initialized",
			logging.F("dlq_path", opts.DLQPath),
			logging.F("max_retries", opts.MaxRetries),
		)
	}

	// Initialize deduplication tracker if configured (v1.4.0+)
	if opts.DefaultDeduplicationWindow > 0 {
		dedupStatePath := dir + "/.dedup_state.json"
		dedupTracker := NewDedupTracker(dedupStatePath, opts.MaxDeduplicationEntries)

		// Load persistent state if it exists
		if err := dedupTracker.Load(); err != nil {
			_ = metadata.Close()
			_ = segments.Close()
			if q.retryTracker != nil {
				_ = q.retryTracker.Close()
			}
			if q.dlq != nil {
				_ = q.dlq.Close()
			}
			return nil, fmt.Errorf("failed to load dedup state: %w", err)
		}
		q.dedupTracker = dedupTracker

		// Start periodic cleanup
		q.startDedupCleanup()

		opts.Logger.Info("deduplication initialized",
			logging.F("window", opts.DefaultDeduplicationWindow.String()),
			logging.F("max_entries", opts.MaxDeduplicationEntries),
			logging.F("loaded_entries", dedupTracker.Count()),
		)
	}

	// Start periodic sync timer if configured
	if !opts.AutoSync && opts.SyncInterval > 0 {
		q.startSyncTimer()
	}

	// Start periodic compaction timer if configured
	if opts.CompactionInterval > 0 {
		q.startCompactionTimer()
	}

	// Update initial metrics state
	stats := q.Stats()
	opts.MetricsCollector.UpdateQueueState(
		stats.PendingMessages,
		uint64(stats.SegmentCount),
		stats.NextMessageID,
		stats.ReadMessageID,
	)

	// Log queue opened
	opts.Logger.Info("queue opened",
		logging.F("dir", dir),
		logging.F("next_msg_id", nextMsgID),
		logging.F("read_msg_id", readMsgID),
		logging.F("segments", len(segments.GetSegments())),
	)

	return q, nil
}

// recoverState scans existing segments to determine the next message ID and read position.
func recoverState(segments *segment.Manager) (nextMsgID, readMsgID uint64, err error) {
	allSegments := segments.GetSegments()

	if len(allSegments) == 0 {
		// No segments yet, start from 1
		return 1, 1, nil
	}

	// Find the highest message ID by scanning the last segment
	lastSeg := allSegments[len(allSegments)-1]
	reader, err := segment.NewReader(lastSeg.Path)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to open last segment: %w", err)
	}
	defer func() { _ = reader.Close() }()

	var maxMsgID uint64 = 0

	// Scan all entries in the last segment to find max message ID
	err = reader.ScanAll(func(entry *format.Entry, offset uint64) error {
		if entry.MsgID > maxMsgID {
			maxMsgID = entry.MsgID
		}
		return nil
	})

	if err != nil {
		return 0, 0, fmt.Errorf("failed to scan segment: %w", err)
	}

	// Next message ID is one after the max
	nextMsgID = maxMsgID + 1

	// For now, start reading from the beginning
	// In a future phase, we'll track read position in metadata
	readMsgID = 1

	return nextMsgID, readMsgID, nil
}

// SeekToMessageID sets the read position to a specific message ID.
// Subsequent Dequeue() calls will start reading from this message.
// Returns an error if the message ID is invalid or out of range.

// Close closes the queue and releases all resources.
func (q *Queue) Close() error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return nil
	}

	// Stop sync timer
	if q.syncTimerActive {
		q.syncTimer.Stop()
		q.syncTimerActive = false
	}

	// Stop compaction timer
	if q.compactionTimerActive {
		q.compactionTimer.Stop()
		q.compactionTimerActive = false
	}

	// Stop dedup cleanup goroutine (v1.4.0+)
	if q.dedupCleanupTicker != nil {
		q.dedupCleanupTicker.Stop()
		close(q.dedupCleanupDone)
	}

	// Close dedup tracker and persist state (v1.4.0+)
	if q.dedupTracker != nil {
		if err := q.dedupTracker.Close(); err != nil {
			q.opts.Logger.Error("failed to close dedup tracker",
				logging.F("error", err.Error()),
			)
			return err
		}
	}

	// Close DLQ components first (v1.2.0+)
	if q.retryTracker != nil {
		if err := q.retryTracker.Close(); err != nil {
			q.opts.Logger.Error("failed to close retry tracker",
				logging.F("error", err.Error()),
			)
			return err
		}
	}

	if q.dlq != nil {
		if err := q.dlq.Close(); err != nil {
			q.opts.Logger.Error("failed to close DLQ",
				logging.F("error", err.Error()),
			)
			return err
		}
	}

	// Close metadata first
	if q.metadata != nil {
		if err := q.metadata.Close(); err != nil {
			q.opts.Logger.Error("failed to close metadata",
				logging.F("error", err.Error()),
			)
			return err
		}
	}

	// Close segments
	if q.segments != nil {
		if err := q.segments.Close(); err != nil {
			q.opts.Logger.Error("failed to close segments",
				logging.F("error", err.Error()),
			)
			return err
		}
	}

	q.opts.Logger.Info("queue closed",
		logging.F("total_messages", q.nextMsgID-1),
		logging.F("pending_messages", q.nextMsgID-q.readMsgID),
	)

	q.closed = true
	return nil
}

// IsClosed returns whether the queue has been closed.
func (q *Queue) IsClosed() bool {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.closed
}
