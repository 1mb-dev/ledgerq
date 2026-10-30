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

	// Held from Open to Close: one process per queue directory
	dirLock *os.File

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

	// Take the lock before anything reads or repairs the directory
	dirLock, err := acquireDirLock(dir)
	if err != nil {
		return nil, err
	}
	opened := false
	defer func() {
		if !opened {
			_ = dirLock.Close()
		}
	}()

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

	// Recover state from segments
	recoveredNextMsgID, hasEntries, err := recoverState(segments)
	if err != nil {
		_ = segments.Close()
		_ = metadata.Close()
		return nil, fmt.Errorf("failed to recover queue state: %w", err)
	}

	if dropped := segments.RepairedBytes(); dropped > 0 {
		opts.Logger.Warn("cut a torn entry from a segment; the bytes are in a .torn file beside it",
			logging.F("bytes", dropped),
		)
	}

	// The segments are the truth. Metadata can trail them when it wasn't synced before a crash,
	// and it can lead them: it is written per message, while entries wait in the write buffer.
	// Leading metadata would leave readers waiting forever for messages that never reached disk.
	//
	// Lowering next reissues the IDs of lost messages. That is safe because Dequeue syncs before
	// it reads: a lost message was never delivered, so a reissued ID is above every delivered ID.
	wantNext, wantRead := nextMsgID, readMsgID
	if hasEntries {
		wantNext = recoveredNextMsgID
	} else {
		wantRead = wantNext // Nothing on disk to read
	}
	if wantRead > wantNext {
		wantRead = wantNext
	}
	if wantNext < nextMsgID {
		opts.Logger.Warn("messages recorded in metadata never reached disk",
			logging.F("reissuing_from", wantNext),
			logging.F("metadata_next", nextMsgID),
		)
	}
	if wantNext != nextMsgID || wantRead != readMsgID {
		nextMsgID, readMsgID = wantNext, wantRead
		if err := metadata.UpdateState(nextMsgID, readMsgID); err != nil {
			_ = segments.Close()
			_ = metadata.Close()
			return nil, fmt.Errorf("failed to update metadata: %w", err)
		}
	}

	q := &Queue{
		opts:      opts,
		dirLock:   dirLock,
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
	opened = true
	opts.Logger.Info("queue opened",
		logging.F("dir", dir),
		logging.F("next_msg_id", nextMsgID),
		logging.F("read_msg_id", readMsgID),
		logging.F("segments", len(segments.GetSegments())),
	)

	return q, nil
}

// recoverState finds the next message ID from the newest segment holding entries.
// hasEntries is false when no segment holds any, which also covers a queue with no segments.
func recoverState(segments *segment.Manager) (nextMsgID uint64, hasEntries bool, err error) {
	allSegments := segments.GetSegments()

	for i := len(allSegments) - 1; i >= 0; i-- {
		reader, err := segment.NewReader(allSegments[i].Path)
		if err != nil {
			return 0, false, fmt.Errorf("failed to open segment: %w", err)
		}

		var maxMsgID uint64
		err = reader.ScanAll(func(entry *format.Entry, offset uint64) error {
			if entry.MsgID > maxMsgID {
				maxMsgID = entry.MsgID
			}
			return nil
		})
		_ = reader.Close()
		if err != nil {
			return 0, false, fmt.Errorf("failed to scan segment: %w", err)
		}

		if maxMsgID > 0 {
			return maxMsgID + 1, true, nil
		}
	}

	return 1, false, nil
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
	defer func() { _ = q.dirLock.Close() }()

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
