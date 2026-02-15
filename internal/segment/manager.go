package segment

import (
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/1mb-dev/ledgerq/internal/format"
)

// RotationPolicy defines when to rotate to a new segment.
type RotationPolicy int

const (
	// RotateBySize rotates when segment reaches size limit
	RotateBySize RotationPolicy = iota

	// RotateByCount rotates when segment reaches message count limit
	RotateByCount

	// RotateByTime rotates after a time duration
	RotateByTime

	// RotateByBoth rotates when size OR count limit is reached
	RotateByBoth
)

// RetentionPolicy defines how to retain/remove old segments.
type RetentionPolicy struct {
	// MaxAge is the max age of segments to keep (0 = no age limit)
	MaxAge time.Duration

	// MaxSize is the max total size of all segments (0 = no size limit)
	MaxSize uint64

	// MaxSegments is the max number of segments to keep (0 = no count limit)
	MaxSegments int

	// MinSegments is the minimum number of segments to always keep (even if they exceed limits)
	MinSegments int
}

// DefaultRetentionPolicy returns sensible defaults.
func DefaultRetentionPolicy() *RetentionPolicy {
	return &RetentionPolicy{
		MaxAge:      7 * 24 * time.Hour, // 7 days
		MaxSize:     0,                  // No size limit
		MaxSegments: 0,                  // No count limit
		MinSegments: 1,                  // Always keep at least 1 segment
	}
}

// ManagerOptions configures segment manager behavior.
type ManagerOptions struct {
	// Directory where segments are stored
	Directory string

	// RotationPolicy determines when to rotate segments
	RotationPolicy RotationPolicy

	// MaxSegmentSize is the max size in bytes before rotation (for RotateBySize, RotateByBoth)
	MaxSegmentSize uint64

	// MaxSegmentMessages is the max message count before rotation (for RotateByCount, RotateByBoth)
	MaxSegmentMessages uint64

	// MaxSegmentAge is the max duration before rotation (for RotateByTime)
	MaxSegmentAge time.Duration

	// WriterOptions for creating new segments
	WriterOptions *WriterOptions

	// RetentionPolicy for cleaning up old segments
	RetentionPolicy *RetentionPolicy
}

// DefaultManagerOptions returns sensible defaults for segment management.
func DefaultManagerOptions(dir string) *ManagerOptions {
	return &ManagerOptions{
		Directory:          dir,
		RotationPolicy:     RotateByBoth,
		MaxSegmentSize:     100 * 1024 * 1024, // 100MB
		MaxSegmentMessages: 1000000,           // 1M messages
		MaxSegmentAge:      24 * time.Hour,    // 24 hours
		WriterOptions:      DefaultWriterOptions(),
		RetentionPolicy:    DefaultRetentionPolicy(),
	}
}

// Manager manages a collection of segments with automatic rotation.
type Manager struct {
	opts *ManagerOptions

	mu             sync.RWMutex
	activeWriter   *Writer
	segments       []*SegmentInfo
	nextBaseOffset uint64
	segmentCreated time.Time
	closed         bool
}

// NewManager creates a new segment manager.
// Discovers existing segments and prepares for writing.
func NewManager(opts *ManagerOptions) (*Manager, error) {
	if opts == nil {
		return nil, fmt.Errorf("options cannot be nil")
	}

	// Discover existing segments
	segments, err := DiscoverSegments(opts.Directory)
	if err != nil {
		return nil, fmt.Errorf("failed to discover segments: %w", err)
	}

	// Validate segment sequence
	if err := ValidateSegmentSequence(segments); err != nil {
		return nil, fmt.Errorf("invalid segment sequence: %w", err)
	}

	// Determine next base offset
	var nextOffset uint64
	if len(segments) > 0 {
		// Start from a new base offset after the last segment
		// Use the last segment's base offset + MaxSegmentMessages as the next offset
		lastSeg := segments[len(segments)-1]
		nextOffset = lastSeg.BaseOffset + opts.MaxSegmentMessages
	} else {
		// No segments yet, start from 1
		nextOffset = 1
	}

	m := &Manager{
		opts:           opts,
		segments:       segments,
		nextBaseOffset: nextOffset,
		segmentCreated: time.Now(),
	}

	// Create initial active writer
	if err := m.rotateSegment(); err != nil {
		return nil, fmt.Errorf("failed to create initial segment: %w", err)
	}

	return m, nil
}

// Write writes an entry to the active segment.
// Automatically rotates to a new segment if rotation policy is met.
func (m *Manager) Write(entry *format.Entry) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return 0, fmt.Errorf("manager is closed")
	}

	// Check if we need to rotate before writing
	if m.shouldRotate() {
		if err := m.rotateSegment(); err != nil {
			return 0, fmt.Errorf("failed to rotate segment: %w", err)
		}
	}

	// Write to active segment
	offset, err := m.activeWriter.Write(entry)
	if err != nil {
		return 0, err
	}

	return offset, nil
}

// shouldRotate determines if we should rotate to a new segment.
// Must be called with lock held.
func (m *Manager) shouldRotate() bool {
	if m.activeWriter == nil {
		return true
	}

	switch m.opts.RotationPolicy {
	case RotateBySize:
		return m.activeWriter.BytesWritten() >= m.opts.MaxSegmentSize

	case RotateByCount:
		return m.activeWriter.MessagesWritten() >= m.opts.MaxSegmentMessages

	case RotateByTime:
		return time.Since(m.segmentCreated) >= m.opts.MaxSegmentAge

	case RotateByBoth:
		return m.activeWriter.BytesWritten() >= m.opts.MaxSegmentSize ||
			m.activeWriter.MessagesWritten() >= m.opts.MaxSegmentMessages

	default:
		return false
	}
}

// rotateSegment closes the current segment and creates a new one.
// Must be called with lock held.
func (m *Manager) rotateSegment() error {
	// Close existing writer if any
	if m.activeWriter != nil {
		if err := m.activeWriter.Close(); err != nil {
			return fmt.Errorf("failed to close active segment: %w", err)
		}

		// Add closed segment to segments list
		segInfo := &SegmentInfo{
			BaseOffset: m.activeWriter.BaseOffset(),
			Path:       m.activeWriter.path,
			IndexPath:  m.activeWriter.indexPath,
			Size:       int64(m.activeWriter.BytesWritten()),
		}
		m.segments = append(m.segments, segInfo)
	}

	// Create new writer with next base offset
	writer, err := NewWriter(m.opts.Directory, m.nextBaseOffset, m.opts.WriterOptions)
	if err != nil {
		return fmt.Errorf("failed to create new segment writer: %w", err)
	}

	m.activeWriter = writer
	m.segmentCreated = time.Now()

	// Increment next base offset for future segments
	// This is a simplistic approach - in production you'd track actual message IDs
	m.nextBaseOffset += m.opts.MaxSegmentMessages

	return nil
}

// Sync syncs the active segment to disk.
func (m *Manager) Sync() error {
	m.mu.RLock()
	writer := m.activeWriter
	m.mu.RUnlock()

	if writer == nil {
		return fmt.Errorf("no active segment")
	}

	return writer.Sync()
}

// GetSegments returns information about all segments.
func (m *Manager) GetSegments() []*SegmentInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Return a copy to prevent external modification
	result := make([]*SegmentInfo, len(m.segments))
	copy(result, m.segments)
	return result
}

// GetActiveSegment returns the currently active segment for writing.
func (m *Manager) GetActiveSegment() *SegmentInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.activeWriter == nil {
		return nil
	}

	return &SegmentInfo{
		BaseOffset: m.activeWriter.BaseOffset(),
		Path:       m.activeWriter.path,
		IndexPath:  m.activeWriter.indexPath,
		Size:       int64(m.activeWriter.BytesWritten()),
	}
}

// OpenReader opens a reader for a specific segment by base offset.
func (m *Manager) OpenReader(baseOffset uint64) (*Reader, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Search for segment with matching base offset
	for _, seg := range m.segments {
		if seg.BaseOffset == baseOffset {
			return NewReader(seg.Path)
		}
	}

	// Check if it's the active segment
	if m.activeWriter != nil && m.activeWriter.BaseOffset() == baseOffset {
		return NewReader(m.activeWriter.path)
	}

	return nil, fmt.Errorf("segment with base offset %d not found", baseOffset)
}

// Close closes the manager and all open segments.
func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return nil
	}

	// Close active writer
	if m.activeWriter != nil {
		if err := m.activeWriter.Close(); err != nil {
			return err
		}
		m.activeWriter = nil
	}

	m.closed = true
	return nil
}

// IsClosed returns whether the manager has been closed.
func (m *Manager) IsClosed() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.closed
}

// ActiveWriterStats returns statistics about the active writer.
type ActiveWriterStats struct {
	BaseOffset      uint64
	BytesWritten    uint64
	MessagesWritten uint64
	Age             time.Duration
}

// GetActiveWriterStats returns statistics about the active writer.
func (m *Manager) GetActiveWriterStats() *ActiveWriterStats {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.activeWriter == nil {
		return nil
	}

	return &ActiveWriterStats{
		BaseOffset:      m.activeWriter.BaseOffset(),
		BytesWritten:    m.activeWriter.BytesWritten(),
		MessagesWritten: m.activeWriter.MessagesWritten(),
		Age:             time.Since(m.segmentCreated),
	}
}

// CompactionResult contains information about a compaction operation.
type CompactionResult struct {
	// SegmentsRemoved is the number of segments removed
	SegmentsRemoved int

	// BytesFreed is the total bytes freed
	BytesFreed int64

	// OldestSegmentAge is the age of the oldest remaining segment
	OldestSegmentAge time.Duration
}

// Compact removes old segments according to the retention policy.
// Returns information about what was removed.
func (m *Manager) Compact() (*CompactionResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return nil, fmt.Errorf("manager is closed")
	}

	if m.opts.RetentionPolicy == nil {
		return &CompactionResult{}, nil
	}

	policy := m.opts.RetentionPolicy

	// Determine which segments to remove
	toRemove := m.selectSegmentsForRemoval(policy)

	if len(toRemove) == 0 {
		return &CompactionResult{}, nil
	}

	// Remove the segments
	var bytesFreed int64
	for _, seg := range toRemove {
		bytesFreed += seg.Size

		// Remove segment file
		if err := os.Remove(seg.Path); err != nil {
			return nil, fmt.Errorf("failed to remove segment %s: %w", seg.Path, err)
		}

		// Remove index file if it exists
		if seg.IndexPath != "" {
			_ = os.Remove(seg.IndexPath) // Ignore error if index doesn't exist
		}
	}

	// Update segments list
	newSegments := make([]*SegmentInfo, 0, len(m.segments)-len(toRemove))
	removeSet := make(map[uint64]bool)
	for _, seg := range toRemove {
		removeSet[seg.BaseOffset] = true
	}

	for _, seg := range m.segments {
		if !removeSet[seg.BaseOffset] {
			newSegments = append(newSegments, seg)
		}
	}
	m.segments = newSegments

	// Calculate oldest segment age
	var oldestAge time.Duration
	if len(m.segments) > 0 {
		oldestSeg := m.segments[0]
		if info, err := os.Stat(oldestSeg.Path); err == nil {
			oldestAge = time.Since(info.ModTime())
		}
	}

	return &CompactionResult{
		SegmentsRemoved:  len(toRemove),
		BytesFreed:       bytesFreed,
		OldestSegmentAge: oldestAge,
	}, nil
}

// selectSegmentsForRemoval determines which segments should be removed.
// Must be called with lock held.
func (m *Manager) selectSegmentsForRemoval(policy *RetentionPolicy) []*SegmentInfo {
	if len(m.segments) == 0 {
		return nil
	}

	// Always keep at least MinSegments
	if len(m.segments) <= policy.MinSegments {
		return nil
	}

	var toRemove []*SegmentInfo
	now := time.Now()

	// Check each segment against retention policies
	for i, seg := range m.segments {
		// Ensure we keep at least MinSegments
		remaining := len(m.segments) - len(toRemove)
		if remaining <= policy.MinSegments {
			break
		}

		// Don't remove the last segment (even if it exceeds limits)
		if i == len(m.segments)-1 {
			break
		}

		shouldRemove := false

		// Check age-based retention
		if policy.MaxAge > 0 {
			if info, err := os.Stat(seg.Path); err == nil {
				age := now.Sub(info.ModTime())
				if age > policy.MaxAge {
					shouldRemove = true
				}
			}
		}

		// Check count-based retention
		if policy.MaxSegments > 0 {
			if len(m.segments)-len(toRemove) > policy.MaxSegments {
				shouldRemove = true
			}
		}

		// Check size-based retention
		if policy.MaxSize > 0 {
			totalSize := m.calculateTotalSize(m.segments[i:])
			if totalSize > policy.MaxSize {
				shouldRemove = true
			}
		}

		if shouldRemove {
			toRemove = append(toRemove, seg)
		}
	}

	return toRemove
}

// calculateTotalSize calculates the total size of a set of segments.
func (m *Manager) calculateTotalSize(segments []*SegmentInfo) uint64 {
	var total uint64
	for _, seg := range segments {
		if seg.Size > 0 {
			total += uint64(seg.Size)
		}
	}
	return total
}
