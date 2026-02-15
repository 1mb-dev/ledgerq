package segment

import (
	"bufio"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/1mb-dev/ledgerq/internal/format"
)

// SyncPolicy defines when to fsync data to disk.
type SyncPolicy int

const (
	// SyncImmediate fsyncs after every write (safest, slowest)
	SyncImmediate SyncPolicy = iota

	// SyncInterval fsyncs at regular intervals (balanced)
	SyncInterval

	// SyncManual requires explicit Sync() calls (fastest, riskiest)
	SyncManual
)

// WriterOptions configures segment writer behavior.
type WriterOptions struct {
	// SyncPolicy determines when data is fsynced to disk
	SyncPolicy SyncPolicy

	// SyncInterval is the duration between automatic fsyncs (for SyncInterval policy)
	SyncInterval time.Duration

	// BufferSize is the size of the write buffer in bytes
	BufferSize int

	// IndexInterval is the number of bytes between sparse index entries
	IndexInterval int
}

// DefaultWriterOptions returns sensible defaults for segment writers.
func DefaultWriterOptions() *WriterOptions {
	return &WriterOptions{
		SyncPolicy:    SyncManual,
		SyncInterval:  1 * time.Second,
		BufferSize:    64 * 1024, // 64KB
		IndexInterval: format.DefaultIndexInterval,
	}
}

// Writer provides buffered, append-only writes to a segment file.
type Writer struct {
	baseOffset uint64
	path       string
	indexPath  string

	file   *os.File
	writer *bufio.Writer

	header *format.SegmentHeader
	index  *format.Index

	// Tracking
	nextOffset      uint64 // Next file offset to write
	lastIndexOffset uint64 // Last offset where we added an index entry
	bytesWritten    uint64 // Total bytes written to segment
	messagesWritten uint64 // Total messages written
	minMsgID        uint64 // Smallest message ID written
	maxMsgID        uint64 // Largest message ID written
	firstWrite      bool   // Track if we've written anything

	opts *WriterOptions

	// Sync management
	mu              sync.Mutex
	syncTimer       *time.Timer
	lastSyncTime    time.Time
	needsSync       bool
	syncTimerActive bool

	closed bool
}

// NewWriter creates a new segment writer.
// Creates the segment file and writes the header.
func NewWriter(dir string, baseOffset uint64, opts *WriterOptions) (*Writer, error) {
	if opts == nil {
		opts = DefaultWriterOptions()
	}

	// Create segment file
	path := dir + "/" + FormatSegmentName(baseOffset)
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644) //nolint:gosec // G304: Path is user-provided
	if err != nil {
		return nil, fmt.Errorf("failed to create segment file: %w", err)
	}

	// Create buffered writer
	writer := bufio.NewWriterSize(file, opts.BufferSize)

	// Create and write segment header
	header := format.NewSegmentHeader(baseOffset)
	header.CreatedAt = time.Now().UnixNano()

	headerData := header.Marshal()
	if _, err := writer.Write(headerData); err != nil {
		_ = file.Close()
		_ = os.Remove(path)
		return nil, fmt.Errorf("failed to write segment header: %w", err)
	}

	// Create sparse index
	index := format.NewIndex(baseOffset)

	w := &Writer{
		baseOffset:      baseOffset,
		path:            path,
		indexPath:       dir + "/" + FormatIndexName(baseOffset),
		file:            file,
		writer:          writer,
		header:          header,
		index:           index,
		nextOffset:      uint64(format.SegmentHeaderSize),
		lastIndexOffset: 0,
		bytesWritten:    0,
		messagesWritten: 0,
		firstWrite:      true,
		opts:            opts,
		lastSyncTime:    time.Now(),
	}

	// Start sync timer if using interval-based sync
	if opts.SyncPolicy == SyncInterval {
		w.startSyncTimer()
	}

	return w, nil
}

// Write appends an entry to the segment.
// Returns the file offset where the entry was written.
func (w *Writer) Write(entry *format.Entry) (uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return 0, fmt.Errorf("writer is closed")
	}

	// Set TTL flag if ExpiresAt is specified (Marshal also does this, but we need it for validation)
	if entry.ExpiresAt > 0 {
		entry.Flags |= format.EntryFlagTTL
	}

	// Set Headers flag if headers are present (Marshal also does this, but we need it for validation)
	if len(entry.Headers) > 0 {
		entry.Flags |= format.EntryFlagHeaders
	}

	// Set entry length before validation (Marshal would do this, but we validate first)
	// Account for optional fields based on flags
	headerSize := format.EntryHeaderSize
	if entry.Flags&format.EntryFlagPriority != 0 {
		headerSize += 1 // Add 1 byte for Priority
	}
	if entry.Flags&format.EntryFlagTTL != 0 {
		headerSize += 8 // Add 8 bytes for ExpiresAt
	}
	if entry.Flags&format.EntryFlagHeaders != 0 {
		// Calculate headers size (will be computed by format package too, but we need it here)
		headersSize := 2 // NumHeaders field
		for k, v := range entry.Headers {
			headersSize += 2 + len(k) + 2 + len(v)
		}
		headerSize += headersSize
	}
	if entry.Flags&format.EntryFlagCompressed != 0 {
		headerSize += 1 // Add 1 byte for Compression
	}
	entry.Length = uint32(headerSize + len(entry.Payload) + 4) //nolint:gosec // G115: Safe conversion

	// Validate entry
	if err := entry.Validate(); err != nil {
		return 0, fmt.Errorf("invalid entry: %w", err)
	}

	// Marshal entry
	data := entry.Marshal()

	// Track offset before write
	offset := w.nextOffset

	// Write to buffer
	n, err := w.writer.Write(data)
	if err != nil {
		return 0, fmt.Errorf("failed to write entry: %w", err)
	}

	// Update tracking
	w.nextOffset += uint64(n)
	w.bytesWritten += uint64(n)
	w.messagesWritten++

	// Update min/max message IDs
	if w.firstWrite {
		w.minMsgID = entry.MsgID
		w.maxMsgID = entry.MsgID
		w.firstWrite = false
	} else {
		if entry.MsgID < w.minMsgID {
			w.minMsgID = entry.MsgID
		}
		if entry.MsgID > w.maxMsgID {
			w.maxMsgID = entry.MsgID
		}
	}

	// Add sparse index entry if needed
	if w.shouldAddIndexEntry(offset) {
		w.index.Add(&format.IndexEntry{
			MessageID:  entry.MsgID,
			FileOffset: offset,
			Timestamp:  entry.Timestamp,
		})
		w.lastIndexOffset = offset
	}

	w.needsSync = true

	// Sync if policy requires
	if w.opts.SyncPolicy == SyncImmediate {
		if err := w.syncLocked(); err != nil {
			return offset, err
		}
	}

	return offset, nil
}

// shouldAddIndexEntry determines if we should add a sparse index entry.
func (w *Writer) shouldAddIndexEntry(offset uint64) bool {
	// Always index the first entry
	if w.messagesWritten == 1 {
		return true
	}

	// Add entry if we've written more than IndexInterval bytes since last index
	return (offset - w.lastIndexOffset) >= uint64(w.opts.IndexInterval)
}

// Flush flushes the write buffer to the OS.
func (w *Writer) Flush() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return fmt.Errorf("writer is closed")
	}

	return w.writer.Flush()
}

// Sync flushes the buffer and fsyncs data to disk.
func (w *Writer) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.syncLocked()
}

// syncLocked performs sync with lock already held.
func (w *Writer) syncLocked() error {
	if w.closed {
		return fmt.Errorf("writer is closed")
	}

	// Flush buffer first
	if err := w.writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush buffer: %w", err)
	}

	// Fsync to disk
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync file: %w", err)
	}

	w.lastSyncTime = time.Now()
	w.needsSync = false

	return nil
}

// Close closes the segment writer, flushing all data and writing the index.
func (w *Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return nil
	}

	// Stop sync timer
	if w.syncTimerActive {
		w.syncTimer.Stop()
		w.syncTimerActive = false
	}

	// Final sync
	if w.needsSync {
		if err := w.syncLocked(); err != nil {
			return err
		}
	}

	// Update and write final segment header
	w.header.MinMsgID = w.minMsgID
	w.header.MaxMsgID = w.maxMsgID

	// Seek to beginning and rewrite header
	if _, err := w.file.Seek(0, 0); err != nil {
		return fmt.Errorf("failed to seek to header: %w", err)
	}

	headerData := w.header.Marshal()
	if _, err := w.file.Write(headerData); err != nil {
		return fmt.Errorf("failed to update header: %w", err)
	}

	// Sync header update
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync header: %w", err)
	}

	// Close segment file
	if err := w.file.Close(); err != nil {
		return fmt.Errorf("failed to close segment file: %w", err)
	}

	// Write index file
	if err := format.WriteIndexFile(w.indexPath, w.index); err != nil {
		return fmt.Errorf("failed to write index file: %w", err)
	}

	w.closed = true
	return nil
}

// startSyncTimer starts the automatic sync timer.
func (w *Writer) startSyncTimer() {
	w.syncTimer = time.AfterFunc(w.opts.SyncInterval, func() {
		w.mu.Lock()
		defer w.mu.Unlock()

		if !w.closed && w.needsSync {
			_ = w.syncLocked() // Ignore error in background sync
		}

		// Reschedule if not closed
		if !w.closed {
			w.syncTimer.Reset(w.opts.SyncInterval)
		}
	})
	w.syncTimerActive = true
}

// BaseOffset returns the base offset of this segment.
func (w *Writer) BaseOffset() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.baseOffset
}

// BytesWritten returns the total number of bytes written (excluding header).
func (w *Writer) BytesWritten() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.bytesWritten
}

// MessagesWritten returns the total number of messages written.
func (w *Writer) MessagesWritten() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.messagesWritten
}

// IsClosed returns whether the writer has been closed.
func (w *Writer) IsClosed() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.closed
}
