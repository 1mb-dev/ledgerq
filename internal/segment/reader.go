package segment

import (
	"fmt"
	"io"
	"os"

	"github.com/1mb-dev/ledgerq/internal/format"
)

// Reader provides sequential and indexed reads from a segment file.
type Reader struct {
	baseOffset uint64
	path       string
	indexPath  string

	file   *os.File
	header *format.SegmentHeader
	index  *format.Index

	// Cached file size for bounds checking
	fileSize int64
}

// NewReader opens a segment file for reading.
// Loads the segment header and index (if available).
func NewReader(path string) (*Reader, error) {
	// Open segment file
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open segment file: %w", err)
	}

	// Get file size
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("failed to stat segment file: %w", err)
	}

	// Read segment header
	header, err := format.ReadSegmentHeader(path)
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("failed to read segment header: %w", err)
	}

	// Validate header
	if err := header.Validate(); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("invalid segment header: %w", err)
	}

	// Derive index path from segment path
	baseOffset := header.BaseOffset
	indexPath := ""
	if len(path) >= 4 && path[len(path)-4:] == SegmentFileExtension {
		indexPath = path[:len(path)-4] + IndexFileExtension
	}

	// Try to load index (not required)
	var index *format.Index
	if indexPath != "" {
		idx, err := format.ReadIndexFile(indexPath)
		if err == nil {
			index = idx
		}
		// Ignore index errors - we can still read sequentially
	}

	return &Reader{
		baseOffset: baseOffset,
		path:       path,
		indexPath:  indexPath,
		file:       file,
		header:     header,
		index:      index,
		fileSize:   info.Size(),
	}, nil
}

// ReadAt reads an entry at a specific file offset.
// Returns the entry and the offset of the next entry.
func (r *Reader) ReadAt(offset uint64) (*format.Entry, uint64, error) {
	// Validate offset is within bounds
	if offset >= uint64(r.fileSize) {
		return nil, 0, io.EOF
	}

	// Seek to offset
	if _, err := r.file.Seek(int64(offset), 0); err != nil {
		return nil, 0, fmt.Errorf("failed to seek to offset %d: %w", offset, err)
	}

	// Unmarshal entry directly from file
	entry, err := format.Unmarshal(r.file)
	if err != nil {
		if err == io.EOF {
			return nil, 0, io.EOF
		}
		return nil, 0, fmt.Errorf("failed to unmarshal entry: %w", err)
	}

	// Validate entry
	if err := entry.Validate(); err != nil {
		return nil, 0, fmt.Errorf("invalid entry: %w", err)
	}

	// Calculate next offset (length field is 4 bytes + entry.Length)
	nextOffset := offset + 4 + uint64(entry.Length)

	return entry, nextOffset, nil
}

// Read reads the next entry from the current file position.
// Returns the entry, its file offset, and the offset of the next entry.
func (r *Reader) Read() (*format.Entry, uint64, uint64, error) {
	// Get current position
	currentPos, err := r.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("failed to get current position: %w", err)
	}

	offset := uint64(currentPos)

	// Read entry at current position
	entry, nextOffset, err := r.ReadAt(offset)
	if err != nil {
		return nil, 0, 0, err
	}

	// Update file position
	if _, err := r.file.Seek(int64(nextOffset), 0); err != nil {
		return nil, 0, 0, fmt.Errorf("failed to seek to next entry: %w", err)
	}

	return entry, offset, nextOffset, nil
}

// Seek positions the reader at a specific file offset.
func (r *Reader) Seek(offset uint64) error {
	if offset > uint64(r.fileSize) {
		return fmt.Errorf("offset %d beyond file size %d", offset, r.fileSize)
	}

	if _, err := r.file.Seek(int64(offset), 0); err != nil {
		return fmt.Errorf("failed to seek to offset %d: %w", offset, err)
	}

	return nil
}

// SeekToStart positions the reader at the first entry (after the header).
func (r *Reader) SeekToStart() error {
	return r.Seek(format.SegmentHeaderSize)
}

// FindByMessageID uses the index to find an entry by message ID.
// Returns the entry, its file offset, and the next offset.
// Falls back to sequential scan if no index is available.
func (r *Reader) FindByMessageID(msgID uint64) (*format.Entry, uint64, uint64, error) {
	if r.index == nil {
		// No index - sequential scan from start
		if err := r.SeekToStart(); err != nil {
			return nil, 0, 0, err
		}

		for {
			entry, offset, nextOffset, err := r.Read()
			if err != nil {
				if err == io.EOF {
					return nil, 0, 0, fmt.Errorf("message ID %d not found", msgID)
				}
				return nil, 0, 0, err
			}

			if entry.MsgID == msgID {
				return entry, offset, nextOffset, nil
			}

			if entry.MsgID > msgID {
				// Messages are sequential, we've gone past it
				return nil, 0, 0, fmt.Errorf("message ID %d not found", msgID)
			}
		}
	}

	// Use index to find starting point
	indexEntry := r.index.Find(msgID)
	if indexEntry == nil {
		// Message ID is before all indexed entries
		if err := r.SeekToStart(); err != nil {
			return nil, 0, 0, err
		}
	} else {
		// Seek to the indexed position
		if err := r.Seek(indexEntry.FileOffset); err != nil {
			return nil, 0, 0, err
		}
	}

	// Scan forward from index position to find exact message
	for {
		entry, offset, nextOffset, err := r.Read()
		if err != nil {
			if err == io.EOF {
				return nil, 0, 0, fmt.Errorf("message ID %d not found", msgID)
			}
			return nil, 0, 0, err
		}

		if entry.MsgID == msgID {
			return entry, offset, nextOffset, nil
		}

		if entry.MsgID > msgID {
			// We've gone past it
			return nil, 0, 0, fmt.Errorf("message ID %d not found", msgID)
		}
	}
}

// FindByTimestamp uses the index to find entries near a timestamp.
// Returns the first entry at or after the given timestamp.
func (r *Reader) FindByTimestamp(timestamp int64) (*format.Entry, uint64, uint64, error) {
	if r.index == nil {
		// No index - sequential scan from start
		if err := r.SeekToStart(); err != nil {
			return nil, 0, 0, err
		}

		for {
			entry, offset, nextOffset, err := r.Read()
			if err != nil {
				if err == io.EOF {
					return nil, 0, 0, fmt.Errorf("no entries at or after timestamp %d", timestamp)
				}
				return nil, 0, 0, err
			}

			if entry.Timestamp >= timestamp {
				return entry, offset, nextOffset, nil
			}
		}
	}

	// Use index to find starting point by timestamp
	// Find the largest index entry with timestamp <= target timestamp
	var indexEntry *format.IndexEntry
	for _, entry := range r.index.Entries {
		if entry.Timestamp <= timestamp {
			indexEntry = entry
		} else {
			break // Entries are sorted, so we can stop
		}
	}

	if indexEntry == nil {
		// Timestamp is before all indexed entries
		if err := r.SeekToStart(); err != nil {
			return nil, 0, 0, err
		}
	} else {
		// Seek to the indexed position
		if err := r.Seek(indexEntry.FileOffset); err != nil {
			return nil, 0, 0, err
		}
	}

	// Scan forward to find first entry at or after timestamp
	for {
		entry, offset, nextOffset, err := r.Read()
		if err != nil {
			if err == io.EOF {
				return nil, 0, 0, fmt.Errorf("no entries at or after timestamp %d", timestamp)
			}
			return nil, 0, 0, err
		}

		if entry.Timestamp >= timestamp {
			return entry, offset, nextOffset, nil
		}
	}
}

// BaseOffset returns the base offset of this segment.
func (r *Reader) BaseOffset() uint64 {
	return r.baseOffset
}

// Header returns the segment header.
func (r *Reader) Header() *format.SegmentHeader {
	return r.header
}

// HasIndex returns whether this reader has a loaded index.
func (r *Reader) HasIndex() bool {
	return r.index != nil
}

// Size returns the file size in bytes.
func (r *Reader) Size() int64 {
	return r.fileSize
}

// Close closes the segment reader.
func (r *Reader) Close() error {
	if r.file != nil {
		err := r.file.Close()
		r.file = nil // Prevent double close
		return err
	}
	return nil
}

// ScanAll reads all entries from the segment sequentially.
// Calls the visitor function for each entry with (entry, fileOffset).
// Stops if visitor returns an error.
func (r *Reader) ScanAll(visitor func(*format.Entry, uint64) error) error {
	if err := r.SeekToStart(); err != nil {
		return err
	}

	for {
		entry, offset, _, err := r.Read()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}

		if err := visitor(entry, offset); err != nil {
			return err
		}
	}
}
