package segment

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/1mb-dev/ledgerq/internal/format"
)

// Helper: Create a test segment with entries
func createTestSegment(t *testing.T, dir string, baseOffset uint64, entries []*format.Entry) string {
	t.Helper()

	w, err := NewWriter(dir, baseOffset, nil)
	if err != nil {
		t.Fatalf("NewWriter() error = %v", err)
	}

	for _, entry := range entries {
		if _, err := w.Write(entry); err != nil {
			t.Fatalf("Write() error = %v", err)
		}
	}

	if err := w.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	return filepath.Join(dir, FormatSegmentName(baseOffset))
}

func TestNewReader(t *testing.T) {
	tmpDir := t.TempDir()

	// Create test segment
	entries := []*format.Entry{
		{
			Type:      format.EntryTypeData,
			Flags:     format.EntryFlagNone,
			MsgID:     1000,
			Timestamp: time.Now().UnixNano(),
			Payload:   []byte("test message 1"),
		},
		{
			Type:      format.EntryTypeData,
			Flags:     format.EntryFlagNone,
			MsgID:     1001,
			Timestamp: time.Now().UnixNano(),
			Payload:   []byte("test message 2"),
		},
	}

	segPath := createTestSegment(t, tmpDir, 1000, entries)

	// Open reader
	r, err := NewReader(segPath)
	if err != nil {
		t.Fatalf("NewReader() error = %v", err)
	}
	defer r.Close()

	if r.BaseOffset() != 1000 {
		t.Errorf("BaseOffset() = %d, want 1000", r.BaseOffset())
	}

	if !r.HasIndex() {
		t.Error("HasIndex() = false, want true (index should be created on writer close)")
	}

	if r.Size() <= format.SegmentHeaderSize {
		t.Errorf("Size() = %d, should be > %d", r.Size(), format.SegmentHeaderSize)
	}
}

func TestNewReader_NonExistent(t *testing.T) {
	_, err := NewReader("/nonexistent/segment.log")
	if err == nil {
		t.Error("NewReader() should fail for non-existent file")
	}
}

func TestReader_ReadAt(t *testing.T) {
	tmpDir := t.TempDir()

	// Create test segment
	entries := []*format.Entry{
		{
			Type:      format.EntryTypeData,
			Flags:     format.EntryFlagNone,
			MsgID:     1000,
			Timestamp: time.Now().UnixNano(),
			Payload:   []byte("first message"),
		},
		{
			Type:      format.EntryTypeData,
			Flags:     format.EntryFlagNone,
			MsgID:     1001,
			Timestamp: time.Now().UnixNano(),
			Payload:   []byte("second message"),
		},
	}

	segPath := createTestSegment(t, tmpDir, 1000, entries)

	r, err := NewReader(segPath)
	if err != nil {
		t.Fatalf("NewReader() error = %v", err)
	}
	defer r.Close()

	// Read first entry at header offset
	entry, nextOffset, err := r.ReadAt(format.SegmentHeaderSize)
	if err != nil {
		t.Fatalf("ReadAt() error = %v", err)
	}

	if entry.MsgID != 1000 {
		t.Errorf("ReadAt() MsgID = %d, want 1000", entry.MsgID)
	}

	if string(entry.Payload) != "first message" {
		t.Errorf("ReadAt() Payload = %s, want 'first message'", entry.Payload)
	}

	// Read second entry at next offset
	entry2, _, err := r.ReadAt(nextOffset)
	if err != nil {
		t.Fatalf("ReadAt(nextOffset) error = %v", err)
	}

	if entry2.MsgID != 1001 {
		t.Errorf("ReadAt(nextOffset) MsgID = %d, want 1001", entry2.MsgID)
	}

	if string(entry2.Payload) != "second message" {
		t.Errorf("ReadAt(nextOffset) Payload = %s, want 'second message'", entry2.Payload)
	}
}

func TestReader_Read(t *testing.T) {
	tmpDir := t.TempDir()

	// Create test segment
	entries := []*format.Entry{
		{
			Type:      format.EntryTypeData,
			Flags:     format.EntryFlagNone,
			MsgID:     1000,
			Timestamp: time.Now().UnixNano(),
			Payload:   []byte("msg1"),
		},
		{
			Type:      format.EntryTypeData,
			Flags:     format.EntryFlagNone,
			MsgID:     1001,
			Timestamp: time.Now().UnixNano(),
			Payload:   []byte("msg2"),
		},
		{
			Type:      format.EntryTypeData,
			Flags:     format.EntryFlagNone,
			MsgID:     1002,
			Timestamp: time.Now().UnixNano(),
			Payload:   []byte("msg3"),
		},
	}

	segPath := createTestSegment(t, tmpDir, 1000, entries)

	r, err := NewReader(segPath)
	if err != nil {
		t.Fatalf("NewReader() error = %v", err)
	}
	defer r.Close()

	// Seek to start
	if err := r.SeekToStart(); err != nil {
		t.Fatalf("SeekToStart() error = %v", err)
	}

	// Read entries sequentially
	for i := 0; i < 3; i++ {
		entry, offset, nextOffset, err := r.Read()
		if err != nil {
			t.Fatalf("Read() %d error = %v", i, err)
		}

		expectedMsgID := uint64(1000 + i)
		if entry.MsgID != expectedMsgID {
			t.Errorf("Read() %d MsgID = %d, want %d", i, entry.MsgID, expectedMsgID)
		}

		if i == 0 && offset != format.SegmentHeaderSize {
			t.Errorf("Read() 0 offset = %d, want %d", offset, format.SegmentHeaderSize)
		}

		if i < 2 && nextOffset <= offset {
			t.Errorf("Read() %d nextOffset = %d, should be > %d", i, nextOffset, offset)
		}
	}

	// Next read should return EOF
	_, _, _, err = r.Read()
	if err != io.EOF {
		t.Errorf("Read() after end should return EOF, got %v", err)
	}
}

func TestReader_Seek(t *testing.T) {
	tmpDir := t.TempDir()

	entries := []*format.Entry{
		{
			Type:      format.EntryTypeData,
			Flags:     format.EntryFlagNone,
			MsgID:     1000,
			Timestamp: time.Now().UnixNano(),
			Payload:   []byte("test"),
		},
	}

	segPath := createTestSegment(t, tmpDir, 1000, entries)

	r, err := NewReader(segPath)
	if err != nil {
		t.Fatalf("NewReader() error = %v", err)
	}
	defer r.Close()

	// Seek to start
	if err := r.Seek(format.SegmentHeaderSize); err != nil {
		t.Errorf("Seek() error = %v", err)
	}

	// Seek beyond file should fail
	if err := r.Seek(uint64(r.Size()) + 100); err == nil {
		t.Error("Seek() beyond file size should fail")
	}
}

func TestReader_FindByMessageID(t *testing.T) {
	tmpDir := t.TempDir()

	// Create segment with multiple entries
	entries := []*format.Entry{
		{
			Type:      format.EntryTypeData,
			Flags:     format.EntryFlagNone,
			MsgID:     1000,
			Timestamp: time.Now().UnixNano(),
			Payload:   []byte("msg 1000"),
		},
		{
			Type:      format.EntryTypeData,
			Flags:     format.EntryFlagNone,
			MsgID:     1001,
			Timestamp: time.Now().UnixNano(),
			Payload:   []byte("msg 1001"),
		},
		{
			Type:      format.EntryTypeData,
			Flags:     format.EntryFlagNone,
			MsgID:     1002,
			Timestamp: time.Now().UnixNano(),
			Payload:   []byte("msg 1002"),
		},
		{
			Type:      format.EntryTypeData,
			Flags:     format.EntryFlagNone,
			MsgID:     1005,
			Timestamp: time.Now().UnixNano(),
			Payload:   []byte("msg 1005"),
		},
	}

	segPath := createTestSegment(t, tmpDir, 1000, entries)

	r, err := NewReader(segPath)
	if err != nil {
		t.Fatalf("NewReader() error = %v", err)
	}
	defer r.Close()

	// Find existing message
	entry, _, _, err := r.FindByMessageID(1001)
	if err != nil {
		t.Fatalf("FindByMessageID(1001) error = %v", err)
	}

	if entry.MsgID != 1001 {
		t.Errorf("FindByMessageID(1001) MsgID = %d, want 1001", entry.MsgID)
	}

	if string(entry.Payload) != "msg 1001" {
		t.Errorf("FindByMessageID(1001) Payload = %s, want 'msg 1001'", entry.Payload)
	}

	// Find last message
	entry, _, _, err = r.FindByMessageID(1005)
	if err != nil {
		t.Fatalf("FindByMessageID(1005) error = %v", err)
	}

	if entry.MsgID != 1005 {
		t.Errorf("FindByMessageID(1005) MsgID = %d, want 1005", entry.MsgID)
	}

	// Find non-existent message (before range)
	_, _, _, err = r.FindByMessageID(999)
	if err == nil {
		t.Error("FindByMessageID(999) should fail for non-existent message")
	}

	// Find non-existent message (in gap)
	_, _, _, err = r.FindByMessageID(1003)
	if err == nil {
		t.Error("FindByMessageID(1003) should fail for message in gap")
	}

	// Find non-existent message (after range)
	_, _, _, err = r.FindByMessageID(2000)
	if err == nil {
		t.Error("FindByMessageID(2000) should fail for non-existent message")
	}
}

func TestReader_FindByTimestamp(t *testing.T) {
	tmpDir := t.TempDir()

	// Create entries with different timestamps
	baseTime := time.Now().UnixNano()
	entries := []*format.Entry{
		{
			Type:      format.EntryTypeData,
			Flags:     format.EntryFlagNone,
			MsgID:     1000,
			Timestamp: baseTime,
			Payload:   []byte("msg at base time"),
		},
		{
			Type:      format.EntryTypeData,
			Flags:     format.EntryFlagNone,
			MsgID:     1001,
			Timestamp: baseTime + 1000,
			Payload:   []byte("msg at base+1000"),
		},
		{
			Type:      format.EntryTypeData,
			Flags:     format.EntryFlagNone,
			MsgID:     1002,
			Timestamp: baseTime + 2000,
			Payload:   []byte("msg at base+2000"),
		},
	}

	segPath := createTestSegment(t, tmpDir, 1000, entries)

	r, err := NewReader(segPath)
	if err != nil {
		t.Fatalf("NewReader() error = %v", err)
	}
	defer r.Close()

	// Find entry at exact timestamp
	entry, _, _, err := r.FindByTimestamp(baseTime + 1000)
	if err != nil {
		t.Fatalf("FindByTimestamp() error = %v", err)
	}

	if entry.MsgID != 1001 {
		t.Errorf("FindByTimestamp() MsgID = %d, want 1001", entry.MsgID)
	}

	// Find entry at or after timestamp (should return next entry)
	entry, _, _, err = r.FindByTimestamp(baseTime + 500)
	if err != nil {
		t.Fatalf("FindByTimestamp(base+500) error = %v", err)
	}

	if entry.MsgID != 1001 {
		t.Errorf("FindByTimestamp(base+500) MsgID = %d, want 1001", entry.MsgID)
	}

	// Find before all timestamps
	entry, _, _, err = r.FindByTimestamp(baseTime - 1000)
	if err != nil {
		t.Fatalf("FindByTimestamp(base-1000) error = %v", err)
	}

	if entry.MsgID != 1000 {
		t.Errorf("FindByTimestamp(base-1000) MsgID = %d, want 1000", entry.MsgID)
	}

	// Find after all timestamps
	_, _, _, err = r.FindByTimestamp(baseTime + 10000)
	if err == nil {
		t.Error("FindByTimestamp(base+10000) should fail when no entries after timestamp")
	}
}

func TestReader_ScanAll(t *testing.T) {
	tmpDir := t.TempDir()

	// Create test segment
	entryCount := 5
	entries := make([]*format.Entry, entryCount)
	for i := 0; i < entryCount; i++ {
		entries[i] = &format.Entry{
			Type:      format.EntryTypeData,
			Flags:     format.EntryFlagNone,
			MsgID:     uint64(1000 + i),
			Timestamp: time.Now().UnixNano(),
			Payload:   []byte("test"),
		}
	}

	segPath := createTestSegment(t, tmpDir, 1000, entries)

	r, err := NewReader(segPath)
	if err != nil {
		t.Fatalf("NewReader() error = %v", err)
	}
	defer r.Close()

	// Scan all entries
	count := 0
	err = r.ScanAll(func(entry *format.Entry, offset uint64) error {
		expectedMsgID := uint64(1000 + count)
		if entry.MsgID != expectedMsgID {
			t.Errorf("ScanAll() entry %d MsgID = %d, want %d", count, entry.MsgID, expectedMsgID)
		}

		if count == 0 && offset != format.SegmentHeaderSize {
			t.Errorf("ScanAll() first entry offset = %d, want %d", offset, format.SegmentHeaderSize)
		}

		count++
		return nil
	})

	if err != nil {
		t.Errorf("ScanAll() error = %v", err)
	}

	if count != entryCount {
		t.Errorf("ScanAll() visited %d entries, want %d", count, entryCount)
	}
}

func TestReader_ScanAll_EarlyStop(t *testing.T) {
	tmpDir := t.TempDir()

	entries := make([]*format.Entry, 5)
	for i := 0; i < 5; i++ {
		entries[i] = &format.Entry{
			Type:      format.EntryTypeData,
			Flags:     format.EntryFlagNone,
			MsgID:     uint64(1000 + i),
			Timestamp: time.Now().UnixNano(),
			Payload:   []byte("test"),
		}
	}

	segPath := createTestSegment(t, tmpDir, 1000, entries)

	r, err := NewReader(segPath)
	if err != nil {
		t.Fatalf("NewReader() error = %v", err)
	}
	defer r.Close()

	// Stop after 3 entries
	count := 0
	stopErr := fmt.Errorf("stop scanning")
	err = r.ScanAll(func(entry *format.Entry, offset uint64) error {
		count++
		if count >= 3 {
			return stopErr
		}
		return nil
	})

	if err != stopErr {
		t.Errorf("ScanAll() with early stop error = %v, want %v", err, stopErr)
	}

	if count != 3 {
		t.Errorf("ScanAll() visited %d entries, want 3", count)
	}
}

func TestReader_NoIndex(t *testing.T) {
	tmpDir := t.TempDir()

	// Create segment
	entries := []*format.Entry{
		{
			Type:      format.EntryTypeData,
			Flags:     format.EntryFlagNone,
			MsgID:     1000,
			Timestamp: time.Now().UnixNano(),
			Payload:   []byte("test"),
		},
		{
			Type:      format.EntryTypeData,
			Flags:     format.EntryFlagNone,
			MsgID:     1001,
			Timestamp: time.Now().UnixNano(),
			Payload:   []byte("test"),
		},
	}

	segPath := createTestSegment(t, tmpDir, 1000, entries)

	// Remove index file
	indexPath := filepath.Join(tmpDir, FormatIndexName(1000))
	os.Remove(indexPath)

	r, err := NewReader(segPath)
	if err != nil {
		t.Fatalf("NewReader() error = %v", err)
	}
	defer r.Close()

	if r.HasIndex() {
		t.Error("HasIndex() = true, want false after index removal")
	}

	// FindByMessageID should still work (falls back to sequential scan)
	entry, _, _, err := r.FindByMessageID(1001)
	if err != nil {
		t.Fatalf("FindByMessageID() without index error = %v", err)
	}

	if entry.MsgID != 1001 {
		t.Errorf("FindByMessageID() MsgID = %d, want 1001", entry.MsgID)
	}
}

func TestReader_Close(t *testing.T) {
	tmpDir := t.TempDir()

	entries := []*format.Entry{
		{
			Type:      format.EntryTypeData,
			Flags:     format.EntryFlagNone,
			MsgID:     1000,
			Timestamp: time.Now().UnixNano(),
			Payload:   []byte("test"),
		},
	}

	segPath := createTestSegment(t, tmpDir, 1000, entries)

	r, err := NewReader(segPath)
	if err != nil {
		t.Fatalf("NewReader() error = %v", err)
	}

	if err := r.Close(); err != nil {
		t.Errorf("Close() error = %v", err)
	}

	// Double close should be safe
	if err := r.Close(); err != nil {
		t.Errorf("second Close() error = %v", err)
	}
}
