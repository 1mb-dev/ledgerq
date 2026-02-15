package segment

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/1mb-dev/ledgerq/internal/format"
)

func TestNewWriter(t *testing.T) {
	tmpDir := t.TempDir()

	w, err := NewWriter(tmpDir, 1000, nil)
	if err != nil {
		t.Fatalf("NewWriter() error = %v", err)
	}
	defer w.Close()

	if w.BaseOffset() != 1000 {
		t.Errorf("BaseOffset() = %d, want 1000", w.BaseOffset())
	}

	if w.BytesWritten() != 0 {
		t.Errorf("BytesWritten() = %d, want 0", w.BytesWritten())
	}

	if w.MessagesWritten() != 0 {
		t.Errorf("MessagesWritten() = %d, want 0", w.MessagesWritten())
	}

	// Verify segment file exists
	segPath := filepath.Join(tmpDir, FormatSegmentName(1000))
	if _, err := os.Stat(segPath); os.IsNotExist(err) {
		t.Error("segment file was not created")
	}
}

func TestNewWriter_AlreadyExists(t *testing.T) {
	tmpDir := t.TempDir()

	// Create first writer
	w1, err := NewWriter(tmpDir, 1000, nil)
	if err != nil {
		t.Fatalf("NewWriter() first error = %v", err)
	}
	w1.Close()

	// Try to create second writer with same base offset
	_, err = NewWriter(tmpDir, 1000, nil)
	if err == nil {
		t.Error("NewWriter() should fail when segment already exists")
	}
}

func TestWriter_Write(t *testing.T) {
	tmpDir := t.TempDir()

	w, err := NewWriter(tmpDir, 1000, nil)
	if err != nil {
		t.Fatalf("NewWriter() error = %v", err)
	}
	defer w.Close()

	// Write an entry
	entry := &format.Entry{
		Type:      format.EntryTypeData,
		Flags:     format.EntryFlagNone,
		MsgID:     1000,
		Timestamp: time.Now().UnixNano(),
		Payload:   []byte("test message"),
	}

	offset, err := w.Write(entry)
	if err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	if offset != format.SegmentHeaderSize {
		t.Errorf("first Write() offset = %d, want %d", offset, format.SegmentHeaderSize)
	}

	if w.MessagesWritten() != 1 {
		t.Errorf("MessagesWritten() = %d, want 1", w.MessagesWritten())
	}

	if w.BytesWritten() != uint64(entry.TotalSize()) {
		t.Errorf("BytesWritten() = %d, want %d", w.BytesWritten(), entry.TotalSize())
	}
}

func TestWriter_WriteMultiple(t *testing.T) {
	tmpDir := t.TempDir()

	w, err := NewWriter(tmpDir, 1000, nil)
	if err != nil {
		t.Fatalf("NewWriter() error = %v", err)
	}
	defer w.Close()

	// Write multiple entries
	msgCount := 10
	var lastOffset uint64

	for i := 0; i < msgCount; i++ {
		entry := &format.Entry{
			Type:      format.EntryTypeData,
			Flags:     format.EntryFlagNone,
			MsgID:     uint64(1000 + i),
			Timestamp: time.Now().UnixNano(),
			Payload:   []byte("test message"),
		}

		offset, err := w.Write(entry)
		if err != nil {
			t.Fatalf("Write() %d error = %v", i, err)
		}

		if i > 0 && offset <= lastOffset {
			t.Errorf("Write() %d offset = %d, should be > %d", i, offset, lastOffset)
		}

		lastOffset = offset
	}

	if w.MessagesWritten() != uint64(msgCount) {
		t.Errorf("MessagesWritten() = %d, want %d", w.MessagesWritten(), msgCount)
	}
}

func TestWriter_InvalidEntry(t *testing.T) {
	tmpDir := t.TempDir()

	w, err := NewWriter(tmpDir, 1000, nil)
	if err != nil {
		t.Fatalf("NewWriter() error = %v", err)
	}
	defer w.Close()

	// Try to write invalid entry (zero MsgID)
	entry := &format.Entry{
		Type:      format.EntryTypeData,
		Flags:     format.EntryFlagNone,
		MsgID:     0, // Invalid
		Timestamp: time.Now().UnixNano(),
		Payload:   []byte("test"),
	}

	_, err = w.Write(entry)
	if err == nil {
		t.Error("Write() should fail with invalid entry")
	}
}

func TestWriter_WriteAfterClose(t *testing.T) {
	tmpDir := t.TempDir()

	w, err := NewWriter(tmpDir, 1000, nil)
	if err != nil {
		t.Fatalf("NewWriter() error = %v", err)
	}

	w.Close()

	entry := &format.Entry{
		Type:      format.EntryTypeData,
		Flags:     format.EntryFlagNone,
		MsgID:     1000,
		Timestamp: time.Now().UnixNano(),
		Payload:   []byte("test"),
	}

	_, err = w.Write(entry)
	if err == nil {
		t.Error("Write() should fail after Close()")
	}
}

func TestWriter_Sync(t *testing.T) {
	tmpDir := t.TempDir()

	opts := DefaultWriterOptions()
	opts.SyncPolicy = SyncManual

	w, err := NewWriter(tmpDir, 1000, opts)
	if err != nil {
		t.Fatalf("NewWriter() error = %v", err)
	}
	defer w.Close()

	// Write entry
	entry := &format.Entry{
		Type:      format.EntryTypeData,
		Flags:     format.EntryFlagNone,
		MsgID:     1000,
		Timestamp: time.Now().UnixNano(),
		Payload:   []byte("test message"),
	}

	w.Write(entry)

	// Manual sync
	if err := w.Sync(); err != nil {
		t.Errorf("Sync() error = %v", err)
	}
}

func TestWriter_SyncImmediate(t *testing.T) {
	tmpDir := t.TempDir()

	opts := DefaultWriterOptions()
	opts.SyncPolicy = SyncImmediate

	w, err := NewWriter(tmpDir, 1000, opts)
	if err != nil {
		t.Fatalf("NewWriter() error = %v", err)
	}
	defer w.Close()

	// Write should trigger immediate sync
	entry := &format.Entry{
		Type:      format.EntryTypeData,
		Flags:     format.EntryFlagNone,
		MsgID:     1000,
		Timestamp: time.Now().UnixNano(),
		Payload:   []byte("test message"),
	}

	_, err = w.Write(entry)
	if err != nil {
		t.Errorf("Write() with SyncImmediate error = %v", err)
	}
}

func TestWriter_Close(t *testing.T) {
	tmpDir := t.TempDir()

	w, err := NewWriter(tmpDir, 1000, nil)
	if err != nil {
		t.Fatalf("NewWriter() error = %v", err)
	}

	// Write some entries
	for i := 0; i < 5; i++ {
		entry := &format.Entry{
			Type:      format.EntryTypeData,
			Flags:     format.EntryFlagNone,
			MsgID:     uint64(1000 + i),
			Timestamp: time.Now().UnixNano(),
			Payload:   []byte("test"),
		}
		w.Write(entry)
	}

	if err := w.Close(); err != nil {
		t.Errorf("Close() error = %v", err)
	}

	// Verify index file was created
	idxPath := filepath.Join(tmpDir, FormatIndexName(1000))
	if _, err := os.Stat(idxPath); os.IsNotExist(err) {
		t.Error("index file was not created on Close()")
	}

	// Verify header was updated
	segPath := filepath.Join(tmpDir, FormatSegmentName(1000))
	header, err := format.ReadSegmentHeader(segPath)
	if err != nil {
		t.Fatalf("failed to read header: %v", err)
	}

	if header.MinMsgID != 1000 {
		t.Errorf("header.MinMsgID = %d, want 1000", header.MinMsgID)
	}

	if header.MaxMsgID != 1004 {
		t.Errorf("header.MaxMsgID = %d, want 1004", header.MaxMsgID)
	}
}

func TestWriter_SparseIndex(t *testing.T) {
	tmpDir := t.TempDir()

	opts := DefaultWriterOptions()
	opts.IndexInterval = 100 // Small interval for testing

	w, err := NewWriter(tmpDir, 1000, opts)
	if err != nil {
		t.Fatalf("NewWriter() error = %v", err)
	}

	// Write entries to trigger multiple index entries
	for i := 0; i < 20; i++ {
		entry := &format.Entry{
			Type:      format.EntryTypeData,
			Flags:     format.EntryFlagNone,
			MsgID:     uint64(1000 + i),
			Timestamp: time.Now().UnixNano(),
			Payload:   []byte("test message with some payload to trigger indexing"),
		}
		w.Write(entry)
	}

	w.Close()

	// Read and verify index
	idxPath := filepath.Join(tmpDir, FormatIndexName(1000))
	idx, err := format.ReadIndexFile(idxPath)
	if err != nil {
		t.Fatalf("failed to read index: %v", err)
	}

	// Should have multiple index entries due to small interval
	if len(idx.Entries) < 2 {
		t.Errorf("index has %d entries, want at least 2", len(idx.Entries))
	}

	// First entry should be first message
	if idx.Entries[0].MessageID != 1000 {
		t.Errorf("first index entry MessageID = %d, want 1000", idx.Entries[0].MessageID)
	}
}

func TestWriter_IsClosed(t *testing.T) {
	tmpDir := t.TempDir()

	w, err := NewWriter(tmpDir, 1000, nil)
	if err != nil {
		t.Fatalf("NewWriter() error = %v", err)
	}

	if w.IsClosed() {
		t.Error("IsClosed() = true, want false for new writer")
	}

	w.Close()

	if !w.IsClosed() {
		t.Error("IsClosed() = false, want true after Close()")
	}
}

func TestWriter_DoubleClose(t *testing.T) {
	tmpDir := t.TempDir()

	w, err := NewWriter(tmpDir, 1000, nil)
	if err != nil {
		t.Fatalf("NewWriter() error = %v", err)
	}

	if err := w.Close(); err != nil {
		t.Errorf("first Close() error = %v", err)
	}

	if err := w.Close(); err != nil {
		t.Errorf("second Close() error = %v", err)
	}
}

func TestDefaultWriterOptions(t *testing.T) {
	opts := DefaultWriterOptions()

	if opts.SyncPolicy != SyncManual {
		t.Errorf("SyncPolicy = %v, want SyncManual", opts.SyncPolicy)
	}

	if opts.BufferSize != 64*1024 {
		t.Errorf("BufferSize = %d, want 65536", opts.BufferSize)
	}

	if opts.IndexInterval != format.DefaultIndexInterval {
		t.Errorf("IndexInterval = %d, want %d", opts.IndexInterval, format.DefaultIndexInterval)
	}
}
