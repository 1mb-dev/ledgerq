package segment

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/1mb-dev/ledgerq/internal/format"
)

func TestNewManager(t *testing.T) {
	tmpDir := t.TempDir()

	opts := DefaultManagerOptions(tmpDir)
	m, err := NewManager(opts)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}
	defer m.Close()

	if m.IsClosed() {
		t.Error("IsClosed() = true, want false for new manager")
	}

	// Should have created an active segment
	activeSeg := m.GetActiveSegment()
	if activeSeg == nil {
		t.Fatal("GetActiveSegment() = nil, want active segment")
	}

	if activeSeg.BaseOffset != 1 {
		t.Errorf("GetActiveSegment().BaseOffset = %d, want 1", activeSeg.BaseOffset)
	}
}

func TestNewManager_WithExistingSegments(t *testing.T) {
	tmpDir := t.TempDir()

	// Create a segment manually
	entries := []*format.Entry{
		{
			Type:      format.EntryTypeData,
			Flags:     format.EntryFlagNone,
			MsgID:     1000,
			Timestamp: time.Now().UnixNano(),
			Payload:   []byte("test"),
		},
	}
	createTestSegment(t, tmpDir, 1000, entries)

	opts := DefaultManagerOptions(tmpDir)
	m, err := NewManager(opts)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}
	defer m.Close()

	// Should discover the existing segment
	segments := m.GetSegments()
	if len(segments) < 1 {
		t.Errorf("GetSegments() returned %d segments, want at least 1", len(segments))
	}
}

func TestManager_Write(t *testing.T) {
	tmpDir := t.TempDir()

	opts := DefaultManagerOptions(tmpDir)
	m, err := NewManager(opts)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}
	defer m.Close()

	// Write an entry
	entry := &format.Entry{
		Type:      format.EntryTypeData,
		Flags:     format.EntryFlagNone,
		MsgID:     1000,
		Timestamp: time.Now().UnixNano(),
		Payload:   []byte("test message"),
	}

	offset, err := m.Write(entry)
	if err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	if offset != format.SegmentHeaderSize {
		t.Errorf("Write() offset = %d, want %d", offset, format.SegmentHeaderSize)
	}

	// Check stats
	stats := m.GetActiveWriterStats()
	if stats == nil {
		t.Fatal("GetActiveWriterStats() = nil")
	}

	if stats.MessagesWritten != 1 {
		t.Errorf("MessagesWritten = %d, want 1", stats.MessagesWritten)
	}

	if stats.BytesWritten == 0 {
		t.Error("BytesWritten = 0, want > 0")
	}
}

func TestManager_WriteMultiple(t *testing.T) {
	tmpDir := t.TempDir()

	opts := DefaultManagerOptions(tmpDir)
	m, err := NewManager(opts)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}
	defer m.Close()

	// Write multiple entries
	count := 10
	for i := 0; i < count; i++ {
		entry := &format.Entry{
			Type:      format.EntryTypeData,
			Flags:     format.EntryFlagNone,
			MsgID:     uint64(1000 + i),
			Timestamp: time.Now().UnixNano(),
			Payload:   []byte("test message"),
		}

		if _, err := m.Write(entry); err != nil {
			t.Fatalf("Write() %d error = %v", i, err)
		}
	}

	stats := m.GetActiveWriterStats()
	if stats.MessagesWritten != uint64(count) {
		t.Errorf("MessagesWritten = %d, want %d", stats.MessagesWritten, count)
	}
}

func TestManager_RotateBySize(t *testing.T) {
	tmpDir := t.TempDir()

	opts := DefaultManagerOptions(tmpDir)
	opts.RotationPolicy = RotateBySize
	opts.MaxSegmentSize = 500 // Very small size to trigger rotation

	m, err := NewManager(opts)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}
	defer m.Close()

	initialBaseOffset := m.GetActiveSegment().BaseOffset

	// Write entries until rotation happens
	for i := 0; i < 20; i++ {
		entry := &format.Entry{
			Type:      format.EntryTypeData,
			Flags:     format.EntryFlagNone,
			MsgID:     uint64(1000 + i),
			Timestamp: time.Now().UnixNano(),
			Payload:   []byte("test message with some payload to increase size"),
		}

		if _, err := m.Write(entry); err != nil {
			t.Fatalf("Write() %d error = %v", i, err)
		}

		// Check if rotation happened
		if m.GetActiveSegment().BaseOffset != initialBaseOffset {
			// Rotation happened!
			segments := m.GetSegments()
			if len(segments) < 1 {
				t.Error("Expected at least one closed segment after rotation")
			}
			return
		}
	}

	t.Error("Expected segment rotation by size, but it didn't happen")
}

func TestManager_RotateByCount(t *testing.T) {
	tmpDir := t.TempDir()

	opts := DefaultManagerOptions(tmpDir)
	opts.RotationPolicy = RotateByCount
	opts.MaxSegmentMessages = 5 // Small count to trigger rotation

	m, err := NewManager(opts)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}
	defer m.Close()

	initialBaseOffset := m.GetActiveSegment().BaseOffset

	// Write exactly MaxSegmentMessages + 1 entries
	for i := 0; i < 6; i++ {
		entry := &format.Entry{
			Type:      format.EntryTypeData,
			Flags:     format.EntryFlagNone,
			MsgID:     uint64(1000 + i),
			Timestamp: time.Now().UnixNano(),
			Payload:   []byte("test"),
		}

		if _, err := m.Write(entry); err != nil {
			t.Fatalf("Write() %d error = %v", i, err)
		}
	}

	// Should have rotated
	if m.GetActiveSegment().BaseOffset == initialBaseOffset {
		t.Error("Expected segment rotation by count, but it didn't happen")
	}

	segments := m.GetSegments()
	if len(segments) < 1 {
		t.Error("Expected at least one closed segment after rotation")
	}
}

func TestManager_OpenReader(t *testing.T) {
	tmpDir := t.TempDir()

	opts := DefaultManagerOptions(tmpDir)
	m, err := NewManager(opts)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}
	defer m.Close()

	// Write an entry
	entry := &format.Entry{
		Type:      format.EntryTypeData,
		Flags:     format.EntryFlagNone,
		MsgID:     1000,
		Timestamp: time.Now().UnixNano(),
		Payload:   []byte("test message"),
	}

	if _, err := m.Write(entry); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	// Sync to ensure data is written
	if err := m.Sync(); err != nil {
		t.Fatalf("Sync() error = %v", err)
	}

	// Open reader for active segment
	baseOffset := m.GetActiveSegment().BaseOffset
	reader, err := m.OpenReader(baseOffset)
	if err != nil {
		t.Fatalf("OpenReader() error = %v", err)
	}
	defer reader.Close()

	// Read the entry back
	if err := reader.SeekToStart(); err != nil {
		t.Fatalf("SeekToStart() error = %v", err)
	}

	readEntry, _, _, err := reader.Read()
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}

	if readEntry.MsgID != entry.MsgID {
		t.Errorf("Read() MsgID = %d, want %d", readEntry.MsgID, entry.MsgID)
	}

	if string(readEntry.Payload) != string(entry.Payload) {
		t.Errorf("Read() Payload = %s, want %s", readEntry.Payload, entry.Payload)
	}
}

func TestManager_OpenReader_NonExistent(t *testing.T) {
	tmpDir := t.TempDir()

	opts := DefaultManagerOptions(tmpDir)
	m, err := NewManager(opts)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}
	defer m.Close()

	// Try to open non-existent segment
	_, err = m.OpenReader(9999)
	if err == nil {
		t.Error("OpenReader() for non-existent segment should fail")
	}
}

func TestManager_Sync(t *testing.T) {
	tmpDir := t.TempDir()

	opts := DefaultManagerOptions(tmpDir)
	m, err := NewManager(opts)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}
	defer m.Close()

	// Write an entry
	entry := &format.Entry{
		Type:      format.EntryTypeData,
		Flags:     format.EntryFlagNone,
		MsgID:     1000,
		Timestamp: time.Now().UnixNano(),
		Payload:   []byte("test"),
	}

	if _, err := m.Write(entry); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	// Sync
	if err := m.Sync(); err != nil {
		t.Errorf("Sync() error = %v", err)
	}
}

func TestManager_Close(t *testing.T) {
	tmpDir := t.TempDir()

	opts := DefaultManagerOptions(tmpDir)
	m, err := NewManager(opts)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}

	// Write an entry
	entry := &format.Entry{
		Type:      format.EntryTypeData,
		Flags:     format.EntryFlagNone,
		MsgID:     1000,
		Timestamp: time.Now().UnixNano(),
		Payload:   []byte("test"),
	}

	if _, err := m.Write(entry); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	if err := m.Close(); err != nil {
		t.Errorf("Close() error = %v", err)
	}

	if !m.IsClosed() {
		t.Error("IsClosed() = false, want true after Close()")
	}

	// Double close should be safe
	if err := m.Close(); err != nil {
		t.Errorf("second Close() error = %v", err)
	}

	// Write after close should fail
	_, err = m.Write(entry)
	if err == nil {
		t.Error("Write() after Close() should fail")
	}
}

func TestManager_WriteAfterClose(t *testing.T) {
	tmpDir := t.TempDir()

	opts := DefaultManagerOptions(tmpDir)
	m, err := NewManager(opts)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}

	m.Close()

	entry := &format.Entry{
		Type:      format.EntryTypeData,
		Flags:     format.EntryFlagNone,
		MsgID:     1000,
		Timestamp: time.Now().UnixNano(),
		Payload:   []byte("test"),
	}

	_, err = m.Write(entry)
	if err == nil {
		t.Error("Write() after Close() should fail")
	}
}

func TestManager_GetSegments(t *testing.T) {
	tmpDir := t.TempDir()

	// Create some existing segments
	for i := 0; i < 3; i++ {
		entries := []*format.Entry{
			{
				Type:      format.EntryTypeData,
				Flags:     format.EntryFlagNone,
				MsgID:     uint64(1000 * (i + 1)),
				Timestamp: time.Now().UnixNano(),
				Payload:   []byte("test"),
			},
		}
		createTestSegment(t, tmpDir, uint64(1000*(i+1)), entries)
	}

	opts := DefaultManagerOptions(tmpDir)
	m, err := NewManager(opts)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}
	defer m.Close()

	segments := m.GetSegments()
	if len(segments) < 3 {
		t.Errorf("GetSegments() returned %d segments, want at least 3", len(segments))
	}

	// Verify segments are sorted by base offset
	for i := 1; i < len(segments); i++ {
		if segments[i].BaseOffset <= segments[i-1].BaseOffset {
			t.Error("GetSegments() not sorted by base offset")
		}
	}
}

func TestDefaultManagerOptions(t *testing.T) {
	tmpDir := t.TempDir()

	opts := DefaultManagerOptions(tmpDir)

	if opts.Directory != tmpDir {
		t.Errorf("Directory = %s, want %s", opts.Directory, tmpDir)
	}

	if opts.RotationPolicy != RotateByBoth {
		t.Errorf("RotationPolicy = %v, want RotateByBoth", opts.RotationPolicy)
	}

	if opts.MaxSegmentSize == 0 {
		t.Error("MaxSegmentSize should be > 0")
	}

	if opts.MaxSegmentMessages == 0 {
		t.Error("MaxSegmentMessages should be > 0")
	}

	if opts.WriterOptions == nil {
		t.Error("WriterOptions should not be nil")
	}
}

func TestManager_GetActiveWriterStats(t *testing.T) {
	tmpDir := t.TempDir()

	opts := DefaultManagerOptions(tmpDir)
	m, err := NewManager(opts)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}
	defer m.Close()

	// Initial stats
	stats := m.GetActiveWriterStats()
	if stats == nil {
		t.Fatal("GetActiveWriterStats() = nil")
	}

	if stats.MessagesWritten != 0 {
		t.Errorf("Initial MessagesWritten = %d, want 0", stats.MessagesWritten)
	}

	if stats.BytesWritten != 0 {
		t.Errorf("Initial BytesWritten = %d, want 0", stats.BytesWritten)
	}

	// Write an entry
	entry := &format.Entry{
		Type:      format.EntryTypeData,
		Flags:     format.EntryFlagNone,
		MsgID:     1000,
		Timestamp: time.Now().UnixNano(),
		Payload:   []byte("test"),
	}

	if _, err := m.Write(entry); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	// Stats should update
	stats = m.GetActiveWriterStats()
	if stats.MessagesWritten != 1 {
		t.Errorf("MessagesWritten = %d, want 1", stats.MessagesWritten)
	}

	if stats.BytesWritten == 0 {
		t.Error("BytesWritten = 0, want > 0")
	}

	if stats.Age == 0 {
		t.Error("Age = 0, want > 0")
	}
}

func TestManager_SegmentFilesCreated(t *testing.T) {
	tmpDir := t.TempDir()

	opts := DefaultManagerOptions(tmpDir)
	m, err := NewManager(opts)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}

	// Write an entry
	entry := &format.Entry{
		Type:      format.EntryTypeData,
		Flags:     format.EntryFlagNone,
		MsgID:     1000,
		Timestamp: time.Now().UnixNano(),
		Payload:   []byte("test"),
	}

	if _, err := m.Write(entry); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	if err := m.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	// Check that segment and index files exist
	baseOffset := uint64(1)
	segPath := filepath.Join(tmpDir, FormatSegmentName(baseOffset))
	idxPath := filepath.Join(tmpDir, FormatIndexName(baseOffset))

	if _, err := os.Stat(segPath); os.IsNotExist(err) {
		t.Errorf("Segment file %s was not created", segPath)
	}

	if _, err := os.Stat(idxPath); os.IsNotExist(err) {
		t.Errorf("Index file %s was not created", idxPath)
	}
}
