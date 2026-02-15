package segment

import (
	"os"
	"testing"
	"time"

	"github.com/1mb-dev/ledgerq/internal/format"
)

func TestCompact_NoRetentionPolicy(t *testing.T) {
	tmpDir := t.TempDir()

	opts := DefaultManagerOptions(tmpDir)
	opts.RetentionPolicy = nil  // Disable retention
	opts.MaxSegmentMessages = 5 // Small segments for testing

	mgr, err := NewManager(opts)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}
	defer func() { _ = mgr.Close() }()

	// Write some messages
	for i := 0; i < 15; i++ {
		entry := &format.Entry{
			Type:      format.EntryTypeData,
			Flags:     format.EntryFlagNone,
			MsgID:     uint64(i + 1),
			Timestamp: time.Now().UnixNano(),
			Payload:   []byte("test"),
		}
		if _, err := mgr.Write(entry); err != nil {
			t.Fatalf("Write() error = %v", err)
		}
	}

	// Compact with no retention policy should do nothing
	result, err := mgr.Compact()
	if err != nil {
		t.Fatalf("Compact() error = %v", err)
	}

	if result.SegmentsRemoved != 0 {
		t.Errorf("Compact() removed %d segments, want 0", result.SegmentsRemoved)
	}
}

func TestCompact_ByAge(t *testing.T) {
	tmpDir := t.TempDir()

	opts := DefaultManagerOptions(tmpDir)
	opts.RetentionPolicy = &RetentionPolicy{
		MaxAge:      100 * time.Millisecond,
		MinSegments: 0, // Allow removing all old segments
	}
	opts.MaxSegmentMessages = 3 // Small segments for testing

	mgr, err := NewManager(opts)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}
	defer func() { _ = mgr.Close() }()

	// Write first batch of messages to create multiple segments
	for i := 0; i < 12; i++ {
		entry := &format.Entry{
			Type:      format.EntryTypeData,
			Flags:     format.EntryFlagNone,
			MsgID:     uint64(i + 1),
			Timestamp: time.Now().UnixNano(),
			Payload:   []byte("test"),
		}
		if _, err := mgr.Write(entry); err != nil {
			t.Fatalf("Write() error = %v", err)
		}
	}

	segmentsBefore := len(mgr.GetSegments()) + 1 // +1 for active

	// Ensure we have multiple segments
	if segmentsBefore < 3 {
		t.Skipf("Not enough segments created for test: %d", segmentsBefore)
	}

	// Wait for segments to age
	time.Sleep(150 * time.Millisecond)

	// Write more messages to create fresh segments
	for i := 12; i < 18; i++ {
		entry := &format.Entry{
			Type:      format.EntryTypeData,
			Flags:     format.EntryFlagNone,
			MsgID:     uint64(i + 1),
			Timestamp: time.Now().UnixNano(),
			Payload:   []byte("test"),
		}
		if _, err := mgr.Write(entry); err != nil {
			t.Fatalf("Write() error = %v", err)
		}
	}

	// Compact - should remove old segments
	result, err := mgr.Compact()
	if err != nil {
		t.Fatalf("Compact() error = %v", err)
	}

	if result.SegmentsRemoved == 0 {
		t.Errorf("Compact() removed 0 segments, expected some to be removed")
	}

	segmentsAfter := len(mgr.GetSegments()) + 1 // +1 for active

	if segmentsAfter >= segmentsBefore {
		t.Errorf("Compact() did not reduce segment count: before=%d, after=%d", segmentsBefore, segmentsAfter)
	}
}

func TestCompact_ByCount(t *testing.T) {
	tmpDir := t.TempDir()

	opts := DefaultManagerOptions(tmpDir)
	opts.RetentionPolicy = &RetentionPolicy{
		MaxSegments: 3, // Keep only 3 segments
		MinSegments: 1,
	}
	opts.MaxSegmentMessages = 5 // Small segments for testing

	mgr, err := NewManager(opts)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}
	defer func() { _ = mgr.Close() }()

	// Write enough messages to create 5 segments
	for i := 0; i < 25; i++ {
		entry := &format.Entry{
			Type:      format.EntryTypeData,
			Flags:     format.EntryFlagNone,
			MsgID:     uint64(i + 1),
			Timestamp: time.Now().UnixNano(),
			Payload:   []byte("test"),
		}
		if _, err := mgr.Write(entry); err != nil {
			t.Fatalf("Write() error = %v", err)
		}
	}

	segmentsBefore := len(mgr.GetSegments()) + 1 // +1 for active

	// Compact - should remove old segments to meet MaxSegments
	result, err := mgr.Compact()
	if err != nil {
		t.Fatalf("Compact() error = %v", err)
	}

	if result.SegmentsRemoved == 0 {
		t.Errorf("Compact() removed 0 segments, expected some to be removed")
	}

	segmentsAfter := len(mgr.GetSegments()) + 1 // +1 for active

	// Should have at most MaxSegments + 1 (active)
	maxExpected := opts.RetentionPolicy.MaxSegments + 1
	if segmentsAfter > maxExpected {
		t.Errorf("Compact() left too many segments: after=%d, max=%d", segmentsAfter, maxExpected)
	}

	if segmentsAfter >= segmentsBefore {
		t.Errorf("Compact() did not reduce segment count: before=%d, after=%d", segmentsBefore, segmentsAfter)
	}
}

func TestCompact_BySize(t *testing.T) {
	tmpDir := t.TempDir()

	// Calculate approximate size of one entry
	entry := &format.Entry{
		Type:      format.EntryTypeData,
		Flags:     format.EntryFlagNone,
		MsgID:     1,
		Timestamp: time.Now().UnixNano(),
		Payload:   make([]byte, 100), // 100 bytes payload
	}
	entry.Length = uint32(format.EntryHeaderSize + len(entry.Payload) + 4)
	entrySize := format.EntryHeaderSize + len(entry.Payload) + 4

	// Set MaxSize to roughly 3 segments worth
	maxSize := uint64(entrySize * 15) // ~3 segments of 5 messages each

	opts := DefaultManagerOptions(tmpDir)
	opts.RetentionPolicy = &RetentionPolicy{
		MaxSize:     maxSize,
		MinSegments: 1,
	}
	opts.MaxSegmentMessages = 5 // Small segments for testing

	mgr, err := NewManager(opts)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}
	defer func() { _ = mgr.Close() }()

	// Write enough messages to exceed MaxSize
	for i := 0; i < 30; i++ {
		entry := &format.Entry{
			Type:      format.EntryTypeData,
			Flags:     format.EntryFlagNone,
			MsgID:     uint64(i + 1),
			Timestamp: time.Now().UnixNano(),
			Payload:   make([]byte, 100),
		}
		if _, err := mgr.Write(entry); err != nil {
			t.Fatalf("Write() error = %v", err)
		}
	}

	segmentsBefore := len(mgr.GetSegments()) + 1 // +1 for active

	// Compact - should remove old segments to meet MaxSize
	result, err := mgr.Compact()
	if err != nil {
		t.Fatalf("Compact() error = %v", err)
	}

	if result.SegmentsRemoved == 0 {
		t.Errorf("Compact() removed 0 segments, expected some to be removed")
	}

	segmentsAfter := len(mgr.GetSegments()) + 1 // +1 for active

	if segmentsAfter >= segmentsBefore {
		t.Errorf("Compact() did not reduce segment count: before=%d, after=%d", segmentsBefore, segmentsAfter)
	}

	// Verify total size is under or near MaxSize
	totalSize := uint64(0)
	for _, seg := range mgr.GetSegments() {
		if seg.Size > 0 {
			totalSize += uint64(seg.Size)
		}
	}

	// Allow some leeway since we keep MinSegments
	if totalSize > maxSize*2 {
		t.Errorf("Compact() left too much data: size=%d, max=%d", totalSize, maxSize)
	}
}

func TestCompact_MinSegments(t *testing.T) {
	tmpDir := t.TempDir()

	opts := DefaultManagerOptions(tmpDir)
	opts.RetentionPolicy = &RetentionPolicy{
		MaxAge:      1 * time.Nanosecond, // Expire immediately
		MinSegments: 2,                   // Keep at least 2
	}
	opts.MaxSegmentMessages = 5

	mgr, err := NewManager(opts)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}
	defer func() { _ = mgr.Close() }()

	// Write messages to create multiple segments
	for i := 0; i < 20; i++ {
		entry := &format.Entry{
			Type:      format.EntryTypeData,
			Flags:     format.EntryFlagNone,
			MsgID:     uint64(i + 1),
			Timestamp: time.Now().UnixNano(),
			Payload:   []byte("test"),
		}
		if _, err := mgr.Write(entry); err != nil {
			t.Fatalf("Write() error = %v", err)
		}
	}

	// Wait for all segments to "expire"
	time.Sleep(10 * time.Millisecond)

	// Compact - should keep MinSegments
	_, err = mgr.Compact()
	if err != nil {
		t.Fatalf("Compact() error = %v", err)
	}

	segmentsAfter := len(mgr.GetSegments())

	// Should have kept at least MinSegments (not counting active)
	if segmentsAfter < opts.RetentionPolicy.MinSegments {
		t.Errorf("Compact() removed too many: after=%d, min=%d", segmentsAfter, opts.RetentionPolicy.MinSegments)
	}
}

func TestCompact_EmptyQueue(t *testing.T) {
	tmpDir := t.TempDir()

	opts := DefaultManagerOptions(tmpDir)
	opts.RetentionPolicy = &RetentionPolicy{
		MaxAge:      1 * time.Millisecond,
		MinSegments: 1,
	}

	mgr, err := NewManager(opts)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}
	defer func() { _ = mgr.Close() }()

	// Compact empty queue should not error
	result, err := mgr.Compact()
	if err != nil {
		t.Fatalf("Compact() on empty queue error = %v", err)
	}

	if result.SegmentsRemoved != 0 {
		t.Errorf("Compact() removed %d segments from empty queue, want 0", result.SegmentsRemoved)
	}
}

func TestCompact_AfterClose(t *testing.T) {
	tmpDir := t.TempDir()

	opts := DefaultManagerOptions(tmpDir)
	mgr, err := NewManager(opts)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}

	if err := mgr.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	// Compact after close should error
	_, err = mgr.Compact()
	if err == nil {
		t.Error("Compact() after Close() should fail")
	}
}

func TestCompact_FilesActuallyDeleted(t *testing.T) {
	tmpDir := t.TempDir()

	opts := DefaultManagerOptions(tmpDir)
	opts.RetentionPolicy = &RetentionPolicy{
		MaxSegments: 2,
		MinSegments: 1,
	}
	opts.MaxSegmentMessages = 5

	mgr, err := NewManager(opts)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}
	defer func() { _ = mgr.Close() }()

	// Write messages to create multiple segments
	for i := 0; i < 20; i++ {
		entry := &format.Entry{
			Type:      format.EntryTypeData,
			Flags:     format.EntryFlagNone,
			MsgID:     uint64(i + 1),
			Timestamp: time.Now().UnixNano(),
			Payload:   []byte("test"),
		}
		if _, err := mgr.Write(entry); err != nil {
			t.Fatalf("Write() error = %v", err)
		}
	}

	// Get list of segment files before compaction
	filesBefore, err := os.ReadDir(tmpDir)
	if err != nil {
		t.Fatalf("ReadDir() error = %v", err)
	}

	segmentFilesBefore := 0
	for _, f := range filesBefore {
		if !f.IsDir() && (len(f.Name()) > 4 && f.Name()[len(f.Name())-4:] == ".log") {
			segmentFilesBefore++
		}
	}

	// Compact
	result, err := mgr.Compact()
	if err != nil {
		t.Fatalf("Compact() error = %v", err)
	}

	// Get list of segment files after compaction
	filesAfter, err := os.ReadDir(tmpDir)
	if err != nil {
		t.Fatalf("ReadDir() error = %v", err)
	}

	segmentFilesAfter := 0
	for _, f := range filesAfter {
		if !f.IsDir() && (len(f.Name()) > 4 && f.Name()[len(f.Name())-4:] == ".log") {
			segmentFilesAfter++
		}
	}

	// Verify files were actually deleted
	expectedReduction := result.SegmentsRemoved
	actualReduction := segmentFilesBefore - segmentFilesAfter

	if actualReduction != expectedReduction {
		t.Errorf("File count reduction mismatch: expected=%d, actual=%d", expectedReduction, actualReduction)
	}

	if result.BytesFreed == 0 && result.SegmentsRemoved > 0 {
		t.Error("Compact() removed segments but reported 0 bytes freed")
	}
}

func TestCompact_MultipleRounds(t *testing.T) {
	tmpDir := t.TempDir()

	opts := DefaultManagerOptions(tmpDir)
	opts.RetentionPolicy = &RetentionPolicy{
		MaxAge:      50 * time.Millisecond,
		MinSegments: 1,
	}
	opts.MaxSegmentMessages = 5

	mgr, err := NewManager(opts)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}
	defer func() { _ = mgr.Close() }()

	// Write first batch
	for i := 0; i < 10; i++ {
		entry := &format.Entry{
			Type:      format.EntryTypeData,
			Flags:     format.EntryFlagNone,
			MsgID:     uint64(i + 1),
			Timestamp: time.Now().UnixNano(),
			Payload:   []byte("test"),
		}
		if _, err := mgr.Write(entry); err != nil {
			t.Fatalf("Write() error = %v", err)
		}
	}

	time.Sleep(60 * time.Millisecond)

	// Write second batch
	for i := 10; i < 20; i++ {
		entry := &format.Entry{
			Type:      format.EntryTypeData,
			Flags:     format.EntryFlagNone,
			MsgID:     uint64(i + 1),
			Timestamp: time.Now().UnixNano(),
			Payload:   []byte("test"),
		}
		if _, err := mgr.Write(entry); err != nil {
			t.Fatalf("Write() error = %v", err)
		}
	}

	// First compaction
	result1, err := mgr.Compact()
	if err != nil {
		t.Fatalf("Compact() first error = %v", err)
	}

	if result1.SegmentsRemoved == 0 {
		t.Error("First Compact() removed 0 segments, expected some")
	}

	time.Sleep(60 * time.Millisecond)

	// Write third batch
	for i := 20; i < 30; i++ {
		entry := &format.Entry{
			Type:      format.EntryTypeData,
			Flags:     format.EntryFlagNone,
			MsgID:     uint64(i + 1),
			Timestamp: time.Now().UnixNano(),
			Payload:   []byte("test"),
		}
		if _, err := mgr.Write(entry); err != nil {
			t.Fatalf("Write() error = %v", err)
		}
	}

	// Second compaction
	result2, err := mgr.Compact()
	if err != nil {
		t.Fatalf("Compact() second error = %v", err)
	}

	// Second compaction should also remove segments
	if result2.SegmentsRemoved == 0 {
		t.Error("Second Compact() removed 0 segments, expected some")
	}

	// Verify we still have data
	segments := mgr.GetSegments()
	if len(segments) == 0 && mgr.GetActiveSegment() == nil {
		t.Error("All segments removed, expected some to remain")
	}
}
