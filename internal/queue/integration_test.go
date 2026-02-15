package queue

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/1mb-dev/ledgerq/internal/logging"
	"github.com/1mb-dev/ledgerq/internal/segment"
)

// TestIntegration_FullLifecycle tests a complete queue lifecycle
func TestIntegration_FullLifecycle(t *testing.T) {
	tmpDir := t.TempDir()

	opts := DefaultOptions(tmpDir)
	opts.AutoSync = true

	// Open queue
	q, err := Open(tmpDir, opts)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	// Enqueue messages
	messageCount := 100
	for i := 0; i < messageCount; i++ {
		payload := []byte(fmt.Sprintf("message-%d", i))
		if _, err := q.Enqueue(payload); err != nil {
			t.Fatalf("Enqueue(%d) error = %v", i, err)
		}
	}

	// Check stats
	stats := q.Stats()
	if stats.TotalMessages != uint64(messageCount) {
		t.Errorf("TotalMessages = %d, want %d", stats.TotalMessages, messageCount)
	}
	if stats.PendingMessages != uint64(messageCount) {
		t.Errorf("PendingMessages = %d, want %d", stats.PendingMessages, messageCount)
	}

	// Dequeue half
	for i := 0; i < messageCount/2; i++ {
		msg, err := q.Dequeue()
		if err != nil {
			t.Fatalf("Dequeue(%d) error = %v", i, err)
		}
		expected := fmt.Sprintf("message-%d", i)
		if string(msg.Payload) != expected {
			t.Errorf("Dequeue(%d) payload = %s, want %s", i, msg.Payload, expected)
		}
	}

	// Check stats again
	stats = q.Stats()
	if stats.PendingMessages != uint64(messageCount/2) {
		t.Errorf("PendingMessages after dequeue = %d, want %d", stats.PendingMessages, messageCount/2)
	}

	// Close and reopen
	if err := q.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	q2, err := Open(tmpDir, opts)
	if err != nil {
		t.Fatalf("Reopen() error = %v", err)
	}
	defer func() { _ = q2.Close() }()

	// Verify persistence
	stats = q2.Stats()
	if stats.TotalMessages != uint64(messageCount) {
		t.Errorf("After reopen: TotalMessages = %d, want %d", stats.TotalMessages, messageCount)
	}

	// Read position IS persisted - should continue from where we left off (message 50)
	// We consumed half (50 messages), so 50 should remain
	if stats.PendingMessages != uint64(messageCount/2) {
		t.Errorf("PendingMessages after reopen = %d, want %d", stats.PendingMessages, messageCount/2)
	}

	// Dequeue remaining messages (messages 50-99)
	for i := messageCount / 2; i < messageCount; i++ {
		msg, err := q2.Dequeue()
		if err != nil {
			t.Fatalf("Dequeue(%d) after reopen error = %v", i, err)
		}
		expected := fmt.Sprintf("message-%d", i)
		if string(msg.Payload) != expected {
			t.Errorf("Dequeue(%d) payload = %s, want %s", i, msg.Payload, expected)
		}
	}

	// Should be empty now
	stats = q2.Stats()
	if stats.PendingMessages != 0 {
		t.Errorf("PendingMessages after draining = %d, want 0", stats.PendingMessages)
	}
}

// TestIntegration_MultiSegment tests operations spanning multiple segments
func TestIntegration_MultiSegment(t *testing.T) {
	tmpDir := t.TempDir()

	opts := DefaultOptions(tmpDir)
	opts.SegmentOptions.MaxSegmentMessages = 10 // Force small segments
	opts.SegmentOptions.RotationPolicy = segment.RotateByCount

	q, err := Open(tmpDir, opts)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer func() { _ = q.Close() }()

	// Write enough messages to span multiple segments
	messageCount := 50
	for i := 0; i < messageCount; i++ {
		payload := []byte(fmt.Sprintf("msg-%d", i))
		if _, err := q.Enqueue(payload); err != nil {
			t.Fatalf("Enqueue(%d) error = %v", i, err)
		}
	}

	// Verify multiple segments created
	stats := q.Stats()
	if stats.SegmentCount < 3 {
		t.Errorf("Expected at least 3 segments, got %d", stats.SegmentCount)
	}

	// Read all messages back
	for i := 0; i < messageCount; i++ {
		msg, err := q.Dequeue()
		if err != nil {
			t.Fatalf("Dequeue(%d) error = %v", i, err)
		}
		expected := fmt.Sprintf("msg-%d", i)
		if string(msg.Payload) != expected {
			t.Errorf("Dequeue(%d) payload = %s, want %s", i, msg.Payload, expected)
		}
	}
}

// TestIntegration_ReplayOperations tests replay functionality
func TestIntegration_ReplayOperations(t *testing.T) {
	tmpDir := t.TempDir()

	q, err := Open(tmpDir, nil)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer func() { _ = q.Close() }()

	// Enqueue messages with delays to get different timestamps
	messageCount := 20
	timestamps := make([]int64, messageCount)
	for i := 0; i < messageCount; i++ {
		timestamps[i] = time.Now().UnixNano()
		payload := []byte(fmt.Sprintf("timestamped-%d", i))
		if _, err := q.Enqueue(payload); err != nil {
			t.Fatalf("Enqueue(%d) error = %v", i, err)
		}
		if i%5 == 0 {
			time.Sleep(10 * time.Millisecond) // Create timestamp gaps
		}
	}

	// Consume some messages
	for i := 0; i < 5; i++ {
		if _, err := q.Dequeue(); err != nil {
			t.Fatalf("Initial Dequeue(%d) error = %v", i, err)
		}
	}

	// Seek back to beginning by message ID
	if err := q.SeekToMessageID(1); err != nil {
		t.Fatalf("SeekToMessageID(1) error = %v", err)
	}

	// Should be able to read from beginning again
	msg, err := q.Dequeue()
	if err != nil {
		t.Fatalf("Dequeue after seek error = %v", err)
	}
	if msg.ID != 1 {
		t.Errorf("After SeekToMessageID(1), got ID %d, want 1", msg.ID)
	}

	// Seek to middle by timestamp
	midTimestamp := timestamps[10]
	if err := q.SeekToTimestamp(midTimestamp); err != nil {
		t.Fatalf("SeekToTimestamp error = %v", err)
	}

	msg, err = q.Dequeue()
	if err != nil {
		t.Fatalf("Dequeue after timestamp seek error = %v", err)
	}
	if msg.ID < 10 || msg.ID > 12 {
		t.Errorf("After SeekToTimestamp, got ID %d, expected around 10-12", msg.ID)
	}
}

// TestIntegration_WithCompaction tests compaction in realistic scenario
func TestIntegration_WithCompaction(t *testing.T) {
	tmpDir := t.TempDir()

	opts := DefaultOptions(tmpDir)
	opts.SegmentOptions.MaxSegmentMessages = 10
	opts.SegmentOptions.RotationPolicy = segment.RotateByCount
	opts.SegmentOptions.RetentionPolicy = &segment.RetentionPolicy{
		MaxSegments: 3,
		MinSegments: 1,
	}

	q, err := Open(tmpDir, opts)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer func() { _ = q.Close() }()

	// Write many messages to create multiple segments
	for i := 0; i < 60; i++ {
		payload := []byte(fmt.Sprintf("msg-%d", i))
		if _, err := q.Enqueue(payload); err != nil {
			t.Fatalf("Enqueue(%d) error = %v", i, err)
		}
	}

	segmentsBefore := q.Stats().SegmentCount

	// Compact
	result, err := q.Compact()
	if err != nil {
		t.Fatalf("Compact() error = %v", err)
	}

	if result.SegmentsRemoved == 0 {
		t.Error("Compact() removed 0 segments, expected some")
	}

	segmentsAfter := q.Stats().SegmentCount

	if segmentsAfter >= segmentsBefore {
		t.Errorf("Compaction did not reduce segments: before=%d, after=%d", segmentsBefore, segmentsAfter)
	}

	// Queue should still be functional
	payload := []byte("after-compaction")
	if _, err := q.Enqueue(payload); err != nil {
		t.Fatalf("Enqueue after compaction error = %v", err)
	}
}

// TestIntegration_WithLogging tests logging integration
func TestIntegration_WithLogging(t *testing.T) {
	tmpDir := t.TempDir()

	opts := DefaultOptions(tmpDir)
	opts.Logger = logging.NewDefaultLogger(logging.LevelDebug)

	q, err := Open(tmpDir, opts)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	// These operations should log (visual verification in test output)
	if _, err := q.Enqueue([]byte("test message")); err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}

	batch := [][]byte{
		[]byte("batch-1"),
		[]byte("batch-2"),
		[]byte("batch-3"),
	}
	if _, err := q.EnqueueBatch(batch); err != nil {
		t.Fatalf("EnqueueBatch() error = %v", err)
	}

	if _, err := q.Dequeue(); err != nil {
		t.Fatalf("Dequeue() error = %v", err)
	}

	if err := q.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

// TestIntegration_CrashRecovery simulates crash and recovery
func TestIntegration_CrashRecovery(t *testing.T) {
	tmpDir := t.TempDir()

	// First session: write some messages
	opts1 := DefaultOptions(tmpDir)
	opts1.AutoSync = true // Ensure data is persisted

	q1, err := Open(tmpDir, opts1)
	if err != nil {
		t.Fatalf("Open() first session error = %v", err)
	}

	messageCount := 50
	for i := 0; i < messageCount; i++ {
		payload := []byte(fmt.Sprintf("persistent-%d", i))
		if _, err := q1.Enqueue(payload); err != nil {
			t.Fatalf("Enqueue(%d) error = %v", i, err)
		}
	}

	// Consume some
	for i := 0; i < 20; i++ {
		if _, err := q1.Dequeue(); err != nil {
			t.Fatalf("Dequeue(%d) error = %v", i, err)
		}
	}

	stats1 := q1.Stats()

	// Close gracefully
	if err := q1.Close(); err != nil {
		t.Fatalf("Close() first session error = %v", err)
	}

	// Second session: verify recovery
	q2, err := Open(tmpDir, nil)
	if err != nil {
		t.Fatalf("Open() second session error = %v", err)
	}
	defer func() { _ = q2.Close() }()

	stats2 := q2.Stats()

	// Verify state recovered correctly
	if stats2.TotalMessages != stats1.TotalMessages {
		t.Errorf("TotalMessages after recovery: got %d, want %d", stats2.TotalMessages, stats1.TotalMessages)
	}

	// Read position IS persisted - we consumed 20, so should have 30 remaining
	expectedRemaining := messageCount - 20
	if stats2.PendingMessages != uint64(expectedRemaining) {
		t.Errorf("PendingMessages after recovery: got %d, want %d", stats2.PendingMessages, expectedRemaining)
	}

	// Read remaining messages (21-50)
	for i := 0; i < expectedRemaining; i++ {
		msg, err := q2.Dequeue()
		if err != nil {
			t.Fatalf("Dequeue(%d) after recovery error = %v", i, err)
		}
		// We consumed 20 in first session, so next message should be ID 21
		expectedID := uint64(21 + i)
		if msg.ID != expectedID {
			t.Errorf("Message %d: ID = %d, want %d", i, msg.ID, expectedID)
		}
	}

	// Should be empty now
	if _, err := q2.Dequeue(); err == nil {
		t.Error("Dequeue() on empty queue should fail")
	}
}

// TestIntegration_EmptyPayloads tests handling of empty messages
func TestIntegration_EmptyPayloads(t *testing.T) {
	tmpDir := t.TempDir()

	q, err := Open(tmpDir, nil)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer func() { _ = q.Close() }()

	// Enqueue empty message
	if _, err := q.Enqueue([]byte{}); err != nil {
		t.Fatalf("Enqueue(empty) error = %v", err)
	}

	// Dequeue and verify
	msg, err := q.Dequeue()
	if err != nil {
		t.Fatalf("Dequeue() error = %v", err)
	}

	if len(msg.Payload) != 0 {
		t.Errorf("Empty message payload length = %d, want 0", len(msg.Payload))
	}
}

// TestIntegration_DirectoryCreation tests directory handling
func TestIntegration_DirectoryCreation(t *testing.T) {
	tmpDir := t.TempDir()
	queueDir := tmpDir + "/queue"

	// Create directory first (automatic directory creation not implemented)
	if err := os.MkdirAll(queueDir, 0755); err != nil {
		t.Fatalf("MkdirAll() error = %v", err)
	}

	// Open queue
	q, err := Open(queueDir, nil)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer func() { _ = q.Close() }()

	// Verify directory exists
	if _, err := os.Stat(queueDir); err != nil {
		t.Fatalf("Queue directory not accessible: %v", err)
	}

	// Should be functional
	if _, err := q.Enqueue([]byte("test")); err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}

	// Verify segment files were created
	entries, err := os.ReadDir(queueDir)
	if err != nil {
		t.Fatalf("ReadDir() error = %v", err)
	}

	if len(entries) == 0 {
		t.Error("No segment files created in queue directory")
	}
}
