package ledgerq_test

import (
	"testing"
	"time"

	"github.com/1mb-dev/ledgerq/pkg/ledgerq"
)

// TestBasicOperations tests basic queue operations using the public API
func TestBasicOperations(t *testing.T) {
	tmpDir := t.TempDir()

	// Open queue with default options
	q, err := ledgerq.Open(tmpDir, nil)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer func() { _ = q.Close() }()

	// Enqueue a message
	offset, err := q.Enqueue([]byte("Hello, World!"))
	if err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}
	if offset == 0 {
		t.Error("Expected non-zero offset")
	}

	// Check stats
	stats := q.Stats()
	if stats.TotalMessages != 1 {
		t.Errorf("TotalMessages = %d, want 1", stats.TotalMessages)
	}
	if stats.PendingMessages != 1 {
		t.Errorf("PendingMessages = %d, want 1", stats.PendingMessages)
	}

	// Dequeue the message
	msg, err := q.Dequeue()
	if err != nil {
		t.Fatalf("Dequeue() error = %v", err)
	}

	if string(msg.Payload) != "Hello, World!" {
		t.Errorf("Payload = %s, want 'Hello, World!'", msg.Payload)
	}

	if msg.ID != 1 {
		t.Errorf("ID = %d, want 1", msg.ID)
	}

	// Check stats after dequeue
	stats = q.Stats()
	if stats.PendingMessages != 0 {
		t.Errorf("PendingMessages = %d, want 0", stats.PendingMessages)
	}
}

// TestBatchOperations tests batch operations using the public API
func TestBatchOperations(t *testing.T) {
	tmpDir := t.TempDir()

	q, err := ledgerq.Open(tmpDir, nil)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer func() { _ = q.Close() }()

	// Enqueue batch
	payloads := [][]byte{
		[]byte("message 1"),
		[]byte("message 2"),
		[]byte("message 3"),
	}

	offsets, err := q.EnqueueBatch(payloads)
	if err != nil {
		t.Fatalf("EnqueueBatch() error = %v", err)
	}

	if len(offsets) != 3 {
		t.Errorf("EnqueueBatch returned %d offsets, want 3", len(offsets))
	}

	// Dequeue batch
	messages, err := q.DequeueBatch(3)
	if err != nil {
		t.Fatalf("DequeueBatch() error = %v", err)
	}

	if len(messages) != 3 {
		t.Errorf("DequeueBatch returned %d messages, want 3", len(messages))
	}

	for i, msg := range messages {
		expected := payloads[i]
		if string(msg.Payload) != string(expected) {
			t.Errorf("Message %d: payload = %s, want %s", i, msg.Payload, expected)
		}
	}
}

// TestWithOptions tests queue with custom options
func TestWithOptions(t *testing.T) {
	tmpDir := t.TempDir()

	opts := &ledgerq.Options{
		AutoSync:           true,
		SyncInterval:       1 * time.Second,
		CompactionInterval: 0, // Disabled
		MaxSegmentSize:     1024 * 1024,
		MaxSegmentMessages: 100,
		RotationPolicy:     ledgerq.RotateByBoth,
		RetentionPolicy: &ledgerq.RetentionPolicy{
			MaxAge:      24 * time.Hour,
			MaxSize:     10 * 1024 * 1024,
			MaxSegments: 10,
			MinSegments: 1,
		},
		Logger: nil, // No logging
	}

	q, err := ledgerq.Open(tmpDir, opts)
	if err != nil {
		t.Fatalf("Open() with options error = %v", err)
	}
	defer func() { _ = q.Close() }()

	// Basic operations should work
	if _, err := q.Enqueue([]byte("test")); err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}

	msg, err := q.Dequeue()
	if err != nil {
		t.Fatalf("Dequeue() error = %v", err)
	}

	if string(msg.Payload) != "test" {
		t.Errorf("Payload = %s, want 'test'", msg.Payload)
	}
}

// TestSeekOperations tests seek functionality
func TestSeekOperations(t *testing.T) {
	tmpDir := t.TempDir()

	q, err := ledgerq.Open(tmpDir, nil)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer func() { _ = q.Close() }()

	// Enqueue some messages
	for i := 1; i <= 10; i++ {
		if _, err := q.Enqueue([]byte("message")); err != nil {
			t.Fatalf("Enqueue(%d) error = %v", i, err)
		}
	}

	// Dequeue first 5
	for i := 0; i < 5; i++ {
		if _, err := q.Dequeue(); err != nil {
			t.Fatalf("Dequeue(%d) error = %v", i, err)
		}
	}

	// Seek back to beginning
	if err := q.SeekToMessageID(1); err != nil {
		t.Fatalf("SeekToMessageID(1) error = %v", err)
	}

	// Should be able to read from beginning again
	msg, err := q.Dequeue()
	if err != nil {
		t.Fatalf("Dequeue after seek error = %v", err)
	}

	if msg.ID != 1 {
		t.Errorf("After seek, got ID %d, want 1", msg.ID)
	}
}

// TestCompaction tests manual compaction using public API
func TestCompaction(t *testing.T) {
	tmpDir := t.TempDir()

	opts := &ledgerq.Options{
		MaxSegmentMessages: 10,
		RotationPolicy:     ledgerq.RotateByCount,
		RetentionPolicy: &ledgerq.RetentionPolicy{
			MaxSegments: 3,
			MinSegments: 1,
		},
	}

	q, err := ledgerq.Open(tmpDir, opts)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer func() { _ = q.Close() }()

	// Write many messages to create multiple segments
	for i := 0; i < 50; i++ {
		if _, err := q.Enqueue([]byte("test")); err != nil {
			t.Fatalf("Enqueue(%d) error = %v", i, err)
		}
	}

	// Consume first 30 messages
	for i := 0; i < 30; i++ {
		if _, err := q.Dequeue(); err != nil {
			t.Fatalf("Dequeue(%d) error = %v", i, err)
		}
	}

	initialSegmentCount := q.Stats().SegmentCount

	// Manually trigger compaction
	result, err := q.Compact()
	if err != nil {
		t.Fatalf("Compact() error = %v", err)
	}

	t.Logf("Compaction removed %d segments, freed %d bytes",
		result.SegmentsRemoved, result.BytesFreed)

	finalSegmentCount := q.Stats().SegmentCount

	// Should have reduced segments
	if finalSegmentCount >= initialSegmentCount {
		t.Errorf("Compaction did not reduce segments: before=%d, after=%d",
			initialSegmentCount, finalSegmentCount)
	}
}

// TestSync tests manual sync operation
func TestSync(t *testing.T) {
	tmpDir := t.TempDir()

	opts := &ledgerq.Options{
		AutoSync: false, // Manual sync mode
	}

	q, err := ledgerq.Open(tmpDir, opts)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer func() { _ = q.Close() }()

	// Enqueue without auto-sync
	if _, err := q.Enqueue([]byte("test")); err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}

	// Manual sync
	if err := q.Sync(); err != nil {
		t.Fatalf("Sync() error = %v", err)
	}

	// Dequeue should work
	msg, err := q.Dequeue()
	if err != nil {
		t.Fatalf("Dequeue() error = %v", err)
	}

	if string(msg.Payload) != "test" {
		t.Errorf("Payload = %s, want 'test'", msg.Payload)
	}
}
