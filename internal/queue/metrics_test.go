package queue

import (
	"testing"
	"time"

	"github.com/1mb-dev/ledgerq/internal/metrics"
)

func TestQueue_WithMetrics(t *testing.T) {
	tmpDir := t.TempDir()

	// Create metrics collector
	collector := metrics.NewCollector("test_queue")

	opts := DefaultOptions(tmpDir)
	opts.MetricsCollector = collector

	q := setupQueue(t, opts)

	// Enqueue some messages
	enqueueN(t, q, 10)

	// Enqueue a batch
	payloads := [][]byte{
		[]byte("batch message 1"),
		[]byte("batch message 2"),
		[]byte("batch message 3"),
	}
	_, err := q.EnqueueBatch(payloads)
	assertNoError(t, err)

	// Dequeue some messages
	for i := 0; i < 5; i++ {
		_, err := q.Dequeue()
		if err != nil {
			t.Fatalf("Dequeue failed: %v", err)
		}
	}

	// Dequeue a batch
	_, err = q.DequeueBatch(3)
	assertNoError(t, err)

	// Seek operation
	assertNoError(t, q.SeekToMessageID(10))

	// Get metrics snapshot
	snapshot := collector.GetSnapshot()

	// Verify enqueue metrics
	if snapshot.EnqueueTotal != 13 { // 10 individual + 3 batch
		t.Errorf("EnqueueTotal = %d, want 13", snapshot.EnqueueTotal)
	}

	if snapshot.EnqueueBatch != 1 {
		t.Errorf("EnqueueBatch = %d, want 1", snapshot.EnqueueBatch)
	}

	// Verify dequeue metrics
	if snapshot.DequeueTotal != 8 { // 5 individual + 3 batch
		t.Errorf("DequeueTotal = %d, want 8", snapshot.DequeueTotal)
	}

	if snapshot.DequeueBatch != 1 {
		t.Errorf("DequeueBatch = %d, want 1", snapshot.DequeueBatch)
	}

	// Verify seek metrics
	if snapshot.SeekOperations != 1 {
		t.Errorf("SeekOperations = %d, want 1", snapshot.SeekOperations)
	}

	// Verify no errors
	if snapshot.EnqueueErrors != 0 {
		t.Errorf("EnqueueErrors = %d, want 0", snapshot.EnqueueErrors)
	}

	if snapshot.DequeueErrors != 0 {
		t.Errorf("DequeueErrors = %d, want 0", snapshot.DequeueErrors)
	}

	// Verify queue state was updated
	if snapshot.NextMessageID == 0 {
		t.Error("NextMessageID should be set")
	}

	if snapshot.ReadMessageID == 0 {
		t.Error("ReadMessageID should be set")
	}

	// Verify durations were recorded
	if snapshot.EnqueueDurationP50 == 0 {
		t.Error("EnqueueDurationP50 should be non-zero")
	}

	if snapshot.DequeueDurationP50 == 0 {
		t.Error("DequeueDurationP50 should be non-zero")
	}
}

func TestQueue_MetricsErrors(t *testing.T) {
	tmpDir := t.TempDir()

	collector := metrics.NewCollector("test_queue")

	opts := DefaultOptions(tmpDir)
	opts.MetricsCollector = collector

	q := setupQueue(t, opts)

	// Close queue
	assertNoError(t, q.Close())

	// Try operations on closed queue
	_, err := q.Enqueue([]byte("test"))
	if err == nil {
		t.Error("Expected error on closed queue")
	}

	_, err = q.Dequeue()
	if err == nil {
		t.Error("Expected error on closed queue")
	}

	// Check error metrics
	snapshot := collector.GetSnapshot()

	if snapshot.EnqueueErrors == 0 {
		t.Error("EnqueueErrors should be > 0")
	}

	if snapshot.DequeueErrors == 0 {
		t.Error("DequeueErrors should be > 0")
	}
}

func TestQueue_CompactionMetrics(t *testing.T) {
	tmpDir := t.TempDir()

	collector := metrics.NewCollector("test_queue")

	opts := DefaultOptions(tmpDir)
	opts.MetricsCollector = collector
	// Set small segment size to trigger rotation
	opts.SegmentOptions.MaxSegmentSize = 100

	q := setupQueue(t, opts)

	// Enqueue enough messages to create multiple segments
	for i := 0; i < 50; i++ {
		_, err := q.Enqueue([]byte("test message to fill segments"))
		if err != nil {
			t.Fatalf("Enqueue failed: %v", err)
		}
	}

	// Manually trigger compaction (should have no effect without retention policy)
	_, err := q.Compact()
	assertNoError(t, err)

	// Check compaction metrics (may be 0 if nothing was compacted)
	snapshot := collector.GetSnapshot()

	// Compaction was called, so total should be 1
	if snapshot.CompactionsTotal != 1 {
		t.Errorf("CompactionsTotal = %d, want 1", snapshot.CompactionsTotal)
	}
}

func TestQueue_NoopMetrics(t *testing.T) {
	q := setupQueue(t, nil)

	// These operations should work fine with NoopCollector
	_, err := q.Enqueue([]byte("test"))
	assertNoError(t, err)

	_, err = q.Dequeue()
	assertNoError(t, err)
}

func TestQueue_MetricsWithTimestamp(t *testing.T) {
	tmpDir := t.TempDir()

	collector := metrics.NewCollector("test_queue")

	opts := DefaultOptions(tmpDir)
	opts.MetricsCollector = collector

	q := setupQueue(t, opts)

	// Enqueue a message
	_, err := q.Enqueue([]byte("test"))
	assertNoError(t, err)

	// Small delay to ensure different timestamp
	time.Sleep(10 * time.Millisecond)

	// Seek by timestamp
	assertNoError(t, q.SeekToTimestamp(time.Now().Add(-1*time.Second).UnixNano()))

	// Verify seek was recorded
	snapshot := collector.GetSnapshot()

	if snapshot.SeekOperations != 1 {
		t.Errorf("SeekOperations = %d, want 1", snapshot.SeekOperations)
	}
}

func BenchmarkQueue_WithMetrics(b *testing.B) {
	tmpDir := b.TempDir()

	collector := metrics.NewCollector("bench_queue")

	opts := DefaultOptions(tmpDir)
	opts.MetricsCollector = collector

	q, err := Open(tmpDir, opts)
	if err != nil {
		b.Fatalf("Open failed: %v", err)
	}
	defer q.Close()

	payload := []byte("benchmark message")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := q.Enqueue(payload)
		if err != nil {
			b.Fatalf("Enqueue failed: %v", err)
		}
	}
}

func BenchmarkQueue_WithoutMetrics(b *testing.B) {
	tmpDir := b.TempDir()

	q, err := Open(tmpDir, nil) // NoopCollector
	if err != nil {
		b.Fatalf("Open failed: %v", err)
	}
	defer q.Close()

	payload := []byte("benchmark message")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := q.Enqueue(payload)
		if err != nil {
			b.Fatalf("Enqueue failed: %v", err)
		}
	}
}
