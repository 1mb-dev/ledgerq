package queue

import (
	"testing"
	"time"

	"github.com/1mb-dev/ledgerq/internal/format"
)

// TestQueue_Priority_BasicOrdering tests that messages are dequeued in priority order
func TestQueue_Priority_BasicOrdering(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.EnablePriorities = true

	q, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("failed to create queue: %v", err)
	}
	defer q.Close()

	// Enqueue messages in mixed priority order
	messages := []struct {
		priority uint8
		data     string
	}{
		{format.PriorityLow, "low1"},
		{format.PriorityHigh, "high1"},
		{format.PriorityMedium, "medium1"},
		{format.PriorityLow, "low2"},
		{format.PriorityHigh, "high2"},
		{format.PriorityMedium, "medium2"},
	}

	for _, msg := range messages {
		if _, err := q.EnqueueWithPriority([]byte(msg.data), msg.priority); err != nil {
			t.Fatalf("failed to enqueue %s: %v", msg.data, err)
		}
	}

	// Dequeue and verify priority order: all High, then all Medium, then all Low
	expectedOrder := []string{"high1", "high2", "medium1", "medium2", "low1", "low2"}

	for i, expected := range expectedOrder {
		msg, err := q.Dequeue()
		if err != nil {
			t.Fatalf("failed to dequeue message %d: %v", i, err)
		}
		if string(msg.Payload) != expected {
			t.Errorf("message %d: got %q, want %q", i, string(msg.Payload), expected)
		}
	}
}

// TestQueue_Priority_StarvationPrevention tests that low-priority messages are promoted after starvation window
func TestQueue_Priority_StarvationPrevention(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.EnablePriorities = true
	opts.PriorityStarvationWindow = 5 * time.Millisecond // Short window for testing

	q, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("failed to create queue: %v", err)
	}
	defer q.Close()

	// Enqueue a low-priority message
	if _, err := q.EnqueueWithPriority([]byte("low-starved"), format.PriorityLow); err != nil {
		t.Fatalf("failed to enqueue low-priority message: %v", err)
	}

	// Wait for starvation window to pass
	time.Sleep(10 * time.Millisecond)

	// Enqueue high-priority messages
	if _, err := q.EnqueueWithPriority([]byte("high1"), format.PriorityHigh); err != nil {
		t.Fatalf("failed to enqueue high-priority message: %v", err)
	}
	if _, err := q.EnqueueWithPriority([]byte("high2"), format.PriorityHigh); err != nil {
		t.Fatalf("failed to enqueue high-priority message: %v", err)
	}

	// First dequeue should return the starved low-priority message
	msg, err := q.Dequeue()
	if err != nil {
		t.Fatalf("failed to dequeue: %v", err)
	}
	if string(msg.Payload) != "low-starved" {
		t.Errorf("expected starved low-priority message, got %q", string(msg.Payload))
	}

	// Next dequeues should return high-priority messages
	msg, err = q.Dequeue()
	if err != nil {
		t.Fatalf("failed to dequeue: %v", err)
	}
	if string(msg.Payload) != "high1" {
		t.Errorf("expected high1, got %q", string(msg.Payload))
	}

	msg, err = q.Dequeue()
	if err != nil {
		t.Fatalf("failed to dequeue: %v", err)
	}
	if string(msg.Payload) != "high2" {
		t.Errorf("expected high2, got %q", string(msg.Payload))
	}
}

// TestQueue_Priority_WithTTL tests priority queue with TTL expiration
func TestQueue_Priority_WithTTL(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.EnablePriorities = true

	q, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("failed to create queue: %v", err)
	}
	defer q.Close()

	// Enqueue high-priority message with short TTL
	if _, err := q.EnqueueWithAllOptions([]byte("high-expired"), format.PriorityHigh, 2*time.Millisecond, nil); err != nil {
		t.Fatalf("failed to enqueue: %v", err)
	}

	// Enqueue low-priority message without TTL
	if _, err := q.EnqueueWithPriority([]byte("low-valid"), format.PriorityLow); err != nil {
		t.Fatalf("failed to enqueue: %v", err)
	}

	// Wait for high-priority message to expire
	time.Sleep(5 * time.Millisecond)

	// Dequeue should skip expired high-priority and return low-priority
	msg, err := q.Dequeue()
	if err != nil {
		t.Fatalf("failed to dequeue: %v", err)
	}
	if string(msg.Payload) != "low-valid" {
		t.Errorf("expected low-valid, got %q", string(msg.Payload))
	}
}

// TestQueue_Priority_WithHeaders tests priority queue with headers
func TestQueue_Priority_WithHeaders(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.EnablePriorities = true

	q, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("failed to create queue: %v", err)
	}
	defer q.Close()

	headers := map[string]string{
		"content-type": "application/json",
		"trace-id":     "abc-123",
	}

	// Enqueue high-priority message with headers
	if _, err := q.EnqueueWithAllOptions([]byte("high-data"), format.PriorityHigh, 0, headers); err != nil {
		t.Fatalf("failed to enqueue: %v", err)
	}

	// Enqueue low-priority message
	if _, err := q.EnqueueWithPriority([]byte("low-data"), format.PriorityLow); err != nil {
		t.Fatalf("failed to enqueue: %v", err)
	}

	// Dequeue should return high-priority message with headers
	msg, err := q.Dequeue()
	if err != nil {
		t.Fatalf("failed to dequeue: %v", err)
	}
	if string(msg.Payload) != "high-data" {
		t.Errorf("expected high-data, got %q", string(msg.Payload))
	}
	if msg.Headers["content-type"] != "application/json" {
		t.Errorf("expected content-type header, got %q", msg.Headers["content-type"])
	}
	if msg.Headers["trace-id"] != "abc-123" {
		t.Errorf("expected trace-id header, got %q", msg.Headers["trace-id"])
	}
}

// TestQueue_Priority_FIFOMode tests that FIFO mode still works when EnablePriorities is false
func TestQueue_Priority_FIFOMode(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.EnablePriorities = false // FIFO mode

	q, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("failed to create queue: %v", err)
	}
	defer q.Close()

	// Enqueue messages with different priorities (should be ignored in FIFO mode)
	messages := []struct {
		priority uint8
		data     string
	}{
		{format.PriorityLow, "first"},
		{format.PriorityHigh, "second"},
		{format.PriorityMedium, "third"},
	}

	for _, msg := range messages {
		if _, err := q.EnqueueWithPriority([]byte(msg.data), msg.priority); err != nil {
			t.Fatalf("failed to enqueue %s: %v", msg.data, err)
		}
	}

	// Dequeue should return in FIFO order (insertion order)
	expectedOrder := []string{"first", "second", "third"}

	for i, expected := range expectedOrder {
		msg, err := q.Dequeue()
		if err != nil {
			t.Fatalf("failed to dequeue message %d: %v", i, err)
		}
		if string(msg.Payload) != expected {
			t.Errorf("message %d: got %q, want %q", i, string(msg.Payload), expected)
		}
	}
}

// TestQueue_Priority_Batch tests that batch enqueue works with priority mode enabled
func TestQueue_Priority_Batch(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.EnablePriorities = true

	q, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("failed to create queue: %v", err)
	}
	defer q.Close()

	// EnqueueBatch doesn't support priorities yet - all messages get default (PriorityLow)
	// This test verifies batch enqueue works in priority mode
	batch := [][]byte{
		[]byte("batch1"),
		[]byte("batch2"),
		[]byte("batch3"),
	}

	if _, err := q.EnqueueBatch(batch); err != nil {
		t.Fatalf("failed to enqueue batch: %v", err)
	}

	// Dequeue and verify FIFO order within same priority
	expectedOrder := []string{"batch1", "batch2", "batch3"}

	for i, expected := range expectedOrder {
		msg, err := q.Dequeue()
		if err != nil {
			t.Fatalf("failed to dequeue message %d: %v", i, err)
		}
		if string(msg.Payload) != expected {
			t.Errorf("message %d: got %q, want %q", i, string(msg.Payload), expected)
		}
	}
}

// TestQueue_Priority_Empty tests dequeue from empty priority queue
func TestQueue_Priority_Empty(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.EnablePriorities = true

	q, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("failed to create queue: %v", err)
	}
	defer q.Close()

	// Dequeue from empty queue should return error
	_, err = q.Dequeue()
	if err == nil {
		t.Error("expected error when dequeueing from empty queue")
	}
}

// TestQueue_Priority_Persistence tests that priority index is rebuilt correctly after restart
func TestQueue_Priority_Persistence(t *testing.T) {
	dir := t.TempDir()

	// First queue: enqueue messages
	{
		opts := DefaultOptions(dir)
		opts.EnablePriorities = true

		q, err := Open(dir, opts)
		if err != nil {
			t.Fatalf("failed to create queue: %v", err)
		}

		// Enqueue messages with different priorities
		if _, err := q.EnqueueWithPriority([]byte("low1"), format.PriorityLow); err != nil {
			t.Fatalf("failed to enqueue: %v", err)
		}
		if _, err := q.EnqueueWithPriority([]byte("high1"), format.PriorityHigh); err != nil {
			t.Fatalf("failed to enqueue: %v", err)
		}
		if _, err := q.EnqueueWithPriority([]byte("medium1"), format.PriorityMedium); err != nil {
			t.Fatalf("failed to enqueue: %v", err)
		}

		q.Close()
	}

	// Second queue: reopen and verify priority order is preserved
	{
		opts := DefaultOptions(dir)
		opts.EnablePriorities = true

		q, err := Open(dir, opts)
		if err != nil {
			t.Fatalf("failed to reopen queue: %v", err)
		}
		defer q.Close()

		// Dequeue and verify priority order is preserved after restart
		expectedOrder := []string{"high1", "medium1", "low1"}

		for i, expected := range expectedOrder {
			msg, err := q.Dequeue()
			if err != nil {
				t.Fatalf("failed to dequeue message %d: %v", i, err)
			}
			if string(msg.Payload) != expected {
				t.Errorf("message %d: got %q, want %q", i, string(msg.Payload), expected)
			}
		}
	}
}

// TestQueue_BatchWithOptions_TTL tests batch enqueue with TTL
func TestQueue_BatchWithOptions_TTL(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.EnablePriorities = true

	q, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("failed to create queue: %v", err)
	}
	defer q.Close()

	// Enqueue batch with TTL
	messages := []BatchEnqueueOptions{
		{Payload: []byte("expires"), Priority: format.PriorityHigh, TTL: 2 * time.Millisecond},
		{Payload: []byte("persistent"), Priority: format.PriorityLow, TTL: 0},
	}

	_, err = q.EnqueueBatchWithOptions(messages)
	if err != nil {
		t.Fatalf("failed to enqueue batch: %v", err)
	}

	// Wait for first message to expire
	time.Sleep(10 * time.Millisecond)

	// Should get second message (first expired)
	msg, err := q.Dequeue()
	if err != nil {
		t.Fatalf("failed to dequeue: %v", err)
	}
	if string(msg.Payload) != "persistent" {
		t.Errorf("expected persistent, got %q", string(msg.Payload))
	}
}

// TestQueue_BatchWithOptions_Headers tests batch enqueue with headers
func TestQueue_BatchWithOptions_Headers(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.EnablePriorities = true

	q, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("failed to create queue: %v", err)
	}
	defer q.Close()

	// Enqueue batch with headers
	messages := []BatchEnqueueOptions{
		{
			Payload:  []byte("msg1"),
			Priority: format.PriorityHigh,
			Headers: map[string]string{
				"source": "server1",
				"type":   "alert",
			},
		},
		{
			Payload:  []byte("msg2"),
			Priority: format.PriorityLow,
			Headers: map[string]string{
				"source": "server2",
			},
		},
	}

	_, err = q.EnqueueBatchWithOptions(messages)
	if err != nil {
		t.Fatalf("failed to enqueue batch: %v", err)
	}

	// Dequeue high priority first
	msg, err := q.Dequeue()
	if err != nil {
		t.Fatalf("failed to dequeue: %v", err)
	}
	if string(msg.Payload) != "msg1" {
		t.Errorf("expected msg1, got %q", string(msg.Payload))
	}
	if msg.Headers["source"] != "server1" {
		t.Errorf("expected source=server1, got %q", msg.Headers["source"])
	}
	if msg.Headers["type"] != "alert" {
		t.Errorf("expected type=alert, got %q", msg.Headers["type"])
	}
}

// TestQueue_BatchWithOptions_AllFeatures tests batch with all features combined
func TestQueue_BatchWithOptions_AllFeatures(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.EnablePriorities = true

	q, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("failed to create queue: %v", err)
	}
	defer q.Close()

	// Enqueue batch with all features
	messages := []BatchEnqueueOptions{
		{
			Payload:  []byte("low-with-headers"),
			Priority: format.PriorityLow,
			Headers:  map[string]string{"key": "value1"},
		},
		{
			Payload:  []byte("high-with-ttl"),
			Priority: format.PriorityHigh,
			TTL:      time.Hour, // Long TTL so it doesn't expire during test
		},
		{
			Payload:  []byte("medium-all"),
			Priority: format.PriorityMedium,
			TTL:      time.Hour,
			Headers:  map[string]string{"key": "value2"},
		},
	}

	offsets, err := q.EnqueueBatchWithOptions(messages)
	if err != nil {
		t.Fatalf("failed to enqueue batch: %v", err)
	}

	if len(offsets) != 3 {
		t.Fatalf("expected 3 offsets, got %d", len(offsets))
	}

	// Dequeue in priority order
	// High first
	msg, err := q.Dequeue()
	if err != nil {
		t.Fatalf("failed to dequeue: %v", err)
	}
	if string(msg.Payload) != "high-with-ttl" {
		t.Errorf("expected high-with-ttl, got %q", string(msg.Payload))
	}
	if msg.Priority != format.PriorityHigh {
		t.Errorf("expected high priority, got %d", msg.Priority)
	}

	// Medium second
	msg, err = q.Dequeue()
	if err != nil {
		t.Fatalf("failed to dequeue: %v", err)
	}
	if string(msg.Payload) != "medium-all" {
		t.Errorf("expected medium-all, got %q", string(msg.Payload))
	}
	if msg.Headers["key"] != "value2" {
		t.Errorf("expected key=value2, got %q", msg.Headers["key"])
	}

	// Low last
	msg, err = q.Dequeue()
	if err != nil {
		t.Fatalf("failed to dequeue: %v", err)
	}
	if string(msg.Payload) != "low-with-headers" {
		t.Errorf("expected low-with-headers, got %q", string(msg.Payload))
	}
	if msg.Headers["key"] != "value1" {
		t.Errorf("expected key=value1, got %q", msg.Headers["key"])
	}
}

// TestQueue_BatchWithOptions_EmptyBatch tests empty batch error handling
func TestQueue_BatchWithOptions_EmptyBatch(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.EnablePriorities = true

	q, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("failed to create queue: %v", err)
	}
	defer q.Close()

	_, err = q.EnqueueBatchWithOptions([]BatchEnqueueOptions{})
	if err == nil {
		t.Error("expected error for empty batch")
	}
}

// TestQueue_BatchWithOptions_EmptyPayload tests validation of empty payload
func TestQueue_BatchWithOptions_EmptyPayload(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.EnablePriorities = true

	q, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("failed to create queue: %v", err)
	}
	defer q.Close()

	// Try to enqueue with empty payload
	messages := []BatchEnqueueOptions{
		{Payload: []byte("valid"), Priority: format.PriorityHigh},
		{Payload: []byte{}, Priority: format.PriorityLow}, // Empty payload
	}

	_, err = q.EnqueueBatchWithOptions(messages)
	if err == nil {
		t.Error("expected error for empty payload")
	}
	if err != nil && err.Error() != "message 1: payload cannot be empty" {
		t.Errorf("unexpected error message: %v", err)
	}
}

// TestQueue_PriorityValidation consolidates all priority validation tests
func TestQueue_PriorityValidation(t *testing.T) {
	tests := []struct {
		name          string
		invalidPrio   uint8
		expectedError string
		testFunc      func(*Queue, uint8) error
	}{
		{
			name:          "EnqueueWithPriority",
			invalidPrio:   5,
			expectedError: "invalid priority 5 (must be 0-2)",
			testFunc: func(q *Queue, prio uint8) error {
				_, err := q.EnqueueWithPriority([]byte("test"), prio)
				return err
			},
		},
		{
			name:          "EnqueueWithAllOptions",
			invalidPrio:   10,
			expectedError: "invalid priority 10 (must be 0-2)",
			testFunc: func(q *Queue, prio uint8) error {
				_, err := q.EnqueueWithAllOptions([]byte("test"), prio, 0, nil)
				return err
			},
		},
		{
			name:          "EnqueueBatchWithOptions",
			invalidPrio:   7,
			expectedError: "message 0: invalid priority 7 (must be 0-2)",
			testFunc: func(q *Queue, prio uint8) error {
				messages := []BatchEnqueueOptions{
					{Payload: []byte("test"), Priority: prio},
				}
				_, err := q.EnqueueBatchWithOptions(messages)
				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			opts := DefaultOptions(dir)
			opts.EnablePriorities = true

			q, err := Open(dir, opts)
			if err != nil {
				t.Fatalf("failed to create queue: %v", err)
			}
			defer q.Close()

			// Test invalid priority
			err = tt.testFunc(q, tt.invalidPrio)
			if err == nil {
				t.Error("expected error for invalid priority")
			}
			if err != nil && err.Error() != tt.expectedError {
				t.Errorf("unexpected error message: got %v, want %s", err, tt.expectedError)
			}
		})
	}
}
