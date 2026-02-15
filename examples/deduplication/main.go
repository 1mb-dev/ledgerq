// Package main demonstrates message deduplication in LedgerQ.
//
// This example shows how to:
//   - Enable deduplication with a time window
//   - Detect duplicate messages based on unique IDs
//   - Handle duplicate detection in application code
//   - View deduplication statistics
//
// Run with: go run examples/deduplication/main.go
package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/1mb-dev/ledgerq/pkg/ledgerq"
)

func main() {
	// Create a temporary directory for the queue
	queueDir := "/tmp/ledgerq-dedup-example"
	defer os.RemoveAll(queueDir)

	fmt.Println("=== LedgerQ Message Deduplication Example ===")
	fmt.Println()

	// Configure queue with deduplication enabled
	opts := &ledgerq.Options{
		// Enable deduplication with a 5-minute default window
		DefaultDeduplicationWindow: 5 * time.Minute,

		// Track up to 100,000 unique messages (default)
		MaxDeduplicationEntries: 100000,

		// Other settings
		AutoSync: true,
	}

	// Open the queue
	q, err := ledgerq.Open(queueDir, opts)
	if err != nil {
		log.Fatalf("Failed to open queue: %v", err)
	}
	defer q.Close()

	fmt.Println("✓ Queue opened with deduplication enabled")
	fmt.Printf("  Default window: %s\n", opts.DefaultDeduplicationWindow)
	fmt.Printf("  Max entries: %d\n\n", opts.MaxDeduplicationEntries)

	// Example 1: Basic deduplication
	fmt.Println("Example 1: Basic Deduplication")
	fmt.Println("--------------------------------")

	orderPayload := []byte(`{"order_id": "ORD-12345", "amount": 99.99}`)
	dedupID := "order-12345"

	// First enqueue - should succeed
	offset1, isDup1, err := q.EnqueueWithDedup(orderPayload, dedupID, 0)
	if err != nil {
		log.Fatalf("Failed to enqueue: %v", err)
	}
	fmt.Printf("First enqueue:  offset=%d, duplicate=%v ✓\n", offset1, isDup1)

	// Second enqueue with same ID - should be detected as duplicate
	offset2, isDup2, err := q.EnqueueWithDedup(orderPayload, dedupID, 0)
	if err != nil {
		log.Fatalf("Failed to enqueue: %v", err)
	}
	fmt.Printf("Second enqueue: offset=%d, duplicate=%v ✓\n", offset2, isDup2)

	if isDup2 {
		fmt.Printf("  → Duplicate detected! Original at offset %d\n", offset2)
	}
	fmt.Println()

	// Example 2: Custom deduplication window
	fmt.Println("Example 2: Custom Deduplication Window")
	fmt.Println("---------------------------------------")

	// Enqueue with a very short window for demonstration
	shortWindowPayload := []byte(`{"request_id": "REQ-789", "data": "test"}`)
	shortWindowID := "request-789"

	offset3, isDup3, err := q.EnqueueWithDedup(shortWindowPayload, shortWindowID, 100*time.Millisecond)
	if err != nil {
		log.Fatalf("Failed to enqueue: %v", err)
	}
	fmt.Printf("Enqueued with 100ms window: offset=%d, duplicate=%v ✓\n", offset3, isDup3)

	// Immediately try again - should be duplicate
	offset4, isDup4, err := q.EnqueueWithDedup(shortWindowPayload, shortWindowID, 100*time.Millisecond)
	if err != nil {
		log.Fatalf("Failed to enqueue: %v", err)
	}
	fmt.Printf("Immediate retry: offset=%d, duplicate=%v ✓\n", offset4, isDup4)

	// Wait for window to expire
	fmt.Println("  Waiting for dedup window to expire...")
	time.Sleep(150 * time.Millisecond)

	// Try again after expiration - should succeed
	offset5, isDup5, err := q.EnqueueWithDedup(shortWindowPayload, shortWindowID, 100*time.Millisecond)
	if err != nil {
		log.Fatalf("Failed to enqueue: %v", err)
	}
	fmt.Printf("After window expires: offset=%d, duplicate=%v ✓\n", offset5, isDup5)
	fmt.Println()

	// Example 3: Multiple unique messages
	fmt.Println("Example 3: Multiple Unique Messages")
	fmt.Println("------------------------------------")

	messages := []struct {
		dedupID string
		payload string
	}{
		{"payment-001", `{"payment": "001", "amount": 10.00}`},
		{"payment-002", `{"payment": "002", "amount": 20.00}`},
		{"payment-003", `{"payment": "003", "amount": 30.00}`},
		{"payment-001", `{"payment": "001", "amount": 10.00}`}, // Duplicate of first
	}

	for i, msg := range messages {
		offset, isDup, err := q.EnqueueWithDedup([]byte(msg.payload), msg.dedupID, 0)
		if err != nil {
			log.Fatalf("Failed to enqueue: %v", err)
		}

		status := "NEW"
		if isDup {
			status = "DUPLICATE"
		}
		fmt.Printf("Message %d [%s]: %s (offset=%d)\n", i+1, msg.dedupID, status, offset)
	}
	fmt.Println()

	// Example 4: View statistics
	fmt.Println("Example 4: Deduplication Statistics")
	fmt.Println("------------------------------------")

	stats := q.Stats()
	fmt.Printf("Total messages enqueued: %d\n", stats.TotalMessages)
	fmt.Printf("Pending messages: %d\n", stats.PendingMessages)
	fmt.Printf("Dedup entries tracked: %d\n", stats.DedupTrackedEntries)
	fmt.Println()

	// Example 5: Processing with idempotency
	fmt.Println("Example 5: Idempotent Message Processing")
	fmt.Println("-----------------------------------------")

	// Simulate processing messages with retry logic
	processMessage := func(dedupID string, payload []byte) error {
		// In a real application, this might be a database operation,
		// API call, or other operation that needs idempotency
		fmt.Printf("  Processing: %s\n", dedupID)

		// Enqueue with deduplication to ensure exactly-once semantics
		offset, isDup, err := q.EnqueueWithDedup(payload, dedupID, 5*time.Minute)
		if err != nil {
			return fmt.Errorf("enqueue failed: %w", err)
		}

		if isDup {
			fmt.Printf("  ⚠ Already processed (original offset: %d)\n", offset)
			return nil
		}

		fmt.Printf("  ✓ Processed successfully (offset: %d)\n", offset)
		return nil
	}

	// Process a message
	if err := processMessage("txn-555", []byte(`{"txn": "555"}`)); err != nil {
		log.Printf("Error: %v", err)
	}

	// Simulate a retry (e.g., due to network timeout)
	fmt.Println("  Simulating retry...")
	if err := processMessage("txn-555", []byte(`{"txn": "555"}`)); err != nil {
		log.Printf("Error: %v", err)
	}
	fmt.Println()

	// Example 6: Persistence across restarts
	fmt.Println("Example 6: Persistence Across Restarts")
	fmt.Println("---------------------------------------")

	// Add a message
	persistDedupID := "persist-test"
	persistPayload := []byte(`{"test": "persistence"}`)
	offset6, _, err := q.EnqueueWithDedup(persistPayload, persistDedupID, 1*time.Hour)
	if err != nil {
		log.Fatalf("Failed to enqueue: %v", err)
	}
	fmt.Printf("Enqueued before restart: offset=%d\n", offset6)

	// Close queue (this persists dedup state)
	if err := q.Close(); err != nil {
		log.Fatalf("Failed to close queue: %v", err)
	}
	fmt.Println("Queue closed (dedup state persisted)")

	// Reopen queue
	q2, err := ledgerq.Open(queueDir, opts)
	if err != nil {
		log.Fatalf("Failed to reopen queue: %v", err)
	}
	defer q2.Close()
	fmt.Println("Queue reopened")

	// Try to enqueue same message - should still be detected as duplicate
	offset7, isDup7, err := q2.EnqueueWithDedup(persistPayload, persistDedupID, 1*time.Hour)
	if err != nil {
		log.Fatalf("Failed to enqueue: %v", err)
	}
	fmt.Printf("After restart: offset=%d, duplicate=%v ✓\n", offset7, isDup7)

	if isDup7 && offset7 == offset6 {
		fmt.Println("  ✓ Dedup state survived restart!")
	}
	fmt.Println()

	// Final statistics
	fmt.Println("=== Final Statistics ===")
	finalStats := q2.Stats()
	fmt.Printf("Total messages: %d\n", finalStats.TotalMessages)
	fmt.Printf("Dedup entries: %d\n", finalStats.DedupTrackedEntries)
	fmt.Println("\n✓ Deduplication example completed successfully!")
}
