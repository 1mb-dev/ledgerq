package main

import (
	"fmt"
	"log"
	"time"

	"github.com/1mb-dev/ledgerq/pkg/ledgerq"
)

func main() {
	// Create a temporary directory for the queue
	tmpDir := "/tmp/ledgerq-ttl-example"

	// Open the queue
	q, err := ledgerq.Open(tmpDir, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer q.Close()

	fmt.Println("=== LedgerQ TTL (Time-To-Live) Example ===")
	fmt.Println()

	// Example 1: Enqueue messages with different TTLs
	fmt.Println("1. Enqueuing messages with TTL:")

	// Message with 2 second TTL
	offset1, err := q.EnqueueWithTTL([]byte("Short-lived message (2s TTL)"), 2*time.Second)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("   Enqueued message with 2s TTL at offset %d\n", offset1)

	// Message with 5 second TTL
	offset2, err := q.EnqueueWithTTL([]byte("Medium-lived message (5s TTL)"), 5*time.Second)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("   Enqueued message with 5s TTL at offset %d\n", offset2)

	// Regular message without TTL
	offset3, err := q.Enqueue([]byte("Permanent message (no TTL)"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("   Enqueued permanent message at offset %d\n\n", offset3)

	// Example 2: Dequeue immediately (all messages should be available)
	fmt.Println("2. Immediate dequeue (before expiration):")

	msg1, err := q.Dequeue()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("   Dequeued: %s\n", msg1.Payload)
	if msg1.ExpiresAt > 0 {
		expiresIn := time.Until(time.Unix(0, msg1.ExpiresAt))
		fmt.Printf("   Expires in: %v\n", expiresIn.Round(time.Second))
	}

	msg2, err := q.Dequeue()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("   Dequeued: %s\n", msg2.Payload)
	if msg2.ExpiresAt > 0 {
		expiresIn := time.Until(time.Unix(0, msg2.ExpiresAt))
		fmt.Printf("   Expires in: %v\n", expiresIn.Round(time.Second))
	}

	msg3, err := q.Dequeue()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("   Dequeued: %s\n", msg3.Payload)
	if msg3.ExpiresAt == 0 {
		fmt.Println("   No expiration (permanent message)")
	}

	fmt.Println()

	// Example 3: Enqueue more messages and wait for some to expire
	fmt.Println("3. Message expiration demonstration:")

	// Reset read position to start
	if err := q.SeekToMessageID(1); err != nil {
		log.Fatal(err)
	}

	// Enqueue messages with short TTL
	q.EnqueueWithTTL([]byte("Expires in 1 second"), 1*time.Second)
	q.EnqueueWithTTL([]byte("Expires in 500ms"), 500*time.Millisecond)
	q.Enqueue([]byte("This message stays"))
	q.EnqueueWithTTL([]byte("Expires in 200ms"), 200*time.Millisecond)
	q.Enqueue([]byte("This message also stays"))

	fmt.Println("   Enqueued 5 messages (3 with TTL, 2 permanent)")
	fmt.Println("   Waiting 2 seconds for TTL messages to expire...")
	time.Sleep(2 * time.Second)

	// Try to dequeue - should skip expired messages
	fmt.Println("\n   Dequeueing after expiration:")
	for i := 0; i < 5; i++ {
		msg, err := q.Dequeue()
		if err != nil {
			fmt.Printf("   No more messages: %v\n", err)
			break
		}
		fmt.Printf("   Message %d: %s\n", i+1, msg.Payload)
	}

	// Example 4: Batch operations with TTL
	fmt.Println("\n4. Batch operations with TTL:")

	// Reset queue
	if err := q.Close(); err != nil {
		log.Fatal(err)
	}
	q, err = ledgerq.Open(tmpDir, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer q.Close()

	// Enqueue batch with mixed TTL
	payloads := [][]byte{
		[]byte("Batch message 1"),
		[]byte("Batch message 2"),
		[]byte("Batch message 3"),
	}
	offsets, err := q.EnqueueBatch(payloads)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("   Enqueued %d messages in batch\n", len(offsets))

	// Add some TTL messages
	q.EnqueueWithTTL([]byte("Short-lived batch message"), 100*time.Millisecond)

	// Wait for TTL message to expire
	time.Sleep(200 * time.Millisecond)

	// Batch dequeue
	messages, err := q.DequeueBatch(10)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("   Dequeued %d messages in batch (expired messages skipped)\n", len(messages))
	for i, msg := range messages {
		fmt.Printf("   %d: %s\n", i+1, msg.Payload)
	}

	// Example 5: Check queue stats
	fmt.Println("\n5. Queue statistics:")
	stats := q.Stats()
	fmt.Printf("   Total messages: %d\n", stats.TotalMessages)
	fmt.Printf("   Pending messages: %d\n", stats.PendingMessages)
	fmt.Printf("   Next message ID: %d\n", stats.NextMessageID)
	fmt.Printf("   Read message ID: %d\n", stats.ReadMessageID)

	fmt.Println()
	fmt.Println("=== TTL Example Complete ===")
}
