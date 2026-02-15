// Package main demonstrates replay capabilities of LedgerQ
package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/1mb-dev/ledgerq/pkg/ledgerq"
)

func main() {
	// Create queue directory
	queueDir := "/tmp/ledgerq-replay-example"
	if err := os.MkdirAll(queueDir, 0755); err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(queueDir)

	fmt.Println("LedgerQ Replay Example")
	fmt.Println("======================")

	// Open the queue
	q, err := ledgerq.Open(queueDir, nil)
	if err != nil {
		log.Fatalf("Failed to open queue: %v", err)
	}
	defer q.Close()

	fmt.Printf("\n✓ Queue opened at: %s\n", queueDir)

	// Enqueue messages with timestamps
	fmt.Println("\nEnqueuing messages with delays to capture different timestamps...")

	messages := []string{
		"Event 1: Application started",
		"Event 2: User logged in",
		"Event 3: User viewed dashboard",
		"Event 4: User clicked button",
		"Event 5: Data processed",
		"Event 6: Report generated",
		"Event 7: Email sent",
		"Event 8: User logged out",
		"Event 9: Cache cleared",
		"Event 10: Application stopped",
	}

	timestamps := make([]int64, len(messages))

	for i, msg := range messages {
		timestamps[i] = time.Now().UnixNano()
		offset, err := q.Enqueue([]byte(msg))
		if err != nil {
			log.Fatalf("Failed to enqueue: %v", err)
		}
		fmt.Printf("  [ID:%d, Offset:%d] %s\n", i+1, offset, msg)

		// Add delay every few messages to create timestamp groups
		if i%3 == 2 {
			time.Sleep(100 * time.Millisecond)
		}
	}

	// Demo 1: Seek to specific message ID
	fmt.Println("\n--- Demo 1: Seek to Message ID 5 ---")

	if err := q.SeekToMessageID(5); err != nil {
		log.Fatalf("SeekToMessageID failed: %v", err)
	}

	fmt.Println("Reading 3 messages from ID 5:")
	for i := 0; i < 3; i++ {
		msg, err := q.Dequeue()
		if err != nil {
			log.Printf("Dequeue error: %v", err)
			break
		}
		fmt.Printf("  [ID:%d] %s\n", msg.ID, string(msg.Payload))
	}

	// Demo 2: Seek to beginning
	fmt.Println("\n--- Demo 2: Seek Back to Beginning ---")

	if err := q.SeekToMessageID(1); err != nil {
		log.Fatalf("SeekToMessageID failed: %v", err)
	}

	fmt.Println("Reading first 3 messages:")
	for i := 0; i < 3; i++ {
		msg, err := q.Dequeue()
		if err != nil {
			log.Printf("Dequeue error: %v", err)
			break
		}
		fmt.Printf("  [ID:%d] %s\n", msg.ID, string(msg.Payload))
	}

	// Demo 3: Seek by timestamp
	fmt.Println("\n--- Demo 3: Seek by Timestamp ---")

	// Use timestamp of message 7 (index 6)
	targetTimestamp := timestamps[6]
	fmt.Printf("Seeking to timestamp: %d (around message 7)\n", targetTimestamp)

	if err := q.SeekToTimestamp(targetTimestamp); err != nil {
		log.Fatalf("SeekToTimestamp failed: %v", err)
	}

	fmt.Println("Reading 3 messages from that timestamp:")
	for i := 0; i < 3; i++ {
		msg, err := q.Dequeue()
		if err != nil {
			log.Printf("Dequeue error: %v", err)
			break
		}
		fmt.Printf("  [ID:%d] %s\n", msg.ID, string(msg.Payload))
	}

	// Demo 4: Replay entire log
	fmt.Println("\n--- Demo 4: Replay Entire Log ---")

	if err := q.SeekToMessageID(1); err != nil {
		log.Fatalf("SeekToMessageID failed: %v", err)
	}

	fmt.Println("Replaying all events:")
	count := 0
	for {
		msg, err := q.Dequeue()
		if err != nil {
			break
		}
		count++
		// Convert nanoseconds to time
		ts := time.Unix(0, msg.Timestamp)
		fmt.Printf("  [%s] [ID:%d] %s\n", ts.Format("15:04:05.000"), msg.ID, string(msg.Payload))
	}

	fmt.Printf("\n✓ Replayed %d events successfully!\n", count)

	// Show final stats
	stats := q.Stats()
	fmt.Printf("\nQueue Stats:\n")
	fmt.Printf("  Total messages: %d\n", stats.TotalMessages)
	fmt.Printf("  Read position:  %d\n", stats.ReadMessageID)
}
