// Package main demonstrates basic LedgerQ usage
package main

import (
	"fmt"
	"log"
	"os"

	"github.com/1mb-dev/ledgerq/pkg/ledgerq"
)

func main() {
	// Create a temporary directory for the queue
	queueDir := "/tmp/ledgerq-simple-example"
	if err := os.MkdirAll(queueDir, 0755); err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(queueDir)

	fmt.Println("LedgerQ Simple Example")
	fmt.Println("======================")

	// Open the queue
	q, err := ledgerq.Open(queueDir, nil)
	if err != nil {
		log.Fatalf("Failed to open queue: %v", err)
	}
	defer q.Close()

	fmt.Printf("\n✓ Queue opened at: %s\n", queueDir)

	// Enqueue some messages
	messages := []string{
		"Hello, World!",
		"This is LedgerQ",
		"A persistent message queue",
		"Written in Go",
	}

	fmt.Println("\nEnqueuing messages:")
	for i, msg := range messages {
		offset, err := q.Enqueue([]byte(msg))
		if err != nil {
			log.Fatalf("Failed to enqueue message: %v", err)
		}
		fmt.Printf("  %d. Enqueued at offset %d: %s\n", i+1, offset, msg)
	}

	// Check queue stats
	stats := q.Stats()
	fmt.Printf("\nQueue Stats:\n")
	fmt.Printf("  Total messages:   %d\n", stats.TotalMessages)
	fmt.Printf("  Pending messages: %d\n", stats.PendingMessages)
	fmt.Printf("  Segments:         %d\n", stats.SegmentCount)

	// Dequeue messages
	fmt.Println("\nDequeuing messages:")
	for i := 0; i < len(messages); i++ {
		msg, err := q.Dequeue()
		if err != nil {
			log.Fatalf("Failed to dequeue message: %v", err)
		}
		fmt.Printf("  %d. [ID:%d] %s\n", i+1, msg.ID, string(msg.Payload))
	}

	// Check stats after dequeue
	stats = q.Stats()
	fmt.Printf("\nFinal Stats:\n")
	fmt.Printf("  Total messages:   %d\n", stats.TotalMessages)
	fmt.Printf("  Pending messages: %d\n", stats.PendingMessages)

	fmt.Println("\n✓ Example completed successfully!")
}
