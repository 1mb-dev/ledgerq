// Package main demonstrates real-time streaming message processing with LedgerQ.
//
// This example shows:
// - Setting up a streaming consumer with context-based cancellation
// - Concurrent message production while streaming
// - Graceful shutdown using context
// - Real-time event-driven message processing
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/1mb-dev/ledgerq/pkg/ledgerq"
)

func main() {
	// Create a temporary directory for the queue
	queueDir := "/tmp/ledgerq-streaming-example"

	// Clean up any previous runs
	_ = os.RemoveAll(queueDir)

	// Open the queue
	q, err := ledgerq.Open(queueDir, nil)
	if err != nil {
		log.Fatalf("Failed to open queue: %v", err)
	}
	defer q.Close()

	fmt.Println("=== LedgerQ Streaming Example ===")
	fmt.Println("Press Ctrl+C to stop")
	fmt.Println()

	// Create context that responds to interrupt signals
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Setup signal handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		fmt.Println("\nReceived interrupt signal, shutting down...")
		cancel()
	}()

	var wg sync.WaitGroup

	// Start streaming consumer
	wg.Add(1)
	go func() {
		defer wg.Done()
		runConsumer(ctx, q)
	}()

	// Start producer
	wg.Add(1)
	go func() {
		defer wg.Done()
		runProducer(ctx, q)
	}()

	// Wait for both to complete
	wg.Wait()

	fmt.Println()
	fmt.Println("=== Streaming Example Complete ===")
}

// runConsumer streams messages from the queue in real-time
func runConsumer(ctx context.Context, q *ledgerq.Queue) {
	fmt.Println("Starting streaming consumer...")

	messageCount := 0

	handler := func(msg *ledgerq.Message) error {
		messageCount++
		fmt.Printf("[Consumer] Received message #%d (ID: %d): %s\n",
			messageCount, msg.ID, string(msg.Payload))

		// Simulate some processing time
		time.Sleep(100 * time.Millisecond)

		return nil
	}

	// Start streaming - this will block until context is canceled or error occurs
	err := q.Stream(ctx, handler)

	if err != nil && err != context.Canceled {
		log.Printf("Streaming error: %v", err)
	}

	fmt.Printf("Consumer stopped. Processed %d messages total.\n", messageCount)
}

// runProducer produces messages at regular intervals
func runProducer(ctx context.Context, q *ledgerq.Queue) {
	// Give consumer time to start
	time.Sleep(200 * time.Millisecond)

	fmt.Println("Starting message producer...")

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	messageNum := 0

	for {
		select {
		case <-ctx.Done():
			fmt.Println("Producer stopping...")
			return

		case <-ticker.C:
			messageNum++
			payload := fmt.Sprintf("Event %d at %s",
				messageNum, time.Now().Format("15:04:05"))

			offset, err := q.Enqueue([]byte(payload))
			if err != nil {
				log.Printf("Failed to enqueue: %v", err)
				continue
			}

			fmt.Printf("[Producer] Enqueued message #%d at offset %d\n",
				messageNum, offset)

			// Stop after 10 messages for demo purposes
			if messageNum >= 10 {
				fmt.Println("Producer finished sending messages.")
				// Give consumer time to process remaining messages
				time.Sleep(2 * time.Second)
				return
			}
		}
	}
}
