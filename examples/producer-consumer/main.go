// Package main demonstrates producer-consumer pattern with LedgerQ
package main

import (
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/1mb-dev/ledgerq/internal/queue"
)

func main() {
	// Create queue directory
	queueDir := "/tmp/ledgerq-producer-consumer"
	if err := os.MkdirAll(queueDir, 0755); err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(queueDir)

	fmt.Println("LedgerQ Producer-Consumer Example")
	fmt.Println("==================================")

	// Open the queue
	opts := queue.DefaultOptions(queueDir)
	opts.AutoSync = true // Ensure durability

	q, err := queue.Open(queueDir, opts)
	if err != nil {
		log.Fatalf("Failed to open queue: %v", err)
	}
	defer q.Close()

	fmt.Printf("\n✓ Queue opened at: %s\n", queueDir)

	var wg sync.WaitGroup

	// Start 3 producers
	numProducers := 3
	messagesPerProducer := 10

	fmt.Printf("\nStarting %d producers (each will produce %d messages)...\n", numProducers, messagesPerProducer)
	wg.Add(numProducers)

	for p := 0; p < numProducers; p++ {
		producerID := p
		go func() {
			defer wg.Done()
			producer(q, producerID, messagesPerProducer)
		}()
	}

	// Give producers a head start
	time.Sleep(100 * time.Millisecond)

	// Start 2 consumers
	numConsumers := 2
	expectedTotal := numProducers * messagesPerProducer

	fmt.Printf("\nStarting %d consumers...\n", numConsumers)
	consumed := make(chan int, numConsumers)
	wg.Add(numConsumers)

	for c := 0; c < numConsumers; c++ {
		consumerID := c
		go func() {
			defer wg.Done()
			count := consumer(q, consumerID, expectedTotal)
			consumed <- count
		}()
	}

	// Wait for all goroutines to complete
	wg.Wait()
	close(consumed)

	// Count total consumed
	totalConsumed := 0
	for count := range consumed {
		totalConsumed += count
	}

	fmt.Printf("\n=== Results ===\n")
	fmt.Printf("Expected:  %d messages\n", expectedTotal)
	fmt.Printf("Consumed:  %d messages\n", totalConsumed)

	if totalConsumed == expectedTotal {
		fmt.Println("\n✓ All messages successfully processed!")
	} else {
		fmt.Printf("\n⚠ Message count mismatch!\n")
	}

	// Final stats
	stats := q.Stats()
	fmt.Printf("\nFinal Queue Stats:\n")
	fmt.Printf("  Total messages:   %d\n", stats.TotalMessages)
	fmt.Printf("  Pending messages: %d\n", stats.PendingMessages)
}

func producer(q *queue.Queue, id int, count int) {
	fmt.Printf("  [Producer %d] Starting...\n", id)

	for i := 0; i < count; i++ {
		message := fmt.Sprintf("Message from Producer %d: #%d", id, i)
		if _, err := q.Enqueue([]byte(message)); err != nil {
			log.Printf("  [Producer %d] Error: %v\n", id, err)
			return
		}
		time.Sleep(10 * time.Millisecond) // Simulate work
	}

	fmt.Printf("  [Producer %d] Completed %d messages\n", id, count)
}

func consumer(q *queue.Queue, id int, expectedTotal int) int {
	fmt.Printf("  [Consumer %d] Starting...\n", id)

	count := 0
	for {
		msg, err := q.Dequeue()
		if err != nil {
			// No more messages available, wait a bit
			time.Sleep(50 * time.Millisecond)

			// Check if we've consumed enough collectively
			stats := q.Stats()
			if stats.PendingMessages == 0 && count > 0 {
				break
			}
			continue
		}

		count++
		fmt.Printf("  [Consumer %d] Received [ID:%d]: %s\n", id, msg.ID, string(msg.Payload))
	}

	fmt.Printf("  [Consumer %d] Completed. Consumed %d messages\n", id, count)
	return count
}
