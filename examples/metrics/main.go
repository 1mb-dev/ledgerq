// Package main demonstrates using LedgerQ with metrics collection.
//
// This example shows how to:
// - Create a metrics collector
// - Configure a queue with metrics enabled
// - Perform operations that generate metrics
// - Retrieve and display metrics snapshots
package main

import (
	"fmt"
	"log"
	"time"

	"github.com/1mb-dev/ledgerq/pkg/ledgerq"
)

func main() {
	// Create a temporary directory for the queue
	queueDir := "/tmp/ledgerq-metrics-example"

	// Create a metrics collector
	collector := ledgerq.NewMetricsCollector("example_queue")

	// Configure queue options with metrics
	opts := ledgerq.DefaultOptions(queueDir)
	opts.MetricsCollector = collector

	// Open the queue
	q, err := ledgerq.Open(queueDir, opts)
	if err != nil {
		log.Fatalf("Failed to open queue: %v", err)
	}
	defer q.Close()

	fmt.Println("=== LedgerQ Metrics Example ===")
	fmt.Println()

	// Perform various operations
	fmt.Println("Enqueueing 10 individual messages...")
	for i := 0; i < 10; i++ {
		payload := fmt.Sprintf("Message %d", i+1)
		_, err := q.Enqueue([]byte(payload))
		if err != nil {
			log.Fatalf("Enqueue failed: %v", err)
		}
	}

	fmt.Println("Enqueueing a batch of 5 messages...")
	batchPayloads := [][]byte{
		[]byte("Batch message 1"),
		[]byte("Batch message 2"),
		[]byte("Batch message 3"),
		[]byte("Batch message 4"),
		[]byte("Batch message 5"),
	}
	_, err = q.EnqueueBatch(batchPayloads)
	if err != nil {
		log.Fatalf("EnqueueBatch failed: %v", err)
	}

	fmt.Println("Dequeuing 5 messages...")
	for i := 0; i < 5; i++ {
		_, err := q.Dequeue()
		if err != nil {
			log.Fatalf("Dequeue failed: %v", err)
		}
	}

	fmt.Println("Dequeuing a batch of 3 messages...")
	_, err = q.DequeueBatch(3)
	if err != nil {
		log.Fatalf("DequeueBatch failed: %v", err)
	}

	fmt.Println("Seeking to message ID 10...")
	err = q.SeekToMessageID(10)
	if err != nil {
		log.Fatalf("Seek failed: %v", err)
	}

	// Small delay to ensure metrics are updated
	time.Sleep(10 * time.Millisecond)

	// Get metrics snapshot
	snapshot := ledgerq.GetMetricsSnapshot(collector)

	// Display metrics
	fmt.Println()
	fmt.Println("=== Metrics Snapshot ===")
	fmt.Println()

	fmt.Printf("Queue Name: %s\n\n", snapshot.QueueName)

	fmt.Println("Operation Counters:")
	fmt.Printf("  Enqueue Total:       %d\n", snapshot.EnqueueTotal)
	fmt.Printf("  Enqueue Batch Count: %d\n", snapshot.EnqueueBatch)
	fmt.Printf("  Dequeue Total:       %d\n", snapshot.DequeueTotal)
	fmt.Printf("  Dequeue Batch Count: %d\n", snapshot.DequeueBatch)
	fmt.Printf("  Seek Operations:     %d\n", snapshot.SeekOperations)
	fmt.Println()

	fmt.Println("Error Counters:")
	fmt.Printf("  Enqueue Errors: %d\n", snapshot.EnqueueErrors)
	fmt.Printf("  Dequeue Errors: %d\n", snapshot.DequeueErrors)
	fmt.Println()

	fmt.Println("Payload Metrics:")
	fmt.Printf("  Total Bytes Enqueued: %d\n", snapshot.EnqueueBytes)
	fmt.Printf("  Total Bytes Dequeued: %d\n", snapshot.DequeueBytes)
	fmt.Println()

	fmt.Println("Duration Percentiles:")
	fmt.Printf("  Enqueue P50: %v\n", snapshot.EnqueueDurationP50)
	fmt.Printf("  Enqueue P95: %v\n", snapshot.EnqueueDurationP95)
	fmt.Printf("  Enqueue P99: %v\n", snapshot.EnqueueDurationP99)
	fmt.Printf("  Dequeue P50: %v\n", snapshot.DequeueDurationP50)
	fmt.Printf("  Dequeue P95: %v\n", snapshot.DequeueDurationP95)
	fmt.Printf("  Dequeue P99: %v\n", snapshot.DequeueDurationP99)
	fmt.Println()

	fmt.Println("Queue State:")
	fmt.Printf("  Pending Messages: %d\n", snapshot.PendingMessages)
	fmt.Printf("  Segment Count:    %d\n", snapshot.SegmentCount)
	fmt.Printf("  Next Message ID:  %d\n", snapshot.NextMessageID)
	fmt.Printf("  Read Message ID:  %d\n", snapshot.ReadMessageID)
	fmt.Println()

	fmt.Println("Compaction Metrics:")
	fmt.Printf("  Compactions Total: %d\n", snapshot.CompactionsTotal)
	fmt.Printf("  Segments Removed:  %d\n", snapshot.SegmentsRemoved)
	fmt.Printf("  Bytes Freed:       %d\n", snapshot.BytesFreed)
	fmt.Printf("  Compaction Errors: %d\n", snapshot.CompactionErrors)
	if snapshot.LastCompactionUnixSec > 0 {
		lastCompaction := time.Unix(snapshot.LastCompactionUnixSec, 0)
		fmt.Printf("  Last Compaction:   %v\n", lastCompaction)
	}

	fmt.Println()
	fmt.Println("=== Example Complete ===")
}
