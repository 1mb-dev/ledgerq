// Package main demonstrates Dead Letter Queue (DLQ) usage in LedgerQ.
//
// This example shows:
//   - Configuring DLQ with max retries
//   - Using Ack() to acknowledge successful processing
//   - Using Nack() to report processing failures
//   - Automatic movement of messages to DLQ after max retries
//   - Inspecting DLQ messages
//   - Requeuing messages from DLQ back to main queue
package main

import (
	"fmt"
	"log"
	"os"

	"github.com/1mb-dev/ledgerq/pkg/ledgerq"
)

func main() {
	// Create temporary directories for the queue and DLQ
	queueDir := "/tmp/ledgerq-dlq-example"
	dlqDir := "/tmp/ledgerq-dlq-example-dlq"

	// Clean up from previous runs
	os.RemoveAll(queueDir)
	os.RemoveAll(dlqDir)
	if err := os.MkdirAll(queueDir, 0755); err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(queueDir)
	defer os.RemoveAll(dlqDir)

	fmt.Println("LedgerQ Dead Letter Queue (DLQ) Example")
	fmt.Println("========================================")

	// Configure queue with DLQ enabled
	opts := ledgerq.DefaultOptions(queueDir)
	opts.DLQPath = dlqDir // Enable DLQ
	opts.MaxRetries = 3   // Max 3 retry attempts before moving to DLQ

	// Open the queue with DLQ configuration
	q, err := ledgerq.Open(queueDir, opts)
	if err != nil {
		log.Fatalf("Failed to open queue: %v", err)
	}
	defer q.Close()

	fmt.Printf("\n✓ Queue opened with DLQ enabled\n")
	fmt.Printf("  Queue directory: %s\n", queueDir)
	fmt.Printf("  DLQ directory:   %s\n", dlqDir)
	fmt.Printf("  Max retries:     %d\n", opts.MaxRetries)

	// Enqueue some test messages
	fmt.Println("\n1. Enqueuing Messages")
	fmt.Println("   ------------------")

	messages := []struct {
		payload    string
		shouldFail bool
	}{
		{"Task: Process payment #1001", false},  // Will succeed
		{"Task: Send email notification", true}, // Will fail and go to DLQ
		{"Task: Update inventory", false},       // Will succeed
		{"Task: Generate report", true},         // Will fail and go to DLQ
	}

	for _, msg := range messages {
		_, err := q.Enqueue([]byte(msg.payload))
		if err != nil {
			log.Fatalf("Failed to enqueue: %v", err)
		}
		status := "normal"
		if msg.shouldFail {
			status = "will fail"
		}
		fmt.Printf("  ✓ Enqueued: %s (%s)\n", msg.payload, status)
	}

	// Process messages with simulated failures
	fmt.Println("\n2. Processing Messages")
	fmt.Println("   -------------------")

	for i := 0; i < len(messages); i++ {
		msg, err := q.Dequeue()
		if err != nil {
			log.Fatalf("Failed to dequeue: %v", err)
		}

		fmt.Printf("  Processing [ID:%d]: %s\n", msg.ID, string(msg.Payload))

		// Simulate processing logic
		shouldFail := messages[i].shouldFail

		if !shouldFail {
			// Success case - acknowledge the message
			if err := q.Ack(msg.ID); err != nil {
				log.Fatalf("Failed to ack: %v", err)
			}
			fmt.Printf("    → ✓ Success (Acked)\n")
		} else {
			// Failure case - nack the message multiple times
			fmt.Printf("    → Processing failed, will retry...\n")

			for retry := 1; retry <= opts.MaxRetries; retry++ {
				reason := fmt.Sprintf("Simulated failure (attempt %d/%d)", retry, opts.MaxRetries)
				if err := q.Nack(msg.ID, reason); err != nil {
					log.Fatalf("Failed to nack: %v", err)
				}

				if retry < opts.MaxRetries {
					fmt.Printf("    → Attempt %d failed, will retry...\n", retry)
				} else {
					fmt.Printf("    → Attempt %d failed, moved to DLQ!\n", retry)
				}
			}
		}
	}

	// Check DLQ contents
	fmt.Println("\n3. Inspecting Dead Letter Queue")
	fmt.Println("   -----------------------------")

	dlq := q.GetDLQ()
	if dlq == nil {
		log.Fatal("DLQ not configured")
	}

	dlqStats := dlq.Stats()
	fmt.Printf("  DLQ has %d message(s)\n\n", dlqStats.PendingMessages)

	if dlqStats.PendingMessages > 0 {
		fmt.Println("  Failed messages in DLQ:")

		// Peek at DLQ messages
		for i := 0; i < int(dlqStats.PendingMessages); i++ {
			dlqMsg, err := dlq.Dequeue()
			if err != nil {
				log.Fatalf("Failed to dequeue from DLQ: %v", err)
			}

			fmt.Printf("\n  Message %d:\n", i+1)
			fmt.Printf("    DLQ ID:           %d\n", dlqMsg.ID)
			fmt.Printf("    Payload:          %s\n", string(dlqMsg.Payload))
			fmt.Printf("    Original Msg ID:  %s\n", dlqMsg.Headers["dlq.original_msg_id"])
			fmt.Printf("    Retry Count:      %s\n", dlqMsg.Headers["dlq.retry_count"])
			fmt.Printf("    Failure Reason:   %s\n", dlqMsg.Headers["dlq.failure_reason"])
			fmt.Printf("    Last Failure:     %s\n", dlqMsg.Headers["dlq.last_failure"])
		}
	}

	// Demonstrate requeuing from DLQ
	fmt.Println("\n4. Requeuing from DLQ")
	fmt.Println("   ------------------")

	// Reset DLQ read position to read messages again
	if err := dlq.SeekToMessageID(1); err != nil {
		log.Fatalf("Failed to seek DLQ: %v", err)
	}

	dlqStats = dlq.Stats()
	if dlqStats.PendingMessages > 0 {
		// Get first message from DLQ
		dlqMsg, err := dlq.Dequeue()
		if err != nil {
			log.Fatalf("Failed to dequeue from DLQ: %v", err)
		}

		fmt.Printf("  Requeuing message: %s\n", string(dlqMsg.Payload))

		// Requeue back to main queue
		if err := q.RequeueFromDLQ(dlqMsg.ID); err != nil {
			log.Fatalf("Failed to requeue from DLQ: %v", err)
		}

		fmt.Printf("  ✓ Message requeued to main queue\n")

		// Process the requeued message (this time it succeeds)
		requeuedMsg, err := q.Dequeue()
		if err != nil {
			log.Fatalf("Failed to dequeue requeued message: %v", err)
		}

		fmt.Printf("  Processing requeued message [ID:%d]: %s\n",
			requeuedMsg.ID, string(requeuedMsg.Payload))

		// This time it succeeds
		if err := q.Ack(requeuedMsg.ID); err != nil {
			log.Fatalf("Failed to ack: %v", err)
		}
		fmt.Printf("  ✓ Requeued message processed successfully!\n")
	}

	// Final stats
	fmt.Println("\n5. Final Statistics")
	fmt.Println("   ----------------")

	mainStats := q.Stats()
	dlqStats = dlq.Stats()

	fmt.Printf("  Main Queue:\n")
	fmt.Printf("    Total messages:   %d\n", mainStats.TotalMessages)
	fmt.Printf("    Pending messages: %d\n", mainStats.PendingMessages)
	fmt.Printf("\n")
	fmt.Printf("  Dead Letter Queue:\n")
	fmt.Printf("    Total messages:   %d\n", dlqStats.TotalMessages)
	fmt.Printf("    Pending messages: %d\n", dlqStats.PendingMessages)

	fmt.Println("\n✓ Example completed successfully!")
	fmt.Println("\nKey Takeaways:")
	fmt.Println("  • Use Ack() after successful message processing")
	fmt.Println("  • Use Nack() to report processing failures")
	fmt.Println("  • Messages move to DLQ after MaxRetries failures")
	fmt.Println("  • DLQ messages preserve original payload and failure metadata")
	fmt.Println("  • Messages can be requeued from DLQ back to main queue")
}
