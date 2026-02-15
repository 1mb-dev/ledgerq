package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/1mb-dev/ledgerq/pkg/ledgerq"
)

func main() {
	// Create a temporary directory for this example
	tmpDir, err := os.MkdirTemp("", "ledgerq-priority-example-")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	fmt.Printf("LedgerQ Priority Queue Example (v%s)\n", ledgerq.Version)
	fmt.Println("========================================")
	fmt.Println()

	// Example 1: Basic Priority Queue
	fmt.Println("Example 1: Basic Priority Ordering")
	fmt.Println("-----------------------------------")
	basicPriorityExample(tmpDir)

	// Example 2: Starvation Prevention
	fmt.Println("\nExample 2: Starvation Prevention")
	fmt.Println("--------------------------------")
	starvationPreventionExample(tmpDir)

	// Example 3: Priority with TTL
	fmt.Println("\nExample 3: Priority with TTL")
	fmt.Println("----------------------------")
	priorityWithTTLExample(tmpDir)

	// Example 4: Priority with Headers
	fmt.Println("\nExample 4: Priority with Headers")
	fmt.Println("--------------------------------")
	priorityWithHeadersExample(tmpDir)

	// Example 5: Realistic Workload
	fmt.Println("\nExample 5: Realistic Mixed Workload")
	fmt.Println("-----------------------------------")
	realisticWorkloadExample(tmpDir)

	// Example 6: Batch Operations with Priorities
	fmt.Println("\nExample 6: Batch Operations with Priorities (v1.1.0+)")
	fmt.Println("-----------------------------------------------------")
	batchWithPrioritiesExample(tmpDir)

	fmt.Println("\n✓ All examples completed successfully!")
}

// basicPriorityExample demonstrates basic priority ordering
func basicPriorityExample(baseDir string) {
	dir := baseDir + "/example1"
	os.MkdirAll(dir, 0755)
	defer os.RemoveAll(dir)

	// Open queue with priority mode enabled
	q, err := ledgerq.Open(dir, &ledgerq.Options{
		EnablePriorities: true,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer q.Close()

	// Enqueue messages with different priorities (in mixed order)
	messages := []struct {
		data     string
		priority ledgerq.Priority
	}{
		{"Low priority task 1", ledgerq.PriorityLow},
		{"High priority alert!", ledgerq.PriorityHigh},
		{"Medium priority update", ledgerq.PriorityMedium},
		{"Low priority task 2", ledgerq.PriorityLow},
		{"High priority critical!", ledgerq.PriorityHigh},
		{"Medium priority notification", ledgerq.PriorityMedium},
	}

	fmt.Println("Enqueuing messages in mixed order:")
	for _, msg := range messages {
		if _, err := q.EnqueueWithPriority([]byte(msg.data), msg.priority); err != nil {
			log.Fatal(err)
		}
		priorityName := getPriorityName(msg.priority)
		fmt.Printf("  [%s] %s\n", priorityName, msg.data)
	}

	fmt.Println("\nDequeuing messages (priority order: High → Medium → Low):")
	for i := 0; i < len(messages); i++ {
		msg, err := q.Dequeue()
		if err != nil {
			log.Fatal(err)
		}
		priorityName := getPriorityName(msg.Priority)
		fmt.Printf("  [%s] %s\n", priorityName, string(msg.Payload))
	}
}

// starvationPreventionExample demonstrates low-priority message promotion
func starvationPreventionExample(baseDir string) {
	dir := baseDir + "/example2"
	os.MkdirAll(dir, 0755)
	defer os.RemoveAll(dir)

	// Open queue with short starvation window for demonstration
	q, err := ledgerq.Open(dir, &ledgerq.Options{
		EnablePriorities:         true,
		PriorityStarvationWindow: 200 * time.Millisecond, // Short window for demo
	})
	if err != nil {
		log.Fatal(err)
	}
	defer q.Close()

	// Enqueue a low-priority message
	fmt.Println("Enqueuing low-priority message...")
	if _, err := q.EnqueueWithPriority([]byte("Old low-priority task"), ledgerq.PriorityLow); err != nil {
		log.Fatal(err)
	}

	// Wait for starvation window to pass
	fmt.Println("Waiting 250ms for starvation window to pass...")
	time.Sleep(250 * time.Millisecond)

	// Now enqueue high-priority messages
	fmt.Println("Enqueuing new high-priority messages...")
	if _, err := q.EnqueueWithPriority([]byte("New high-priority alert 1"), ledgerq.PriorityHigh); err != nil {
		log.Fatal(err)
	}
	if _, err := q.EnqueueWithPriority([]byte("New high-priority alert 2"), ledgerq.PriorityHigh); err != nil {
		log.Fatal(err)
	}

	fmt.Println("\nDequeuing messages:")
	fmt.Println("  (The old low-priority message should be promoted first)")
	for i := 0; i < 3; i++ {
		msg, err := q.Dequeue()
		if err != nil {
			log.Fatal(err)
		}
		priorityName := getPriorityName(msg.Priority)
		age := time.Since(time.Unix(0, msg.Timestamp))
		fmt.Printf("  [%s] %s (age: %v)\n", priorityName, string(msg.Payload), age.Round(time.Millisecond))
	}
}

// priorityWithTTLExample demonstrates priority combined with TTL
func priorityWithTTLExample(baseDir string) {
	dir := baseDir + "/example3"
	os.MkdirAll(dir, 0755)
	defer os.RemoveAll(dir)

	q, err := ledgerq.Open(dir, &ledgerq.Options{
		EnablePriorities: true,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer q.Close()

	// Enqueue high-priority message with short TTL
	fmt.Println("Enqueuing messages:")
	fmt.Println("  High-priority with 100ms TTL")
	if _, err := q.EnqueueWithAllOptions(
		[]byte("High-priority expiring message"),
		ledgerq.EnqueueOptions{
			Priority: ledgerq.PriorityHigh,
			TTL:      100 * time.Millisecond,
			Headers:  nil,
		},
	); err != nil {
		log.Fatal(err)
	}

	fmt.Println("  Low-priority with no TTL")
	if _, err := q.EnqueueWithPriority([]byte("Low-priority persistent message"), ledgerq.PriorityLow); err != nil {
		log.Fatal(err)
	}

	// Wait for high-priority message to expire
	fmt.Println("\nWaiting 150ms for high-priority message to expire...")
	time.Sleep(150 * time.Millisecond)

	fmt.Println("\nDequeuing messages:")
	msg, err := q.Dequeue()
	if err != nil {
		log.Fatal(err)
	}
	// Should get low-priority message since high-priority expired
	priorityName := getPriorityName(msg.Priority)
	fmt.Printf("  [%s] %s\n", priorityName, string(msg.Payload))
	fmt.Println("  (High-priority message was skipped due to TTL expiration)")
}

// priorityWithHeadersExample demonstrates priority with message headers
func priorityWithHeadersExample(baseDir string) {
	dir := baseDir + "/example4"
	os.MkdirAll(dir, 0755)
	defer os.RemoveAll(dir)

	q, err := ledgerq.Open(dir, &ledgerq.Options{
		EnablePriorities: true,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer q.Close()

	// Enqueue messages with headers and priorities
	messages := []struct {
		data     string
		priority ledgerq.Priority
		headers  map[string]string
	}{
		{
			data:     "API error occurred",
			priority: ledgerq.PriorityHigh,
			headers: map[string]string{
				"source":   "api-server",
				"severity": "error",
				"trace-id": "abc-123",
			},
		},
		{
			data:     "User logged in",
			priority: ledgerq.PriorityLow,
			headers: map[string]string{
				"source":  "auth-service",
				"user-id": "user-456",
			},
		},
		{
			data:     "Database connection warning",
			priority: ledgerq.PriorityMedium,
			headers: map[string]string{
				"source":   "db-pool",
				"severity": "warning",
			},
		},
	}

	fmt.Println("Enqueuing messages with headers:")
	for _, msg := range messages {
		if _, err := q.EnqueueWithAllOptions(
			[]byte(msg.data),
			ledgerq.EnqueueOptions{
				Priority: msg.priority,
				TTL:      0, // No TTL
				Headers:  msg.headers,
			},
		); err != nil {
			log.Fatal(err)
		}
		priorityName := getPriorityName(msg.priority)
		fmt.Printf("  [%s] %s (source: %s)\n", priorityName, msg.data, msg.headers["source"])
	}

	fmt.Println("\nDequeuing messages in priority order:")
	for i := 0; i < len(messages); i++ {
		msg, err := q.Dequeue()
		if err != nil {
			log.Fatal(err)
		}
		priorityName := getPriorityName(msg.Priority)
		fmt.Printf("  [%s] %s\n", priorityName, string(msg.Payload))
		fmt.Printf("    Headers: source=%s", msg.Headers["source"])
		if severity, ok := msg.Headers["severity"]; ok {
			fmt.Printf(", severity=%s", severity)
		}
		if traceID, ok := msg.Headers["trace-id"]; ok {
			fmt.Printf(", trace-id=%s", traceID)
		}
		fmt.Println()
	}
}

// realisticWorkloadExample demonstrates a realistic mixed workload
func realisticWorkloadExample(baseDir string) {
	dir := baseDir + "/example5"
	os.MkdirAll(dir, 0755)
	defer os.RemoveAll(dir)

	q, err := ledgerq.Open(dir, &ledgerq.Options{
		EnablePriorities: true,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer q.Close()

	// Realistic workload: 10% high, 30% medium, 60% low
	fmt.Println("Enqueuing 20 messages with realistic distribution:")
	fmt.Println("  (10% high priority, 30% medium, 60% low)")

	highCount := 0
	mediumCount := 0
	lowCount := 0

	for i := 0; i < 20; i++ {
		var priority ledgerq.Priority
		var taskType string

		// Realistic distribution
		r := i % 10
		if r == 0 {
			priority = ledgerq.PriorityHigh
			taskType = "Critical Alert"
			highCount++
		} else if r <= 3 {
			priority = ledgerq.PriorityMedium
			taskType = "Important Update"
			mediumCount++
		} else {
			priority = ledgerq.PriorityLow
			taskType = "Background Task"
			lowCount++
		}

		data := fmt.Sprintf("%s #%d", taskType, i+1)
		if _, err := q.EnqueueWithPriority([]byte(data), priority); err != nil {
			log.Fatal(err)
		}
	}

	fmt.Printf("  Enqueued: %d high, %d medium, %d low\n", highCount, mediumCount, lowCount)

	// Dequeue and show order
	fmt.Println("\nDequeue order (high priority first):")

	highDequeued := 0
	mediumDequeued := 0
	lowDequeued := 0

	for i := 0; i < 20; i++ {
		msg, err := q.Dequeue()
		if err != nil {
			log.Fatal(err)
		}

		switch msg.Priority {
		case ledgerq.PriorityHigh:
			highDequeued++
		case ledgerq.PriorityMedium:
			mediumDequeued++
		case ledgerq.PriorityLow:
			lowDequeued++
		}

		priorityName := getPriorityName(msg.Priority)
		fmt.Printf("  %2d. [%s] %s\n", i+1, priorityName, string(msg.Payload))
	}

	fmt.Printf("\nDequeued: %d high, %d medium, %d low\n", highDequeued, mediumDequeued, lowDequeued)
	fmt.Println("  (Note: All high-priority messages processed first, then medium, then low)")
}

// batchWithPrioritiesExample demonstrates batch enqueue with per-message priorities
func batchWithPrioritiesExample(baseDir string) {
	dir := baseDir + "/example6"
	os.MkdirAll(dir, 0755)
	defer os.RemoveAll(dir)

	q, err := ledgerq.Open(dir, &ledgerq.Options{
		EnablePriorities: true,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer q.Close()

	// Enqueue a batch with different priorities, TTLs, and headers
	fmt.Println("Enqueuing batch with per-message options:")
	messages := []ledgerq.BatchEnqueueOptions{
		{
			Payload:  []byte("Critical alert - database down!"),
			Priority: ledgerq.PriorityHigh,
			Headers: map[string]string{
				"source":   "db-monitor",
				"severity": "critical",
			},
		},
		{
			Payload:  []byte("User activity report"),
			Priority: ledgerq.PriorityLow,
			Headers: map[string]string{
				"source": "analytics",
				"type":   "report",
			},
		},
		{
			Payload:  []byte("API rate limit warning"),
			Priority: ledgerq.PriorityMedium,
			TTL:      5 * time.Second, // Expires in 5 seconds
			Headers: map[string]string{
				"source":   "api-gateway",
				"severity": "warning",
			},
		},
		{
			Payload:  []byte("Security scan completed"),
			Priority: ledgerq.PriorityHigh,
			Headers: map[string]string{
				"source": "security-scanner",
				"type":   "notification",
			},
		},
		{
			Payload:  []byte("Background sync job"),
			Priority: ledgerq.PriorityLow,
			Headers: map[string]string{
				"source": "sync-worker",
				"type":   "job",
			},
		},
	}

	offsets, err := q.EnqueueBatchWithOptions(messages)
	if err != nil {
		log.Fatal(err)
	}

	for i, msg := range messages {
		priorityName := getPriorityName(msg.Priority)
		fmt.Printf("  [%s] %s (offset: %d)\n", priorityName, string(msg.Payload), offsets[i])
	}

	fmt.Println("\nDequeuing messages in priority order:")
	for i := 0; i < len(messages); i++ {
		msg, err := q.Dequeue()
		if err != nil {
			log.Fatal(err)
		}
		priorityName := getPriorityName(msg.Priority)
		fmt.Printf("  %d. [%s] %s\n", i+1, priorityName, string(msg.Payload))
		if source, ok := msg.Headers["source"]; ok {
			fmt.Printf("     Source: %s", source)
			if severity, ok := msg.Headers["severity"]; ok {
				fmt.Printf(", Severity: %s", severity)
			}
			fmt.Println()
		}
	}

	fmt.Println("\nBenefit: Single fsync for entire batch, but with per-message priority control!")
}

// Helper function to get priority name
func getPriorityName(priority ledgerq.Priority) string {
	switch priority {
	case ledgerq.PriorityHigh:
		return "HIGH  "
	case ledgerq.PriorityMedium:
		return "MEDIUM"
	case ledgerq.PriorityLow:
		return "LOW   "
	default:
		return "UNKNOWN"
	}
}
