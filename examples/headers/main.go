package main

import (
	"fmt"
	"log"
	"time"

	"github.com/1mb-dev/ledgerq/pkg/ledgerq"
)

func main() {
	// Create a temporary directory for the queue
	tmpDir := "/tmp/ledgerq-headers-example"

	// Open the queue
	q, err := ledgerq.Open(tmpDir, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer q.Close()

	fmt.Println("=== LedgerQ Headers (Metadata) Example ===")
	fmt.Println()

	// Example 1: Basic headers for routing
	fmt.Println("1. Message routing with headers:")

	routingHeaders := map[string]string{
		"content-type": "application/json",
		"destination":  "service-a",
		"priority":     "high",
	}
	offset1, err := q.EnqueueWithHeaders([]byte(`{"action": "process", "data": 123}`), routingHeaders)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("   Enqueued JSON message with routing headers at offset %d\n", offset1)

	msg1, err := q.Dequeue()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("   Dequeued message: %s\n", msg1.Payload)
	fmt.Printf("   Headers:\n")
	for k, v := range msg1.Headers {
		fmt.Printf("     %s: %s\n", k, v)
	}
	fmt.Println()

	// Example 2: Distributed tracing with correlation ID
	fmt.Println("2. Distributed tracing:")

	tracingHeaders := map[string]string{
		"trace-id":       "550e8400-e29b-41d4-a716-446655440000",
		"span-id":        "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
		"parent-span-id": "6ba7b811-9dad-11d1-80b4-00c04fd430c8",
		"service":        "payment-service",
	}
	offset2, err := q.EnqueueWithHeaders([]byte("payment processed"), tracingHeaders)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("   Enqueued traced event at offset %d\n", offset2)

	msg2, err := q.Dequeue()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("   Event: %s\n", msg2.Payload)
	fmt.Printf("   Trace ID: %s\n", msg2.Headers["trace-id"])
	fmt.Printf("   Span ID: %s\n", msg2.Headers["span-id"])
	fmt.Println()

	// Example 3: Event sourcing with metadata
	fmt.Println("3. Event sourcing metadata:")

	eventHeaders := map[string]string{
		"event-type":     "UserCreated",
		"aggregate-id":   "user-123",
		"aggregate-type": "User",
		"version":        "1",
		"causation-id":   "cmd-456",
	}
	offset3, err := q.EnqueueWithHeaders(
		[]byte(`{"userId": "user-123", "email": "user@example.com"}`),
		eventHeaders,
	)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("   Enqueued domain event at offset %d\n", offset3)

	msg3, err := q.Dequeue()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("   Event Type: %s\n", msg3.Headers["event-type"])
	fmt.Printf("   Aggregate: %s/%s\n", msg3.Headers["aggregate-type"], msg3.Headers["aggregate-id"])
	fmt.Printf("   Payload: %s\n", msg3.Payload)
	fmt.Println()

	// Example 4: Combining TTL and headers
	fmt.Println("4. Message with both TTL and headers:")

	tempHeaders := map[string]string{
		"message-type": "temporary-notification",
		"channel":      "email",
		"priority":     "low",
	}
	offset4, err := q.EnqueueWithOptions(
		[]byte("This notification expires in 10 seconds"),
		10*time.Second,
		tempHeaders,
	)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("   Enqueued temporary notification at offset %d\n", offset4)

	msg4, err := q.Dequeue()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("   Message: %s\n", msg4.Payload)
	fmt.Printf("   Channel: %s\n", msg4.Headers["channel"])
	expiresIn := time.Until(time.Unix(0, msg4.ExpiresAt))
	fmt.Printf("   Expires in: %v\n", expiresIn.Round(time.Second))
	fmt.Println()

	// Example 5: Workflow orchestration
	fmt.Println("5. Workflow orchestration:")

	workflows := []struct {
		step    string
		action  string
		headers map[string]string
	}{
		{
			step:   "1",
			action: "validate-order",
			headers: map[string]string{
				"workflow-id": "wf-789",
				"step":        "1",
				"step-name":   "validate",
				"next-step":   "2",
				"retry-count": "0",
				"max-retries": "3",
			},
		},
		{
			step:   "2",
			action: "charge-payment",
			headers: map[string]string{
				"workflow-id": "wf-789",
				"step":        "2",
				"step-name":   "payment",
				"next-step":   "3",
				"retry-count": "0",
				"max-retries": "5",
			},
		},
		{
			step:   "3",
			action: "ship-order",
			headers: map[string]string{
				"workflow-id": "wf-789",
				"step":        "3",
				"step-name":   "shipping",
				"next-step":   "done",
				"retry-count": "0",
				"max-retries": "3",
			},
		},
	}

	fmt.Println("   Enqueueing workflow steps:")
	for _, wf := range workflows {
		_, err := q.EnqueueWithHeaders([]byte(wf.action), wf.headers)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("     Step %s: %s\n", wf.step, wf.headers["step-name"])
	}

	fmt.Println()
	fmt.Println("   Processing workflow:")
	for i := 0; i < len(workflows); i++ {
		msg, err := q.Dequeue()
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("     Executing step %s: %s\n", msg.Headers["step"], msg.Payload)
		fmt.Printf("       Next: %s, Retries: %s/%s\n",
			msg.Headers["next-step"],
			msg.Headers["retry-count"],
			msg.Headers["max-retries"],
		)
	}
	fmt.Println()

	// Example 6: Message filtering based on headers
	fmt.Println("6. Message filtering:")

	// Enqueue messages with different types
	messages := []struct {
		payload string
		msgType string
		region  string
	}{
		{"User logged in", "audit", "us-east"},
		{"Payment received", "billing", "eu-west"},
		{"User logged out", "audit", "us-east"},
		{"Invoice generated", "billing", "us-east"},
		{"Password changed", "security", "eu-west"},
	}

	fmt.Println("   Enqueueing messages with type and region headers:")
	for _, m := range messages {
		headers := map[string]string{
			"type":   m.msgType,
			"region": m.region,
		}
		_, err := q.EnqueueWithHeaders([]byte(m.payload), headers)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("     [%s/%s] %s\n", m.msgType, m.region, m.payload)
	}

	fmt.Println()
	fmt.Println("   Filtering messages (only 'audit' type):")
	for i := 0; i < len(messages); i++ {
		msg, err := q.Dequeue()
		if err != nil {
			break
		}

		// Simulate filtering logic
		if msg.Headers["type"] == "audit" {
			fmt.Printf("     ✓ Matched: %s [region: %s]\n", msg.Payload, msg.Headers["region"])
		} else {
			fmt.Printf("     ✗ Skipped: %s [type: %s]\n", msg.Payload, msg.Headers["type"])
		}
	}
	fmt.Println()

	// Example 7: Message versioning
	fmt.Println("7. Message schema versioning:")

	// V1 message
	v1Headers := map[string]string{
		"schema-version": "1.0",
		"schema-type":    "UserEvent",
	}
	_, err = q.EnqueueWithHeaders([]byte(`{"user": "john"}`), v1Headers)
	if err != nil {
		log.Fatal(err)
	}

	// V2 message (with additional fields)
	v2Headers := map[string]string{
		"schema-version": "2.0",
		"schema-type":    "UserEvent",
	}
	_, err = q.EnqueueWithHeaders([]byte(`{"userId": "john", "timestamp": 1234567890}`), v2Headers)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("   Processing messages with version-aware handling:")
	for i := 0; i < 2; i++ {
		msg, err := q.Dequeue()
		if err != nil {
			break
		}

		version := msg.Headers["schema-version"]
		fmt.Printf("     Schema v%s: %s\n", version, msg.Payload)

		// Version-specific handling
		switch version {
		case "1.0":
			fmt.Println("       Using v1 parser")
		case "2.0":
			fmt.Println("       Using v2 parser with extended fields")
		}
	}
	fmt.Println()

	// Example 8: Queue statistics
	fmt.Println("8. Queue statistics:")
	stats := q.Stats()
	fmt.Printf("   Total messages processed: %d\n", stats.TotalMessages)
	fmt.Printf("   Pending messages: %d\n", stats.PendingMessages)
	fmt.Printf("   Segments: %d\n", stats.SegmentCount)

	fmt.Println()
	fmt.Println("=== Headers Example Complete ===")
	fmt.Println()
	fmt.Println("Key Takeaways:")
	fmt.Println("  • Headers enable message routing and classification")
	fmt.Println("  • Use headers for distributed tracing (trace-id, span-id)")
	fmt.Println("  • Headers support workflow orchestration")
	fmt.Println("  • Combine headers with TTL for temporary classified messages")
	fmt.Println("  • Headers persist across queue restarts")
	fmt.Println("  • Zero overhead for messages without headers")
}
