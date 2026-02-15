// Package main demonstrates LedgerQ compression features.
//
// This example shows how to use message payload compression to reduce
// disk usage and I/O overhead for large messages.
package main

import (
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/1mb-dev/ledgerq/pkg/ledgerq"
)

func main() {
	// Clean up queue directory
	queueDir := "/tmp/ledgerq-compression-example"
	_ = os.RemoveAll(queueDir)
	defer os.RemoveAll(queueDir)

	// Open queue with compression enabled by default
	opts := &ledgerq.Options{
		DefaultCompression: ledgerq.CompressionGzip, // Enable gzip compression
		CompressionLevel:   6,                       // Balanced compression (default)
		MinCompressionSize: 512,                     // Only compress messages >= 512 bytes
	}

	q, err := ledgerq.Open(queueDir, opts)
	if err != nil {
		log.Fatal(err)
	}
	defer q.Close()

	fmt.Println("=== LedgerQ Compression Example ===")

	// Example 1: Small message (won't be compressed due to MinCompressionSize)
	smallMsg := []byte("Hello, World!")
	offset1, err := q.Enqueue(smallMsg)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("1. Enqueued small message (13 bytes) at offset %d\n", offset1)
	fmt.Println("   → Not compressed (below 512 byte threshold)")

	// Example 2: Large message (will be compressed automatically)
	largeMsg := []byte(strings.Repeat("This is a test message that will be compressed. ", 50))
	offset2, err := q.Enqueue(largeMsg)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("2. Enqueued large message (%d bytes) at offset %d\n", len(largeMsg), offset2)
	fmt.Println("   → Automatically compressed using queue's DefaultCompression")

	// Example 3: Explicit compression override (force compression)
	mediumMsg := []byte("Force compression on this medium-sized message")
	offset3, err := q.EnqueueWithCompression(mediumMsg, ledgerq.CompressionGzip)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("3. Enqueued with explicit compression (%d bytes) at offset %d\n", len(mediumMsg), offset3)
	fmt.Println("   → Explicitly requested gzip compression")

	// Example 4: Disable compression for specific message
	offset4, err := q.EnqueueWithCompression(largeMsg, ledgerq.CompressionNone)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("4. Enqueued without compression (%d bytes) at offset %d\n", len(largeMsg), offset4)
	fmt.Println("   → Explicitly disabled compression")

	// Example 5: Batch with mixed compression
	batch := []ledgerq.BatchEnqueueOptions{
		{
			Payload:     []byte("Compressed batch message 1"),
			Compression: ledgerq.CompressionGzip,
		},
		{
			Payload:     []byte("Uncompressed batch message 2"),
			Compression: ledgerq.CompressionNone,
		},
		{
			Payload:     []byte(strings.Repeat("Auto-compressed batch message 3. ", 30)),
			Compression: ledgerq.CompressionNone, // Will use queue default (gzip)
		},
	}
	offsets, err := q.EnqueueBatchWithOptions(batch)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("5. Enqueued batch with mixed compression at offsets %v\n", offsets)
	fmt.Println("   → Each message can have different compression settings")

	// Example 6: All options including compression
	opts6 := ledgerq.EnqueueOptions{
		Priority:    ledgerq.PriorityHigh,
		Headers:     map[string]string{"type": "compressed"},
		Compression: ledgerq.CompressionGzip,
	}
	payload6 := []byte(strings.Repeat("High priority compressed message with headers. ", 20))
	offset6, err := q.EnqueueWithAllOptions(payload6, opts6)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("6. Enqueued with all options (%d bytes) at offset %d\n", len(payload6), offset6)
	fmt.Println("   → Priority + Headers + Compression combined")

	// Dequeue all messages
	fmt.Println("\n=== Dequeuing Messages (Automatic Decompression) ===")

	for i := 1; i <= 9; i++ {
		msg, err := q.Dequeue()
		if err != nil {
			log.Printf("Dequeue %d: %v\n", i, err)
			break
		}
		fmt.Printf("Message %d: %d bytes (decompressed automatically)\n", i, len(msg.Payload))
	}

	fmt.Println("\n=== Compression Benefits ===")
	fmt.Println("✓ Reduced disk usage for large messages")
	fmt.Println("✓ Lower I/O overhead")
	fmt.Println("✓ Transparent to consumers (auto-decompressed)")
	fmt.Println("✓ Configurable per-queue and per-message")
	fmt.Println("✓ Zero external dependencies (stdlib only)")
}
