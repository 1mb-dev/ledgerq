// Command ledgerq provides a CLI tool for inspecting and managing LedgerQ queues.
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"text/tabwriter"
	"time"

	"github.com/1mb-dev/ledgerq/pkg/ledgerq"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	command := os.Args[1]

	switch command {
	case "stats":
		handleStats()
	case "inspect":
		handleInspect()
	case "compact":
		handleCompact()
	case "peek":
		handlePeek()
	case "version":
		fmt.Printf("ledgerq version %s\n", ledgerq.Version)
	case "help", "-h", "--help":
		printUsage()
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n\n", command)
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println("LedgerQ CLI Tool - Queue Inspection and Management")
	fmt.Println()
	fmt.Println("Usage:")
	fmt.Println("  ledgerq <command> [options]")
	fmt.Println()
	fmt.Println("Commands:")
	fmt.Println("  stats <queue-dir>              Show queue statistics")
	fmt.Println("  inspect <queue-dir>            Detailed queue inspection")
	fmt.Println("  compact <queue-dir>            Manually trigger compaction")
	fmt.Println("  peek <queue-dir> [count]       Peek at next N messages without consuming")
	fmt.Println("  version                        Show version information")
	fmt.Println("  help                           Show this help message")
	fmt.Println()
	fmt.Println("Examples:")
	fmt.Println("  ledgerq stats /path/to/queue")
	fmt.Println("  ledgerq inspect /path/to/queue")
	fmt.Println("  ledgerq compact /path/to/queue")
	fmt.Println("  ledgerq peek /path/to/queue 5")
}

func handleStats() {
	if len(os.Args) < 3 {
		fmt.Fprintln(os.Stderr, "Error: queue directory required")
		fmt.Fprintln(os.Stderr, "Usage: ledgerq stats <queue-dir>")
		os.Exit(1)
	}

	queueDir := os.Args[2]

	q, err := ledgerq.Open(queueDir, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening queue: %v\n", err)
		os.Exit(1)
	}
	defer q.Close()

	stats := q.Stats()

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "Queue Statistics")
	fmt.Fprintln(w, "================")
	fmt.Fprintf(w, "Directory:\t%s\n", queueDir)
	fmt.Fprintf(w, "Total Messages:\t%d\n", stats.TotalMessages)
	fmt.Fprintf(w, "Pending Messages:\t%d\n", stats.PendingMessages)
	fmt.Fprintf(w, "Next Message ID:\t%d\n", stats.NextMessageID)
	fmt.Fprintf(w, "Read Message ID:\t%d\n", stats.ReadMessageID)
	fmt.Fprintf(w, "Segment Count:\t%d\n", stats.SegmentCount)

	if stats.TotalMessages > 0 {
		consumedPct := float64(stats.TotalMessages-stats.PendingMessages) / float64(stats.TotalMessages) * 100
		fmt.Fprintf(w, "Consumed:\t%.1f%%\n", consumedPct)
	}

	if err := w.Flush(); err != nil {
		fmt.Fprintf(os.Stderr, "warning: output formatting failed: %v\n", err)
	}
}

func handleInspect() {
	if len(os.Args) < 3 {
		fmt.Fprintln(os.Stderr, "Error: queue directory required")
		fmt.Fprintln(os.Stderr, "Usage: ledgerq inspect <queue-dir>")
		os.Exit(1)
	}

	queueDir := os.Args[2]

	q, err := ledgerq.Open(queueDir, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening queue: %v\n", err)
		os.Exit(1)
	}
	defer q.Close()

	stats := q.Stats()

	inspection := map[string]interface{}{
		"directory":        queueDir,
		"total_messages":   stats.TotalMessages,
		"pending_messages": stats.PendingMessages,
		"next_message_id":  stats.NextMessageID,
		"read_message_id":  stats.ReadMessageID,
		"segment_count":    stats.SegmentCount,
		"timestamp":        time.Now().UTC().Format(time.RFC3339),
	}

	if stats.TotalMessages > 0 {
		consumedPct := float64(stats.TotalMessages-stats.PendingMessages) / float64(stats.TotalMessages) * 100
		inspection["consumed_percentage"] = fmt.Sprintf("%.1f%%", consumedPct)
	}

	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(inspection); err != nil {
		fmt.Fprintf(os.Stderr, "Error encoding JSON: %v\n", err)
		os.Exit(1)
	}
}

func handleCompact() {
	if len(os.Args) < 3 {
		fmt.Fprintln(os.Stderr, "Error: queue directory required")
		fmt.Fprintln(os.Stderr, "Usage: ledgerq compact <queue-dir>")
		os.Exit(1)
	}

	queueDir := os.Args[2]

	q, err := ledgerq.Open(queueDir, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening queue: %v\n", err)
		os.Exit(1)
	}
	defer q.Close()

	fmt.Printf("Compacting queue at %s...\n", queueDir)

	result, err := q.Compact()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error during compaction: %v\n", err)
		os.Exit(1)
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "\nCompaction Result")
	fmt.Fprintln(w, "=================")
	fmt.Fprintf(w, "Segments Removed:\t%d\n", result.SegmentsRemoved)
	fmt.Fprintf(w, "Bytes Freed:\t%d (%.2f MB)\n", result.BytesFreed, float64(result.BytesFreed)/1024/1024)
	if err := w.Flush(); err != nil {
		fmt.Fprintf(os.Stderr, "warning: output formatting failed: %v\n", err)
	}

	if result.SegmentsRemoved == 0 {
		fmt.Println("\nNo segments were eligible for removal.")
	} else {
		fmt.Println("\n✓ Compaction completed successfully")
	}
}

func handlePeek() {
	if len(os.Args) < 3 {
		fmt.Fprintln(os.Stderr, "Error: queue directory required")
		fmt.Fprintln(os.Stderr, "Usage: ledgerq peek <queue-dir> [count]")
		os.Exit(1)
	}

	queueDir := os.Args[2]
	count := 10 // default

	if len(os.Args) > 3 {
		if _, err := fmt.Sscanf(os.Args[3], "%d", &count); err != nil {
			fmt.Fprintf(os.Stderr, "Error: invalid count: %v\n", err)
			os.Exit(1)
		}
	}

	if count <= 0 {
		fmt.Fprintln(os.Stderr, "Error: count must be positive")
		os.Exit(1)
	}

	q, err := ledgerq.Open(queueDir, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening queue: %v\n", err)
		os.Exit(1)
	}
	defer q.Close()

	stats := q.Stats()
	currentReadPos := stats.ReadMessageID

	// Peek by temporarily seeking and reading, then seeking back
	messages, err := q.DequeueBatch(count)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error peeking messages: %v\n", err)
		os.Exit(1)
	}

	// Seek back to original position
	if err := q.SeekToMessageID(currentReadPos); err != nil {
		fmt.Fprintf(os.Stderr, "Error restoring read position: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Peeking at next %d message(s) from position %d:\n\n", len(messages), currentReadPos)

	for i, msg := range messages {
		ts := time.Unix(0, msg.Timestamp).Format(time.RFC3339)
		fmt.Printf("Message %d:\n", i+1)
		fmt.Printf("  ID:        %d\n", msg.ID)
		fmt.Printf("  Offset:    %d\n", msg.Offset)
		fmt.Printf("  Timestamp: %s\n", ts)
		fmt.Printf("  Size:      %d bytes\n", len(msg.Payload))

		// Show payload preview (first 100 chars)
		payload := string(msg.Payload)
		if len(payload) > 100 {
			payload = payload[:100] + "..."
		}
		fmt.Printf("  Payload:   %q\n", payload)
		fmt.Println()
	}

	fmt.Printf("Note: Read position unchanged at %d\n", currentReadPos)
}
