// Package metrics provides Prometheus metrics integration for LedgerQ.
//
// This package offers optional Prometheus metrics collection without
// requiring Prometheus as a dependency. Metrics are exposed via the
// standard Prometheus client library pattern, but can be disabled
// entirely by not registering them.
//
// Usage:
//
//	import (
//	    "github.com/prometheus/client_golang/prometheus"
//	    "github.com/1mb-dev/ledgerq/internal/metrics"
//	)
//
//	// Create metrics collector
//	collector := metrics.NewCollector("my_queue")
//
//	// Register with Prometheus (optional)
//	prometheus.MustRegister(collector)
//
//	// Record operations
//	collector.RecordEnqueue(payloadSize, duration)
//	collector.RecordDequeue(payloadSize, duration)
package metrics

import (
	"sync/atomic"
	"time"
)

// Collector tracks queue metrics.
// Can be used standalone or integrated with Prometheus.
type Collector struct {
	queueName string

	// Operation counters
	enqueueTotal   atomic.Uint64
	dequeueTotal   atomic.Uint64
	enqueueBatch   atomic.Uint64
	dequeueBatch   atomic.Uint64
	enqueueErrors  atomic.Uint64
	dequeueErrors  atomic.Uint64
	seekOperations atomic.Uint64

	// Payload metrics
	enqueueBytes atomic.Uint64
	dequeueBytes atomic.Uint64

	// Duration histograms (stored as buckets for simplicity)
	enqueueDurations *durationHistogram
	dequeueDurations *durationHistogram

	// Queue state (updated periodically)
	pendingMessages atomic.Uint64
	segmentCount    atomic.Uint64
	nextMessageID   atomic.Uint64
	readMessageID   atomic.Uint64

	// Compaction metrics
	compactionsTotal  atomic.Uint64
	segmentsRemoved   atomic.Uint64
	bytesFreed        atomic.Uint64
	compactionErrors  atomic.Uint64
	lastCompactionSec atomic.Int64 // Unix seconds
}

// NewCollector creates a new metrics collector for a queue.
func NewCollector(queueName string) *Collector {
	return &Collector{
		queueName:        queueName,
		enqueueDurations: newDurationHistogram(),
		dequeueDurations: newDurationHistogram(),
	}
}

// RecordEnqueue records a successful enqueue operation.
func (c *Collector) RecordEnqueue(payloadSize int, duration time.Duration) {
	c.enqueueTotal.Add(1)
	c.enqueueBytes.Add(uint64(payloadSize))
	c.enqueueDurations.observe(duration)
}

// RecordDequeue records a successful dequeue operation.
func (c *Collector) RecordDequeue(payloadSize int, duration time.Duration) {
	c.dequeueTotal.Add(1)
	c.dequeueBytes.Add(uint64(payloadSize))
	c.dequeueDurations.observe(duration)
}

// RecordEnqueueBatch records a successful batch enqueue operation.
func (c *Collector) RecordEnqueueBatch(count int, totalPayloadSize int, duration time.Duration) {
	c.enqueueBatch.Add(1)
	c.enqueueTotal.Add(uint64(count))
	c.enqueueBytes.Add(uint64(totalPayloadSize))
	c.enqueueDurations.observe(duration)
}

// RecordDequeueBatch records a successful batch dequeue operation.
func (c *Collector) RecordDequeueBatch(count int, totalPayloadSize int, duration time.Duration) {
	c.dequeueBatch.Add(1)
	c.dequeueTotal.Add(uint64(count))
	c.dequeueBytes.Add(uint64(totalPayloadSize))
	c.dequeueDurations.observe(duration)
}

// RecordEnqueueError records an enqueue failure.
func (c *Collector) RecordEnqueueError() {
	c.enqueueErrors.Add(1)
}

// RecordDequeueError records a dequeue failure.
func (c *Collector) RecordDequeueError() {
	c.dequeueErrors.Add(1)
}

// RecordSeek records a seek operation.
func (c *Collector) RecordSeek() {
	c.seekOperations.Add(1)
}

// RecordCompaction records a compaction operation.
func (c *Collector) RecordCompaction(segmentsRemoved int, bytesFreed int64, duration time.Duration) {
	c.compactionsTotal.Add(1)
	c.segmentsRemoved.Add(uint64(segmentsRemoved))
	c.bytesFreed.Add(uint64(bytesFreed))
	c.lastCompactionSec.Store(time.Now().Unix())
}

// RecordCompactionError records a compaction failure.
func (c *Collector) RecordCompactionError() {
	c.compactionErrors.Add(1)
}

// UpdateQueueState updates queue state metrics (call periodically).
func (c *Collector) UpdateQueueState(pending, segments, nextMsgID, readMsgID uint64) {
	c.pendingMessages.Store(pending)
	c.segmentCount.Store(segments)
	c.nextMessageID.Store(nextMsgID)
	c.readMessageID.Store(readMsgID)
}

// GetSnapshot returns a snapshot of current metrics.
func (c *Collector) GetSnapshot() *Snapshot {
	return &Snapshot{
		QueueName:             c.queueName,
		EnqueueTotal:          c.enqueueTotal.Load(),
		DequeueTotal:          c.dequeueTotal.Load(),
		EnqueueBatch:          c.enqueueBatch.Load(),
		DequeueBatch:          c.dequeueBatch.Load(),
		EnqueueErrors:         c.enqueueErrors.Load(),
		DequeueErrors:         c.dequeueErrors.Load(),
		SeekOperations:        c.seekOperations.Load(),
		EnqueueBytes:          c.enqueueBytes.Load(),
		DequeueBytes:          c.dequeueBytes.Load(),
		EnqueueDurationP50:    c.enqueueDurations.percentile(0.50),
		EnqueueDurationP95:    c.enqueueDurations.percentile(0.95),
		EnqueueDurationP99:    c.enqueueDurations.percentile(0.99),
		DequeueDurationP50:    c.dequeueDurations.percentile(0.50),
		DequeueDurationP95:    c.dequeueDurations.percentile(0.95),
		DequeueDurationP99:    c.dequeueDurations.percentile(0.99),
		PendingMessages:       c.pendingMessages.Load(),
		SegmentCount:          c.segmentCount.Load(),
		NextMessageID:         c.nextMessageID.Load(),
		ReadMessageID:         c.readMessageID.Load(),
		CompactionsTotal:      c.compactionsTotal.Load(),
		SegmentsRemoved:       c.segmentsRemoved.Load(),
		BytesFreed:            c.bytesFreed.Load(),
		CompactionErrors:      c.compactionErrors.Load(),
		LastCompactionUnixSec: c.lastCompactionSec.Load(),
	}
}

// Reset resets all metrics (useful for testing).
func (c *Collector) Reset() {
	c.enqueueTotal.Store(0)
	c.dequeueTotal.Store(0)
	c.enqueueBatch.Store(0)
	c.dequeueBatch.Store(0)
	c.enqueueErrors.Store(0)
	c.dequeueErrors.Store(0)
	c.seekOperations.Store(0)
	c.enqueueBytes.Store(0)
	c.dequeueBytes.Store(0)
	c.enqueueDurations = newDurationHistogram()
	c.dequeueDurations = newDurationHistogram()
	c.pendingMessages.Store(0)
	c.segmentCount.Store(0)
	c.nextMessageID.Store(0)
	c.readMessageID.Store(0)
	c.compactionsTotal.Store(0)
	c.segmentsRemoved.Store(0)
	c.bytesFreed.Store(0)
	c.compactionErrors.Store(0)
	c.lastCompactionSec.Store(0)
}

// Snapshot is a point-in-time view of metrics.
type Snapshot struct {
	QueueName string

	// Operation counters
	EnqueueTotal   uint64
	DequeueTotal   uint64
	EnqueueBatch   uint64
	DequeueBatch   uint64
	EnqueueErrors  uint64
	DequeueErrors  uint64
	SeekOperations uint64

	// Payload metrics
	EnqueueBytes uint64
	DequeueBytes uint64

	// Duration percentiles (in nanoseconds)
	EnqueueDurationP50 time.Duration
	EnqueueDurationP95 time.Duration
	EnqueueDurationP99 time.Duration
	DequeueDurationP50 time.Duration
	DequeueDurationP95 time.Duration
	DequeueDurationP99 time.Duration

	// Queue state
	PendingMessages uint64
	SegmentCount    uint64
	NextMessageID   uint64
	ReadMessageID   uint64

	// Compaction metrics
	CompactionsTotal      uint64
	SegmentsRemoved       uint64
	BytesFreed            uint64
	CompactionErrors      uint64
	LastCompactionUnixSec int64
}

// durationHistogram is a simple histogram for tracking durations.
// Uses fixed buckets for simplicity (no external dependencies).
type durationHistogram struct {
	buckets [10]atomic.Uint64 // 10 buckets for different duration ranges
}

func newDurationHistogram() *durationHistogram {
	return &durationHistogram{}
}

// observe records a duration in the appropriate bucket.
func (h *durationHistogram) observe(d time.Duration) {
	micros := d.Microseconds()
	var bucket int

	// Bucket boundaries (microseconds):
	// 0: < 1μs, 1: 1-10μs, 2: 10-100μs, 3: 100μs-1ms
	// 4: 1-10ms, 5: 10-100ms, 6: 100ms-1s, 7: 1-10s, 8: >10s
	switch {
	case micros < 1:
		bucket = 0
	case micros < 10:
		bucket = 1
	case micros < 100:
		bucket = 2
	case micros < 1000:
		bucket = 3
	case micros < 10000:
		bucket = 4
	case micros < 100000:
		bucket = 5
	case micros < 1000000:
		bucket = 6
	case micros < 10000000:
		bucket = 7
	case micros < 100000000:
		bucket = 8
	default:
		bucket = 9
	}

	h.buckets[bucket].Add(1)
}

// percentile approximates a percentile from histogram buckets.
func (h *durationHistogram) percentile(p float64) time.Duration {
	// Count total observations
	var total uint64
	for i := 0; i < 10; i++ {
		total += h.buckets[i].Load()
	}

	if total == 0 {
		return 0
	}

	// Find the bucket containing the percentile
	target := uint64(float64(total) * p)
	var count uint64
	for i := 0; i < 10; i++ {
		count += h.buckets[i].Load()
		if count >= target {
			// Return the upper bound of this bucket
			switch i {
			case 0:
				return 500 * time.Nanosecond
			case 1:
				return 5 * time.Microsecond
			case 2:
				return 50 * time.Microsecond
			case 3:
				return 500 * time.Microsecond
			case 4:
				return 5 * time.Millisecond
			case 5:
				return 50 * time.Millisecond
			case 6:
				return 500 * time.Millisecond
			case 7:
				return 5 * time.Second
			case 8:
				return 50 * time.Second
			default:
				return 100 * time.Second
			}
		}
	}

	return 0
}

// NoopCollector is a metrics collector that does nothing.
// Useful when metrics are disabled.
type NoopCollector struct{}

func (n NoopCollector) RecordEnqueue(payloadSize int, duration time.Duration)                  {}
func (n NoopCollector) RecordDequeue(payloadSize int, duration time.Duration)                  {}
func (n NoopCollector) RecordEnqueueBatch(count, totalPayloadSize int, duration time.Duration) {}
func (n NoopCollector) RecordDequeueBatch(count, totalPayloadSize int, duration time.Duration) {}
func (n NoopCollector) RecordEnqueueError()                                                    {}
func (n NoopCollector) RecordDequeueError()                                                    {}
func (n NoopCollector) RecordSeek()                                                            {}
func (n NoopCollector) RecordCompaction(segmentsRemoved int, bytesFreed int64, duration time.Duration) {
}
func (n NoopCollector) RecordCompactionError()                                          {}
func (n NoopCollector) UpdateQueueState(pending, segments, nextMsgID, readMsgID uint64) {}
func (n NoopCollector) GetSnapshot() *Snapshot                                          { return nil }
func (n NoopCollector) Reset()                                                          {}
