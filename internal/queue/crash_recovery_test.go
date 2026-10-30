package queue

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/1mb-dev/ledgerq/internal/format"
	"github.com/1mb-dev/ledgerq/internal/logging"
	"github.com/1mb-dev/ledgerq/internal/segment"
)

const crashEntries = 5

func crashOptions(dir string) *Options {
	opts := DefaultOptions(dir)
	opts.AutoSync = true
	return opts
}

// fillQueue writes crashEntries messages and closes cleanly, leaving one segment with entries.
func fillQueue(t *testing.T, dir string) {
	t.Helper()
	q, err := Open(dir, crashOptions(dir))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	for i := 0; i < crashEntries; i++ {
		if _, err := q.Enqueue(bytes.Repeat([]byte{byte('a' + i)}, 100)); err != nil {
			t.Fatalf("Enqueue() error = %v", err)
		}
	}
	if err := q.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

// reopenEmpty adds a header-only segment after the existing ones, as a clean restart does.
func reopenEmpty(t *testing.T, dir string) {
	t.Helper()
	q, err := Open(dir, crashOptions(dir))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if err := q.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func segmentWithEntries(t *testing.T, dir string) string {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join(dir, "*"+segment.SegmentFileExtension))
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(paths)
	for _, path := range paths {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if info.Size() > format.SegmentHeaderSize {
			return path
		}
	}
	t.Fatal("no segment with entries")
	return ""
}

func entryOffsets(t *testing.T, path string) []uint64 {
	t.Helper()
	r, err := segment.NewReader(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Close() }()
	var offsets []uint64
	err = r.ScanAll(func(_ *format.Entry, offset uint64) error {
		offsets = append(offsets, offset)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return offsets
}

func truncateTo(t *testing.T, path string, size int64) {
	t.Helper()
	if err := os.Truncate(path, size); err != nil {
		t.Fatal(err)
	}
}

// assertUsable drains the queue, checks the count, then proves it still takes new messages.
func assertUsable(t *testing.T, dir string, wantMessages int) {
	t.Helper()
	q, err := Open(dir, crashOptions(dir))
	if err != nil {
		t.Fatalf("Open() after crash error = %v", err)
	}
	defer func() { _ = q.Close() }()

	for i := 0; i < wantMessages; i++ {
		msg, err := q.Dequeue()
		if err != nil {
			t.Fatalf("Dequeue() %d of %d error = %v", i+1, wantMessages, err)
		}
		if want := bytes.Repeat([]byte{byte('a' + i)}, 100); !bytes.Equal(msg.Payload, want) {
			t.Fatalf("Dequeue() %d payload = %q, want %q", i+1, msg.Payload[:1], want[:1])
		}
	}
	if msg, err := q.Dequeue(); err == nil {
		t.Fatalf("Dequeue() past the end returned ID %d, want an error", msg.ID)
	} else if err.Error() != "no messages available" {
		t.Fatalf("Dequeue() past the end error = %v, want no messages available", err)
	}

	if _, err := q.Enqueue([]byte("after the crash")); err != nil {
		t.Fatalf("Enqueue() after crash error = %v", err)
	}
	msg, err := q.Dequeue()
	if err != nil {
		t.Fatalf("Dequeue() of new message error = %v", err)
	}
	if string(msg.Payload) != "after the crash" {
		t.Fatalf("Dequeue() payload = %q, want the new message", msg.Payload)
	}
}

func TestRecovery_TornTail(t *testing.T) {
	for _, keep := range []int64{1, 7, 30, 80} {
		t.Run(fmt.Sprintf("%d bytes of the last entry on disk", keep), func(t *testing.T) {
			dir := t.TempDir()
			fillQueue(t, dir)
			path := segmentWithEntries(t, dir)
			offsets := entryOffsets(t, path)

			truncateTo(t, path, int64(offsets[crashEntries-1])+keep)

			assertUsable(t, dir, crashEntries-1)
		})
	}
}

func TestRecovery_MetadataAheadOfLog(t *testing.T) {
	dir := t.TempDir()
	fillQueue(t, dir)
	path := segmentWithEntries(t, dir)

	// A clean cut on an entry boundary: nothing is torn, but metadata counts messages the log lost.
	truncateTo(t, path, int64(entryOffsets(t, path)[3]))

	assertUsable(t, dir, 3)
}

func TestRecovery_SegmentFileWithoutHeader(t *testing.T) {
	for _, size := range []int{0, 10} {
		t.Run(fmt.Sprintf("%d bytes", size), func(t *testing.T) {
			dir := t.TempDir()
			fillQueue(t, dir)

			// A crash before the first sync leaves the new segment's header in the write buffer.
			name := segment.FormatSegmentName(9_000_000_000)
			if err := os.WriteFile(filepath.Join(dir, name), make([]byte, size), 0o644); err != nil {
				t.Fatal(err)
			}

			assertUsable(t, dir, crashEntries)
		})
	}
}

func TestRecovery_TornSegmentBehindEmptySegments(t *testing.T) {
	dir := t.TempDir()
	fillQueue(t, dir)
	reopenEmpty(t, dir)
	reopenEmpty(t, dir)
	path := segmentWithEntries(t, dir)

	// The state a failed Open used to leave: the torn segment is no longer the last one.
	truncateTo(t, path, int64(entryOffsets(t, path)[4])+7)

	assertUsable(t, dir, crashEntries-1)
}

func TestRecovery_EveryEntryLost(t *testing.T) {
	dir := t.TempDir()
	fillQueue(t, dir)
	truncateTo(t, segmentWithEntries(t, dir), format.SegmentHeaderSize+3)

	assertUsable(t, dir, 0)
}

func TestRecovery_RemovesStaleIndex(t *testing.T) {
	dir := t.TempDir()
	fillQueue(t, dir)
	path := segmentWithEntries(t, dir)
	offsets := entryOffsets(t, path)
	truncateTo(t, path, int64(offsets[4])+7)

	assertUsable(t, dir, crashEntries-1)

	index := path[:len(path)-len(segment.SegmentFileExtension)] + segment.IndexFileExtension
	if _, err := os.Stat(index); !os.IsNotExist(err) {
		t.Fatalf("index for a truncated segment still exists, Stat() error = %v", err)
	}
}

func TestRecovery_CorruptEntryStillFailsLoudly(t *testing.T) {
	dir := t.TempDir()
	fillQueue(t, dir)
	path := segmentWithEntries(t, dir)
	offsets := entryOffsets(t, path)

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	data[offsets[2]+30] ^= 0xff
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	size := int64(len(data))

	if _, err := Open(dir, crashOptions(dir)); err == nil {
		t.Fatal("Open() over a corrupt entry succeeded, want an error")
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != size {
		t.Fatalf("corrupt segment is %d bytes, want it left at %d", info.Size(), size)
	}
}

func TestRecovery_CleanRestartKeepsReadPosition(t *testing.T) {
	dir := t.TempDir()
	fillQueue(t, dir)

	q, err := Open(dir, crashOptions(dir))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if _, err := q.Dequeue(); err != nil {
			t.Fatal(err)
		}
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	reopenEmpty(t, dir)

	q, err = Open(dir, crashOptions(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = q.Close() }()
	msg, err := q.Dequeue()
	if err != nil {
		t.Fatal(err)
	}
	if msg.ID != 3 {
		t.Fatalf("Dequeue() after restart ID = %d, want 3", msg.ID)
	}
}

func TestRecovery_HeaderlessSegmentBeforeAnEmptyOne(t *testing.T) {
	dir := t.TempDir()
	fillQueue(t, dir)
	reopenEmpty(t, dir)

	// Removing a segment from the middle must keep the ones after it, or rotation collides.
	name := segment.FormatSegmentName(500_000)
	if err := os.WriteFile(filepath.Join(dir, name), nil, 0o644); err != nil {
		t.Fatal(err)
	}

	assertUsable(t, dir, crashEntries)
}

type warnLogger struct {
	logging.NoopLogger
	warnings []string
}

func (l *warnLogger) Warn(msg string, _ ...logging.Field) {
	l.warnings = append(l.warnings, msg)
}

func TestRecovery_WarnsWhatWasLost(t *testing.T) {
	dir := t.TempDir()
	fillQueue(t, dir)
	path := segmentWithEntries(t, dir)
	truncateTo(t, path, int64(entryOffsets(t, path)[4])+7)

	logger := &warnLogger{}
	opts := crashOptions(dir)
	opts.Logger = logger
	q, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = q.Close() }()

	got := strings.Join(logger.warnings, "; ")
	for _, want := range []string{"cut a torn entry", "never reached disk"} {
		if !strings.Contains(got, want) {
			t.Errorf("warnings = %q, want one containing %q", got, want)
		}
	}
}

func TestRecovery_NoWarningOnCleanRestart(t *testing.T) {
	dir := t.TempDir()
	fillQueue(t, dir)

	logger := &warnLogger{}
	opts := crashOptions(dir)
	opts.Logger = logger
	q, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = q.Close() }()

	if len(logger.warnings) != 0 {
		t.Fatalf("warnings on a clean restart = %q", logger.warnings)
	}
}

func TestOpen_SecondOpenLeavesALiveQueueAlone(t *testing.T) {
	dir := t.TempDir()
	q, err := Open(dir, DefaultOptions(dir))
	if err != nil {
		t.Fatal(err)
	}
	// Buffered: the file on disk ends wherever the buffer last flushed, as it does for a live writer.
	for i := 0; i < 500; i++ {
		if _, err := q.Enqueue(make([]byte, 200)); err != nil {
			t.Fatal(err)
		}
	}
	path := q.segments.GetActiveSegment().Path
	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := Open(dir, DefaultOptions(dir)); err == nil {
		t.Fatal("second Open() succeeded, want it refused")
	} else if !strings.Contains(err.Error(), "open in another process") {
		t.Fatalf("second Open() error = %v", err)
	}

	after, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if after.Size() != before.Size() {
		t.Fatalf("live segment went from %d to %d bytes", before.Size(), after.Size())
	}

	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	q, err = Open(dir, DefaultOptions(dir))
	if err != nil {
		t.Fatalf("Open() after Close() error = %v", err)
	}
	_ = q.Close()
}

func TestOpen_FailedOpenReleasesTheLock(t *testing.T) {
	dir := t.TempDir()
	fillQueue(t, dir)
	path := segmentWithEntries(t, dir)
	offsets := entryOffsets(t, path)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	data[offsets[2]+30] ^= 0xff
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 2; i++ {
		_, err := Open(dir, crashOptions(dir))
		if err == nil {
			t.Fatal("Open() over a corrupt entry succeeded")
		}
		if strings.Contains(err.Error(), "open in another process") {
			t.Fatalf("attempt %d found the lock still held: %v", i+1, err)
		}
	}
}
