//go:build unix || linux || darwin || freebsd || openbsd || netbsd

package queue

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"syscall"
	"testing"

	"github.com/1mb-dev/ledgerq/internal/format"
)

// A file size limit stands in for a full disk: it stops an entry partway through.
func TestRecovery_ShortWriteChild(t *testing.T) {
	dir := os.Getenv("LEDGERQ_SHORT_WRITE_DIR")
	if dir == "" {
		t.Skip("runs as a child of TestRecovery_ShortWriteIsNotStamped")
	}
	signal.Ignore(syscall.SIGXFSZ)

	q, err := Open(dir, crashOptions(dir))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := q.Enqueue(bytes.Repeat([]byte{'a'}, 100)); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(q.segments.GetActiveSegment().Path)
	if err != nil {
		t.Fatal(err)
	}
	limit := uint64(info.Size()) + 100*1024 //nolint:gosec // G115: Size is positive
	if err := syscall.Setrlimit(syscall.RLIMIT_FSIZE, &syscall.Rlimit{Cur: limit, Max: limit}); err != nil {
		t.Fatal(err)
	}
	_, enqueueErr := q.Enqueue(make([]byte, 1<<20))
	fmt.Printf("enqueue=%v close=%v\n", enqueueErr, q.Close())
}

func TestRecovery_ShortWriteIsNotStamped(t *testing.T) {
	dir := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=^TestRecovery_ShortWriteChild$") //nolint:gosec // G204: The test binary
	cmd.Env = append(os.Environ(), "LEDGERQ_SHORT_WRITE_DIR="+dir)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("child: %v\n%s", err, out)
	}
	if !bytes.Contains(out, []byte("enqueue=failed")) || bytes.Contains(out, []byte("close=<nil>")) {
		t.Fatalf("child output = %s, want the enqueue and the close to fail", out)
	}

	header, err := format.ReadSegmentHeader(segmentWithEntries(t, dir))
	if err != nil {
		t.Fatal(err)
	}
	if header.MaxMsgID != 0 {
		t.Fatalf("a segment with a short write was stamped closed, MaxMsgID = %d", header.MaxMsgID)
	}

	assertUsable(t, dir, 1)
}
