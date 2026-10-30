package segment

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/1mb-dev/ledgerq/internal/format"
)

func closedSegment(t *testing.T, dir string) (*SegmentInfo, []uint64) {
	t.Helper()
	w, err := NewWriter(dir, 1, DefaultWriterOptions())
	if err != nil {
		t.Fatal(err)
	}
	var offsets []uint64
	for id := uint64(1); id <= 6; id++ {
		entry := &format.Entry{
			Type:      format.EntryTypeData,
			MsgID:     id,
			Timestamp: int64(id),
			Payload:   bytes.Repeat([]byte{byte(id)}, 100),
		}
		offset, err := w.Write(entry)
		if err != nil {
			t.Fatal(err)
		}
		offsets = append(offsets, offset)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(w.path)
	if err != nil {
		t.Fatal(err)
	}
	return &SegmentInfo{BaseOffset: 1, Path: w.path, IndexPath: w.indexPath, Size: info.Size()}, offsets
}

// Each bit of each length field, at the first, middle and last entry of a closed segment: damage
// must fail Open and leave the bytes alone, wherever the corrupt length claims the entry ends.
func TestRepairTail_ClosedSegmentFailsOnEveryLengthFlip(t *testing.T) {
	seg, offsets := closedSegment(t, t.TempDir())
	original, err := os.ReadFile(seg.Path)
	if err != nil {
		t.Fatal(err)
	}

	for _, offset := range []uint64{offsets[0], offsets[3], offsets[5]} {
		for bit := uint64(0); bit < 32; bit++ {
			data := bytes.Clone(original)
			data[offset+bit/8] ^= 1 << (bit % 8)
			path := filepath.Join(t.TempDir(), filepath.Base(seg.Path))
			if err := os.WriteFile(path, data, 0o644); err != nil {
				t.Fatal(err)
			}

			damaged := &SegmentInfo{BaseOffset: 1, Path: path, IndexPath: path + ".idx", Size: seg.Size}
			if _, _, err := repairTail([]*SegmentInfo{damaged}); err == nil {
				t.Errorf("flip of bit %d at offset %d: repairTail() succeeded", bit, offset)
			}
			after, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(after, data) {
				t.Errorf("flip of bit %d at offset %d: segment was changed", bit, offset)
			}
		}
	}
}

func TestRepairTail_LeavesAnIntactClosedSegmentAlone(t *testing.T) {
	seg, _ := closedSegment(t, t.TempDir())
	before, err := os.ReadFile(seg.Path)
	if err != nil {
		t.Fatal(err)
	}

	_, dropped, err := repairTail([]*SegmentInfo{seg})
	if err != nil {
		t.Fatalf("repairTail() error = %v", err)
	}
	if dropped != 0 {
		t.Fatalf("repairTail() dropped %d bytes, want 0", dropped)
	}
	after, err := os.ReadFile(seg.Path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(after, before) {
		t.Fatal("repairTail() changed an intact closed segment")
	}
}
