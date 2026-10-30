package segment

import (
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/1mb-dev/ledgerq/internal/format"
)

// tornExtension marks the bytes repair cut from a segment. Nothing deletes these files.
const tornExtension = ".torn"

// repairTail makes the segments a crash left behind readable. Writes are buffered, so a crash can
// cut the newest entry short, or leave a segment whose header never reached disk.
//
// Only the newest segment holding entries can be torn: every Open starts a new one. Header-only
// segments after it are skipped, since a failed Open used to leave them behind.
//
// A closed segment is never cut. Close syncs the entries before it stamps MaxMsgID, so a stamped
// segment that ends mid-entry is damaged, not crashed, and Open fails. An unclosed segment can't
// make that distinction: a corrupt length reads as a torn entry, so the cut bytes are kept beside
// the segment, and repair stamps it so that later damage fails too.
func repairTail(segments []*SegmentInfo) ([]*SegmentInfo, int64, error) {
	for i := len(segments) - 1; i >= 0; i-- {
		seg := segments[i]
		switch {
		case seg.Size < format.SegmentHeaderSize:
			if err := os.Remove(seg.Path); err != nil {
				return nil, 0, fmt.Errorf("failed to remove headerless segment: %w", err)
			}
			segments = append(segments[:i], segments[i+1:]...)
		case seg.Size == format.SegmentHeaderSize:
			continue
		default:
			dropped, err := repairSegment(seg)
			return segments, dropped, err
		}
	}
	return segments, 0, nil
}

// segmentScan is what a read of every complete entry in a segment found.
type segmentScan struct {
	header *format.SegmentHeader
	end    uint64 // Offset just past the last complete entry
	torn   bool   // The bytes after end are an incomplete entry
	minID  uint64
	maxID  uint64
}

// repairSegment cuts an unclosed segment back to its last complete entry, stamps it as closed,
// and returns the bytes it cut.
func repairSegment(seg *SegmentInfo) (int64, error) {
	scan, err := scanSegment(seg.Path)
	if err != nil {
		return 0, fmt.Errorf("failed to scan segment %s: %w", seg.Path, err)
	}
	if scan.header.MaxMsgID != 0 {
		if scan.torn {
			return 0, fmt.Errorf("closed segment %s is damaged at offset %d", seg.Path, scan.end)
		}
		return 0, nil
	}

	var dropped int64
	if scan.torn {
		if err := saveTail(seg, scan.end); err != nil {
			return 0, err
		}
		dropped = seg.Size - int64(scan.end) //nolint:gosec // G115: end is within the file
	}

	file, err := os.OpenFile(seg.Path, os.O_WRONLY, 0) //nolint:gosec // G304: Path from DiscoverSegments
	if err != nil {
		return 0, fmt.Errorf("failed to open segment for repair: %w", err)
	}
	defer func() { _ = file.Close() }()

	if scan.torn {
		if err := file.Truncate(int64(scan.end)); err != nil { //nolint:gosec // G115: end is within the file
			return 0, fmt.Errorf("failed to truncate torn segment: %w", err)
		}
		seg.Size = int64(scan.end) //nolint:gosec // G115: end is within the file
	}
	if scan.maxID == 0 {
		return dropped, file.Sync()
	}

	// Entries first, as Close does: the stamp must only ever follow complete data.
	if err := file.Sync(); err != nil {
		return 0, fmt.Errorf("failed to sync repaired segment: %w", err)
	}
	scan.header.MinMsgID = scan.minID
	scan.header.MaxMsgID = scan.maxID
	if _, err := file.WriteAt(scan.header.Marshal(), 0); err != nil {
		return 0, fmt.Errorf("failed to stamp repaired segment: %w", err)
	}
	if err := file.Sync(); err != nil {
		return 0, fmt.Errorf("failed to sync repaired segment header: %w", err)
	}
	return dropped, nil
}

// saveTail copies the bytes after end into a side file and syncs it before anything is cut.
func saveTail(seg *SegmentInfo, end uint64) error {
	src, err := os.Open(seg.Path)
	if err != nil {
		return fmt.Errorf("failed to open torn segment: %w", err)
	}
	defer func() { _ = src.Close() }()

	path := seg.Path + tornExtension
	dst, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644) //nolint:gosec // G304: Path from DiscoverSegments
	if err != nil {
		return fmt.Errorf("failed to create %s: %w", path, err)
	}
	defer func() { _ = dst.Close() }()

	tail := io.NewSectionReader(src, int64(end), seg.Size-int64(end)) //nolint:gosec // G115: end is within the file
	if _, err := io.Copy(dst, tail); err != nil {
		return fmt.Errorf("failed to save torn bytes to %s: %w", path, err)
	}
	if err := dst.Sync(); err != nil {
		return fmt.Errorf("failed to sync %s: %w", path, err)
	}
	return nil
}

func scanSegment(path string) (*segmentScan, error) {
	reader, err := NewReader(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = reader.Close() }()

	scan := &segmentScan{header: reader.Header(), end: uint64(format.SegmentHeaderSize)}
	for {
		entry, next, err := reader.ReadAt(scan.end)
		if err == io.EOF {
			return scan, nil
		}
		// A wrapped EOF means the entry's length arrived and nothing after it did.
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			scan.torn = true
			return scan, nil
		}
		if err != nil {
			return nil, err
		}
		if scan.minID == 0 || entry.MsgID < scan.minID {
			scan.minID = entry.MsgID
		}
		if entry.MsgID > scan.maxID {
			scan.maxID = entry.MsgID
		}
		scan.end = next
	}
}
