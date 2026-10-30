package segment

import (
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/1mb-dev/ledgerq/internal/format"
)

// repairTail makes the segments a crash left behind readable. Writes are buffered, so a crash can
// cut the newest entry short, or leave a segment whose header never reached disk.
//
// Only the newest segment holding entries can be torn: every Open starts a new one. Header-only
// segments after it are skipped, since a failed Open used to leave them behind.
//
// A complete entry that fails its checksum is corruption, not a crash, and still fails. A corrupt
// length cannot be told from a torn entry: it reads as one, and the segment is cut there. The
// returned count of bytes dropped is what lets Open say so.
func repairTail(segments []*SegmentInfo) ([]*SegmentInfo, int64, error) {
	for i := len(segments) - 1; i >= 0; i-- {
		seg := segments[i]
		switch {
		case seg.Size < format.SegmentHeaderSize:
			if err := removeSegment(seg); err != nil {
				return nil, 0, err
			}
			segments = append(segments[:i], segments[i+1:]...)
		case seg.Size == format.SegmentHeaderSize:
			continue
		default:
			dropped, err := truncateTornEntry(seg)
			return segments, dropped, err
		}
	}
	return segments, 0, nil
}

func removeSegment(seg *SegmentInfo) error {
	if err := os.Remove(seg.Path); err != nil {
		return fmt.Errorf("failed to remove headerless segment: %w", err)
	}
	return removeIndex(seg)
}

func removeIndex(seg *SegmentInfo) error {
	if err := os.Remove(seg.IndexPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("failed to remove stale index: %w", err)
	}
	return nil
}

// truncateTornEntry cuts the segment back to its last complete entry, if it ends mid-entry, and
// returns the bytes it dropped.
func truncateTornEntry(seg *SegmentInfo) (int64, error) {
	end, torn, err := lastCompleteEntry(seg.Path)
	if err != nil {
		return 0, fmt.Errorf("failed to scan segment %s: %w", seg.Path, err)
	}
	if !torn {
		return 0, nil
	}

	file, err := os.OpenFile(seg.Path, os.O_WRONLY, 0) //nolint:gosec // G304: Path from DiscoverSegments
	if err != nil {
		return 0, fmt.Errorf("failed to open torn segment: %w", err)
	}
	defer func() { _ = file.Close() }()

	if err := file.Truncate(int64(end)); err != nil { //nolint:gosec // G115: end is within the file
		return 0, fmt.Errorf("failed to truncate torn segment: %w", err)
	}
	if err := file.Sync(); err != nil {
		return 0, fmt.Errorf("failed to sync truncated segment: %w", err)
	}
	dropped := seg.Size - int64(end) //nolint:gosec // G115: end is within the file
	seg.Size = int64(end)            //nolint:gosec // G115: end is within the file

	// The index may point past the new end of the segment.
	return dropped, removeIndex(seg)
}

// lastCompleteEntry returns the offset just past the last complete entry, and whether the bytes
// after it are a torn entry.
func lastCompleteEntry(path string) (end uint64, torn bool, err error) {
	reader, err := NewReader(path)
	if err != nil {
		return 0, false, err
	}
	defer func() { _ = reader.Close() }()

	offset := uint64(format.SegmentHeaderSize)
	for {
		_, next, err := reader.ReadAt(offset)
		if err == io.EOF {
			return offset, false, nil
		}
		// A wrapped EOF means the entry's length arrived and nothing after it did.
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return offset, true, nil
		}
		if err != nil {
			return 0, false, err
		}
		offset = next
	}
}
