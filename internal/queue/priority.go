// Package queue provides priority queue management functionality.
// This file contains priority index operations for priority-aware message dequeuing.
package queue

import (
	"github.com/1mb-dev/ledgerq/internal/format"
	"github.com/1mb-dev/ledgerq/internal/logging"
	"github.com/1mb-dev/ledgerq/internal/segment"
)

// rebuildPriorityIndex scans all existing segments and rebuilds the priority index.
// This is called when opening a queue with EnablePriorities=true.
func (q *Queue) rebuildPriorityIndex() {
	allSegments := q.segments.GetSegments()
	activeSeg := q.segments.GetActiveSegment()
	if activeSeg != nil {
		allSegments = append(allSegments, activeSeg)
	}

	if len(allSegments) == 0 {
		// No segments yet, nothing to rebuild
		return
	}

	// Scan all segments and add entries to priority index
	for _, seg := range allSegments {
		reader, err := segment.NewReader(seg.Path)
		if err != nil {
			// Skip segments that can't be opened (e.g., empty segments)
			q.opts.Logger.Debug("skipping segment during priority index rebuild",
				logging.F("path", seg.Path),
				logging.F("error", err.Error()),
			)
			continue
		}

		// Scan all entries in this segment
		err = reader.ScanAll(func(entry *format.Entry, offset uint64) error {
			// Only index messages that haven't been read yet
			if entry.MsgID >= q.readMsgID && entry.Type == format.EntryTypeData {
				// Get position from segment reader for priority indexing
				// We cast offset to uint32 for the priority index which expects this type
				// This is safe as segment offsets are controlled and won't exceed uint32 limits
				position := uint32(offset) //nolint:gosec // G115: Offset won't exceed uint32 in practice
				q.priorityIndex.Insert(offset, position, entry.Priority, entry.Timestamp)
			}
			return nil
		})

		_ = reader.Close()

		if err != nil {
			// Skip segments with scan errors
			q.opts.Logger.Debug("skipping segment scan during priority index rebuild",
				logging.F("path", seg.Path),
				logging.F("error", err.Error()),
			)
			continue
		}
	}

	q.opts.Logger.Info("priority index rebuilt",
		logging.F("total_entries", q.priorityIndex.Count()),
		logging.F("high_priority", q.priorityIndex.CountByPriority(format.PriorityHigh)),
		logging.F("medium_priority", q.priorityIndex.CountByPriority(format.PriorityMedium)),
		logging.F("low_priority", q.priorityIndex.CountByPriority(format.PriorityLow)),
	)
}
