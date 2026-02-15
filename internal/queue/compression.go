// Package queue provides compression utility functions for message payloads.
package queue

import (
	"github.com/1mb-dev/ledgerq/internal/format"
	"github.com/1mb-dev/ledgerq/internal/logging"
)

// compressPayloadIfNeeded compresses a payload if compression is requested and beneficial.
// Returns the (potentially compressed) payload, the compression type used, and any error.
//
// Compression is applied if:
//  1. compressionType is not CompressionNone
//  2. payload size >= minCompressionSize
//  3. compressed size < original size * 0.95 (at least 5% savings)
//
// If compression is not beneficial, returns the original payload with CompressionNone.
func compressPayloadIfNeeded(
	payload []byte,
	compressionType format.CompressionType,
	compressionLevel int,
	minCompressionSize int,
	logger logging.Logger,
) ([]byte, format.CompressionType, error) {
	// No compression requested
	if compressionType == format.CompressionNone {
		return payload, format.CompressionNone, nil
	}

	// Payload too small to compress
	if len(payload) < minCompressionSize {
		logger.Debug("payload too small for compression, storing uncompressed",
			logging.F("payload_size", len(payload)),
			logging.F("min_compression_size", minCompressionSize),
		)
		return payload, format.CompressionNone, nil
	}

	// Try to compress
	compressed, err := format.CompressPayload(payload, compressionType, compressionLevel)
	if err != nil {
		// Compression failed, return error
		return nil, format.CompressionNone, err
	}

	// Check if compression is beneficial
	if format.ShouldCompress(len(payload), len(compressed), minCompressionSize) {
		logger.Debug("payload compressed successfully",
			logging.F("original_size", len(payload)),
			logging.F("compressed_size", len(compressed)),
			logging.F("compression_ratio", float64(len(compressed))/float64(len(payload))),
			logging.F("compression_type", compressionType.String()),
		)
		return compressed, compressionType, nil
	}

	// Compression not beneficial, use original
	logger.Debug("compression not beneficial, storing uncompressed",
		logging.F("original_size", len(payload)),
		logging.F("compressed_size", len(compressed)),
		logging.F("savings_percent", (1.0-float64(len(compressed))/float64(len(payload)))*100),
	)
	return payload, format.CompressionNone, nil
}
